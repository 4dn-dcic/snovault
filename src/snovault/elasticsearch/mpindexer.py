from snovault import DBSESSION
from contextlib import contextmanager
from multiprocessing import get_context
from multiprocessing.pool import Pool
from functools import partial
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager,
)
import atexit
import structlog
import logging
from snovault import set_logging
from snovault.storage import register_storage
import transaction
import signal
from .indexer import (
    INDEXER,
    Indexer,
)
from .interfaces import (
    APP_FACTORY,
    INDEXER_QUEUE
)

log = structlog.getLogger(__name__)

def includeme(config):
    if config.registry.settings.get('indexer_worker'):
        return
    processes = config.registry.settings.get('indexer.processes')
    try:
        processes = int(processes)
    except:
        processes = None
    config.registry[INDEXER] = MPIndexer(config.registry, processes=processes)


### Running in subprocess

app = None

def initializer(app_factory, settings):
    """
    Need to initialize the app for the subprocess
    """
    from snovault.app import configure_engine
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # set up global variables to use throughout subprocess
    global app
    atexit.register(clear_manager_and_dispose_engine)
    app = app_factory(settings, indexer_worker=True, create_tables=False)

    global db_engine
    db_engine = configure_engine(settings)

    set_logging(app.registry.settings.get('elasticsearch.server'),
                app.registry.settings.get('production'), level=logging.INFO)
    global log
    log = structlog.get_logger(__name__)


@contextmanager
def threadlocal_manager():
    """
    Set registry and request attributes using the global app within the
    subprocess
    """
    import snovault.storage
    import zope.sqlalchemy
    from sqlalchemy import orm

    # clear threadlocal manager, though it should be clean
    manager.pop()

    registry = app.registry
    request = app.request_factory.blank('/_indexing_pool')
    request.registry = registry
    request.datastore = 'database'
    apply_request_extensions(request)
    request.invoke_subrequest = app.invoke_subrequest
    request.root = app.root_factory(request)
    request._stats = getattr(request, "_stats", {})

    # configure a sqlalchemy session and adjust isolation level
    DBSession = orm.scoped_session(orm.sessionmaker(bind=db_engine))
    request.registry[DBSESSION] = DBSession
    register_storage(request.registry)
    zope.sqlalchemy.register(DBSession)
    snovault.storage.register(DBSession)  # adds transactions-table listeners
    connection = request.registry[DBSESSION]().connection()
    connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')

    # add the newly created request to the pyramid threadlocal manager
    manager.push({'request': request, 'registry': registry})
    yield
    # remove the session when leaving contextmanager
    request.registry[DBSESSION].remove()


def clear_manager_and_dispose_engine(signum=None, frame=None):
    manager.pop()
    # manually dispose of db engines for garbage collection
    if db_engine is not None:
        db_engine.dispose()


### These helper functions are needed for multiprocessing

def sync_update_helper(uuid):
    """
    Used with synchronous indexing. Counter is controlled at a higher level
    (MPIndexer.update_objects)
    """
    with threadlocal_manager():
        request = get_current_request()
        indexer = request.registry[INDEXER]
        return indexer.update_object(request, uuid)


def queue_update_helper():
    """
    Used with the queue. Keeps a local counter and errors, which are returned
    to the callback function and synchronized with overall values
    """
    with threadlocal_manager():
        local_counter = [0]
        request = get_current_request()
        indexer = request.registry[INDEXER]

        local_errors = indexer.update_objects_queue(request, local_counter)
        return (local_errors, local_counter)


def queue_error_callback(cb_args, counter, errors):
    local_errors, local_counter = cb_args
    if counter:
        counter[0] = local_counter[0] + counter[0]
    errors.extend(local_errors)


### Running in main process

class MPIndexer(Indexer):
    def __init__(self, registry, processes=None):
        super(MPIndexer, self).__init__(registry)
        self.chunksize = int(registry.settings.get('indexer.chunk_size', 1024))
        self.processes = processes
        self.initargs = (registry[APP_FACTORY], registry.settings,)
        # workers in the pool will be replaced after finishing one task
        # to free memory
        self.maxtasks = 1

    def init_pool(self):
        return Pool(
            processes=self.processes,
            initializer=initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasks,
            context=get_context('spawn'),
        )

    def update_objects(self, request, counter=None):
        """
        Initializes a multiprocessing pool with args given in __init__ and
        indexes in one of two mode: synchronous or queued.
        If a list of uuids is passed in the request, sync indexing will occur,
        breaking the list up among all available workers in the pool.
        Otherwise, all available workers will asynchronously pull uuids of the
        queue for indexing (see indexer.py).
        Close the pool at the end of the function and return list of errors.
        """
        pool = self.init_pool()
        sync_uuids = request.json.get('uuids', None)
        workers = pool._processes if self.processes is None else self.processes
        # ensure workers != 0
        # workers = 1 if workers == 0 else workers
        workers = 100
        errors = []
        try:
            if sync_uuids:
                # determine how many uuids should be used for each process
                chunkiness = int((len(sync_uuids) - 1) / workers) + 1
                if chunkiness > self.chunksize:
                    chunkiness = self.chunksize
                # imap_unordered to hopefully shuffle item types and come up with
                # a more or less equal workload for each process
                for error in pool.imap_unordered(sync_update_helper, sync_uuids, chunkiness):
                    if error is not None:
                        errors.append(error)
                    elif counter:  # don't increment counter on an error
                        counter[0] += 1
                    if counter[0] % 10 == 0:
                        log.info('Indexing %d (sync)', counter[0])
            else:
                # use partial here so the callback can use counter and errors
                callback_w_errors = partial(queue_error_callback, counter=counter, errors=errors)
                for i in range(workers):
                    pool.apply_async(queue_update_helper, callback=callback_w_errors)
        except Exception as exc:
            # on exception, terminate workers immediately before joining
            log.error("MPIndexer.update_objects Exception, terminating. Detail: %s" % repr(exc))
            pool.terminate()
        else:
            # standard usage, will let workers finish before joining
            pool.close()
        pool.join()
        return errors
