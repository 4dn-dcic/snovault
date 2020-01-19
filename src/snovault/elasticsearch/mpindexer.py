from .. import DBSESSION
from contextlib import contextmanager
from multiprocessing import (
    get_context,
    cpu_count
)
from multiprocessing.pool import Pool
from functools import partial
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager
)
import atexit
import structlog
import logging
from .. import set_logging
from ..storage import register_storage
import transaction
import signal
import time
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
    config.registry[INDEXER] = MPIndexer(config.registry)


### Running in subprocess

app = None

def initializer(app_factory, settings):
    """
    Need to initialize the app for the subprocess
    """
    from ..app import configure_engine
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # set up global variables to use throughout subprocess
    global app
    atexit.register(clear_manager_and_dispose_engine)
    app = app_factory(settings, indexer_worker=True, create_tables=False)

    global db_engine
    db_engine = configure_engine(settings)

    # Use `es_server=app.registry.settings.get('elasticsearch.server')` when ES logging is working
    set_logging(in_prod=app.registry.settings.get('production'))
    global log
    log = structlog.get_logger(__name__)


@contextmanager
def threadlocal_manager():
    """
    Set registry and request attributes using the global app within the
    subprocess
    """
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

    # configure a sqlalchemy session and set isolation level
    DBSession = orm.scoped_session(orm.sessionmaker(bind=db_engine))
    request.registry[DBSESSION] = DBSession
    register_storage(request.registry)
    zope.sqlalchemy.register(DBSession)
    connection = request.registry[DBSESSION]().connection()
    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')

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
    to the callback function and synchronized with overall values.
    `local_deferred` is True when the indexer hits an sid exception and must
    defer the indexing; it should stay as the third returned value in the
    tuple and is used in overall MPIndexer.update_objects function
    """
    with threadlocal_manager():
        local_counter = [0]
        request = get_current_request()
        indexer = request.registry[INDEXER]
        local_errors, local_deferred = indexer.update_objects_queue(request, local_counter)
        return (local_errors, local_counter, local_deferred)


def queue_error_callback(cb_args, counter, errors):
    """
    Update the counter and errors with the result of the given callback arguments
    """
    local_errors, local_counter, _ = cb_args
    if counter:
        counter[0] = local_counter[0] + counter[0]
    errors.extend(local_errors)


### Running in main process

class MPIndexer(Indexer):
    def __init__(self, registry):
        super(MPIndexer, self).__init__(registry)
        self.chunksize = int(registry.settings.get('indexer.chunk_size', 1024))
        # use 2 fewer processes than cpu count, with a minimum of 1
        num_cpu = cpu_count()
        self.processes = num_cpu - 2 if num_cpu - 2 > 1 else 1
        self.initargs = (registry[APP_FACTORY], registry.settings,)
        # workers in the pool will be replaced after finishing one task
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
        workers = self.processes
        # ensure workers != 0
        workers = 1 if workers == 0 else workers
        errors = []

        # use sync_uuids with imap_unordered for synchronous indexing OR
        # apply_async for asynchronous indexing
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
            # hold AsyncResult objects returned by apply_async
            async_results = []
            # last_count used to track if there is "more" work to do
            last_count = 0

            # create the initial workers (same as number of processes in pool)
            for i in range(workers):
                res = pool.apply_async(queue_update_helper, callback=callback_w_errors)
                async_results.append(res)

            # check worker statuses
            # add more workers if any are finished and indexing is ongoing
            while True:
                results_to_add = []
                idxs_to_rm = []
                for idx, res in enumerate(async_results):
                    if res.ready():
                        # res_vals are returned from one run of `queue_update_helper`
                        # in form: (errors <list>, counter <list>, deferred <bool>)
                        res_vals = res.get()
                        idxs_to_rm.append(idx)
                        # add worker if overall counter has increased OR process is deferred
                        if (counter and counter[0] > last_count) or res_vals[2] is True:
                            last_count = counter[0]
                            res = pool.apply_async(queue_update_helper, callback=callback_w_errors)
                            results_to_add.append(res)

                for idx in sorted(idxs_to_rm, reverse=True):
                    del async_results[idx]
                async_results.extend(results_to_add)

                if len(async_results) == 0:
                    break
                time.sleep(0.5)

        pool.close()
        pool.join()
        return errors
