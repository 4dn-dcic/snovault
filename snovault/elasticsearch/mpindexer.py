import atexit
import logging
import structlog
import time
import transaction
import signal
import zope.sqlalchemy

from contextlib import contextmanager
from functools import partial
from multiprocessing import (
    get_context,
    cpu_count,
    TimeoutError
)
from multiprocessing.pool import Pool
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager
)
from sqlalchemy import orm

from ..interfaces import DBSESSION
from dcicutils.log_utils import set_logging
from ..app import configure_engine
from .indexer import (
    INDEXER,
    Indexer,
)
from .interfaces import (
    APP_FACTORY,
    INDEXER_QUEUE
)
from ..storage import register_storage, RDBStorage


log = structlog.getLogger(__name__)

def includeme(config):
    if config.registry.settings.get('indexer_worker'):
        return
    config.registry[INDEXER] = MPIndexer(config.registry)


### Running in subprocess

app = None

def initializer(app_factory, settings):
    """
    Need to initialize the app for the subprocess.
    As part of this, configue a new database engine and set logging
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # set up global variables to use throughout subprocess
    global db_engine
    db_engine = None
    atexit.register(clear_manager_and_dispose_engine)

    global app
    app = app_factory(settings, indexer_worker=True, create_tables=False)
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

    # clear threadlocal manager to get a clean stack
    manager.clear()

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
    # configue RDBStorage. Overide write storage to use new DBSession
    register_storage(request.registry, write_override=RDBStorage(DBSession))
    zope.sqlalchemy.register(DBSession)
    connection = DBSession().connection()
    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')

    # add the newly created request to the pyramid threadlocal manager
    manager.push({'request': request, 'registry': registry})
    yield
    # remove the session when leaving contextmanager
    request.registry[DBSESSION].remove()


def clear_manager_and_dispose_engine(signum=None, frame=None):
    manager.clear()
    # manually dispose of db engine for garbage collection
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
        self.processes = self.suggested_number_of_processes(registry)
        self.initargs = (registry[APP_FACTORY], registry.settings,)

    @staticmethod
    def suggested_number_of_processes(registry):
        """
        Called by the initializer. Will check the application registry for the 'ENCODED_INDEXER' option,
        in which case we will 1.5x the number of indexing processes.
        """
        num_cpu = cpu_count()
        cpus_to_use = num_cpu
        if registry.settings.get('index_server', 'false').upper() == 'TRUE':  # XXX: option should be imported
            # done somewhat arbitrarily, should be benchmarked -Will 04/30/2020
            cpus_to_use = round((num_cpu - 2) * 1.5) + 1
        return max(cpus_to_use, 1)

    def init_pool(self):
        """
        Initialize multiprocessing pool.
        Use `maxtasksperchild=1`, which causes the worker to be recycled after
        finishing one call to `queue_update_helper`.
        It seems like this should not be needed due to requests being
        created by `threadlocal_manager`, but the transaction scope is not
        correctly reset on each call to `update_objects` without it.

        TODO: figure out how to remove `maxtasksperchild=1` w.r.t. pyramid_tm
              so that transaction scope is correctly handled and we can skip
              work done by `initializer` for each `queue_update_helper` call
        """
        return Pool(
            processes=self.processes,
            initializer=initializer,
            initargs=self.initargs,
            maxtasksperchild=1,  # see rationale in function documentation above.
            context=get_context('spawn'),
        )

    def update_objects(self, request, counter):
        """
        Initializes a multiprocessing pool with args given in __init__ and
        indexes in one of two mode: synchronous or queued.
        If a list of uuids is passed in the request, sync indexing will occur,
        breaking the list up among all available workers in the pool.
        Otherwise, all available workers will asynchronously pull uuids of the
        queue for indexing (see indexer.py).
        Note that counter is a length 1 array (so it can be passed by reference)
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
                else:
                    counter[0] += 1  # don't increment counter on an error
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
                res = pool.apply_async(queue_update_helper,
                                       callback=callback_w_errors)
                async_results.append(res)

            # check worker statuses
            # add more jobs if any are finished and indexing is ongoing
            while True:
                results_to_add = []
                idxs_to_rm = []
                for idx, res in enumerate(async_results):
                    if res.ready():
                        # res_vals are returned from one run of `queue_update_helper`
                        # in form: (errors <list>, counter <list>, deferred <bool>)
                        res_vals = res.get()
                        idxs_to_rm.append(idx)

                        # add jobs if overall counter has increased OR process is deferred
                        if (counter[0] > last_count) or res_vals[2] is True:
                            last_count = counter[0]
                            res = pool.apply_async(queue_update_helper,
                                                   callback=callback_w_errors)
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
