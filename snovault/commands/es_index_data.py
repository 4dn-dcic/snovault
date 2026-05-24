import argparse
import ctypes
import gc
import logging
import webtest

from pyramid.paster import get_app
from dcicutils.log_utils import set_logging


EPILOG = __doc__
INDEXING_RUN_ITERATIONS = 50


def _tune_allocator():
    """Tune glibc so large allocations use mmap and are returned to the OS on free.

    Objects above M_MMAP_THRESHOLD bypass the heap and use mmap directly; freed
    mmap regions are immediately released to the OS without needing malloc_trim.
    Set to 1 MB so that large embedded documents (multi-MB MetaWorkflowRun
    objects) are mmap-backed while smaller repeated allocations still reuse the
    heap free-list for speed.
    """
    try:
        ctypes.CDLL('libc.so.6').mallopt(
            ctypes.c_int(-3),       # M_MMAP_THRESHOLD
            ctypes.c_int(1048576),  # 1 MB
        )
    except Exception:
        pass  # non-Linux environments


def run(app, uuids=None):
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'INDEXER',
    }
    testapp = webtest.TestApp(app, environ)
    post_body = {
        'record': True
    }
    if uuids:
        post_body['uuids'] = list(uuids)
        testapp.post_json('/index', post_body)
    else:
        for _ in range(INDEXING_RUN_ITERATIONS):
            testapp.post_json('/index', post_body)
            gc.collect()


def main():
    """ Indexes app data loaded to elasticsearch """

    parser = argparse.ArgumentParser(
        description="Index data in Elastic Search",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--uuid', action='append', help="uuid to index")
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('config_uri', help="path to configfile")
    args = parser.parse_args()

    logging.basicConfig()
    # NOTE: the embed cache capacity can in fact be too large in the case
    # of indexing many objects that are not used frequently or enough to
    # justify keeping around - Will 22 May 2026
    options = {
        'embed_cache.capacity': '200',
        'indexer': 'true',
    }
    app = get_app(args.config_uri, args.app_name, options)

    # Freeze all framework/app objects so GC never scans them during indexing.
    # Only objects allocated after this point will be tracked by the collector,
    # making each gc.collect() call between iterations much cheaper.
    gc.freeze()

    # Tune glibc mmap threshold so large embedded documents are returned to the
    # OS automatically on free, without needing explicit malloc_trim calls.
    _tune_allocator()

    # Loading app will have configured from config file. Reconfigure here:
    # Use `es_server=app.registry.settings.get('elasticsearch.server')` when ES logging is working
    set_logging(in_prod=app.registry.settings.get('production'), level=logging.INFO)

    return run(app, args.uuid)


if __name__ == '__main__':
    main()
