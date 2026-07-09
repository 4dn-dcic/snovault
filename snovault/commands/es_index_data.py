import argparse
import logging
import webtest

from pyramid.paster import get_app
from dcicutils.log_utils import set_logging

from snovault.elasticsearch.interfaces import INDEXER_QUEUE


EPILOG = __doc__
INDEXING_RUN_ITERATIONS = 100
# require this many consecutive empty-queue/zero-indexed iterations before breaking
# early, since queue_is_empty relies on SQS's approximate (eventually-consistent)
# message counts
CONSECUTIVE_EMPTY_ITERATIONS_TO_STOP = 2


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
        indexer_queue = app.registry[INDEXER_QUEUE]
        consecutive_empty = 0
        for _ in range(INDEXING_RUN_ITERATIONS):
            response = testapp.post_json('/index', post_body)
            indexing_count = response.json.get('indexing_count', 0)
            if indexing_count == 0 and indexer_queue.queue_is_empty(secondary_only=False,
                                                                     include_inflight=True):
                consecutive_empty += 1
                if consecutive_empty >= CONSECUTIVE_EMPTY_ITERATIONS_TO_STOP:
                    break
            else:
                consecutive_empty = 0


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
        'embed_cache.capacity': '2000',
        'indexer': 'true',
    }
    app = get_app(args.config_uri, args.app_name, options)

    # Loading app will have configured from config file. Reconfigure here:
    # Use `es_server=app.registry.settings.get('elasticsearch.server')` when ES logging is working
    set_logging(in_prod=app.registry.settings.get('production'), level=logging.INFO)

    return run(app, args.uuid)


if __name__ == '__main__':
    main()
