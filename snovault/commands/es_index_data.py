import argparse
import logging
import webtest

from pyramid.paster import get_app
from dcicutils.log_utils import set_logging


EPILOG = __doc__
INDEXING_RUN_ITERATIONS = 100


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
    options = {
        'embed_cache.capacity': '5000',
        'indexer': 'true',
    }
    app = get_app(args.config_uri, args.app_name, options)

    # Loading app will have configured from config file. Reconfigure here:
    # Use `es_server=app.registry.settings.get('elasticsearch.server')` when ES logging is working
    set_logging(in_prod=app.registry.settings.get('production'), level=logging.INFO)

    return run(app, args.uuid)


if __name__ == '__main__':
    main()
