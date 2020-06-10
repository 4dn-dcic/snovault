"""\
Example.

    %(prog)s production.ini

"""

import argparse
import atexit
import datetime
import elasticsearch.exceptions
import json
import logging
import os
import psycopg2
import signal
import sqlalchemy.exc
import structlog
import sys
import threading
import time
import webtest

from pyramid import paster
from dcicutils.log_utils import set_logging
from .interfaces import ELASTIC_SEARCH, INDEXER_QUEUE


log = structlog.getLogger(__name__)

EPILOG = __doc__
DEFAULT_INTERVAL = 3  # 3 second default

# We need this because of MVCC visibility.
# See slide 9 at http://momjian.us/main/writings/pgsql/mvcc.pdf
# https://devcenter.heroku.com/articles/postgresql-concurrency


def run(testapp, interval=DEFAULT_INTERVAL, dry_run=False, path='/index', update_status=None):
    log.info('___INDEXER LISTENER STARTING___')
    listening = False
    timestamp = datetime.datetime.now().isoformat()
    update_status(
        listening=listening,
        status='indexing',
        timestamp=timestamp
    )

    # Make sure elasticsearch is up before trying to index.
    es = testapp.app.registry[ELASTIC_SEARCH]
    es.info()

    queue = testapp.app.registry[INDEXER_QUEUE]

    # main listening loop
    while True:
        # if not messages to index, skip the /index call. Counts are approximate
        queue_counts = queue.number_of_messages()
        if (not queue_counts['primary_waiting'] and not queue_counts['secondary_waiting']):
            time.sleep(interval)
            continue

        try:
            res = testapp.post_json(path, {
                'record': True,
                'dry_run': dry_run
            })
        except Exception as e:
            timestamp = datetime.datetime.now().isoformat()
            log.exception('index failed')
            update_status(error={
                'error': repr(e),
                'timestamp': timestamp,
            })
        else:
            timestamp = datetime.datetime.now().isoformat()
            result = res.json
            result['stats'] = res.headers.get('X-Stats', {})
            result['timestamp'] = timestamp
            update_status(last_result=result)
            if result.get('indexing_status') == 'finished':
                update_status(result=result)
                if result.get('errors'):
                    log.error('___INDEX LISTENER RESULT:___\n%s\n' % result)
                else:
                    log.debug('___INDEX LISTENER RESULT:___\n%s\n' % result)
        time.sleep(interval)


class ErrorHandlingThread(threading.Thread):

    def run(self):
        # interval = self._kwargs.get('interval', DEFAULT_INTERVAL)
        interval = 60  # DB polling can and should be slower
        update_status = self._kwargs['update_status']
        while True:
            try:
                self._target(*self._args, **self._kwargs)
            except (psycopg2.OperationalError, sqlalchemy.exc.OperationalError, elasticsearch.exceptions.ConnectionError) as e:
                # Handle database restart
                log.warning('Database not there, maybe starting up: %r', e)
                timestamp = datetime.datetime.now().isoformat()
                update_status(
                    timestamp=timestamp,
                    status='sleeping',
                    error={'error': repr(e), 'timestamp': timestamp},
                )
                log.debug('sleeping')
                time.sleep(interval)
                continue
            except Exception:
                # Unfortunately mod_wsgi does not restart immediately
                log.exception('Exception in listener, restarting process at next request.')
                os.kill(os.getpid(), signal.SIGINT)
            break


def composite(loader, global_conf, **settings):
    listener = None

    # Register before testapp creation.
    @atexit.register
    def join_listener():
        if listener:
            log.debug('joining listening thread')
            listener.join()

    path = settings.get('path', '/index')

    # Composite app is used so we can load the main app
    app_name = settings.get('app', None)
    app = loader.get_app(app_name, global_conf=global_conf)
    username = settings.get('username', 'IMPORT')
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': username,
    }
    testapp = webtest.TestApp(app, environ)


    timestamp = datetime.datetime.now().isoformat()
    status_holder = {
        'status': {
            'status': 'starting listener',
            'started': timestamp,
            'errors': [],
            'results': [],
        },
    }

    def update_status(error=None, result=None, indexed=None, **kw):
        # Setting a value in a dictionary is atomic
        status = status_holder['status'].copy()
        status.update(**kw)
        if error is not None:
            status['errors'] = [error] + status['errors'][:2]
        if result is not None:
            status['results'] = [result] + status['results'][:9]
        status_holder['status'] = status

    kwargs = {
        'testapp': testapp,
        'update_status': update_status,
        'path': path,
    }
    if 'interval' in settings:
        kwargs['interval'] = float(settings['interval'])

    # daemon thread that actually executes `run` method to call /index
    listener = ErrorHandlingThread(target=run, name='listener', kwargs=kwargs)
    listener.daemon = True
    log.debug('starting listener')
    listener.start()

    # Register before testapp creation.
    @atexit.register
    def shutdown_listener():
        log.debug('shutting down listening thread')

    def status_app(environ, start_response):
        status = '200 OK'
        response_headers = [('Content-type', 'application/json')]
        start_response(status, response_headers)
        return [json.dumps(status_holder['status'])]

    return status_app


def internal_app(configfile, app_name=None, username=None):
    app = paster.get_app(configfile, app_name)
    if not username:
        username = 'IMPORT'
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': username,
    }
    return webtest.TestApp(app, environ)


def main():
    parser = argparse.ArgumentParser(
        description="Listen for changes from postgres and index in elasticsearch",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument(
        '--username', '-u', default='INDEXER', help="Import username")
    parser.add_argument(
        '--dry-run', action='store_true', help="Don't post to ES, just print")
    parser.add_argument(
        '-v', '--verbose', action='store_true', help="Print debug level logging")
    parser.add_argument(
        '--poll-interval', type=int, default=DEFAULT_INTERVAL,
        help="Poll interval between notifications")
    parser.add_argument(
        '--path', default='/index',
        help="Path of indexing view")
    parser.add_argument('config_uri', help="path to configfile")
    args = parser.parse_args()

    # logging.basicConfig()
    testapp = internal_app(args.config_uri, args.app_name, args.username)


    # Loading app will have configured from config file. Reconfigure here:
    level = logging.INFO
    if args.verbose or args.dry_run:
        level = logging.DEBUG

    # Loading app will have configured from config file. Reconfigure here:
    # Use `es_server=app.registry.settings.get('elasticsearch.server')` when ES logging is working
    set_logging(in_prod=app.registry.settings.get('production'), level=logging.INFO)
    return run(testapp, args.poll_interval, args.dry_run, args.path)


if __name__ == '__main__':
    main()
