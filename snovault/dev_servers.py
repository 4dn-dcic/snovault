"""\
Examples
For the development.ini you must supply the paster app name:

    %(prog)s development.ini --app-name app --init --clear

"""

import argparse
import atexit
import logging
import os.path
import select
import shutil
import subprocess
import sys
from urllib.parse import urlparse as url_parse, parse_qs as url_parse_query

from dcicutils.misc_utils import PRINT
from pyramid.paster import get_app, get_appsettings
from pyramid.path import DottedNameResolver
from .elasticsearch import create_mapping
from .project_app import app_project
from .tests import elasticsearch_fixture, postgresql_fixture


EPILOG = __doc__
DEFAULT_DATA_DIR = "/tmp/snovault"

logger = logging.getLogger(__name__)


def nginx_server_process(prefix='', echo=False):
    args = [
        os.path.join(prefix, 'nginx'),
        '-c', app_project().project_filename('nginx-dev.conf'),
        '-g', 'daemon off;'
    ]
    process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    if not echo:
        process.stdout.close()

    if echo:
        PRINT('Started: http://localhost:8000')

    return process


def ingestion_listener_compute_command(config_uri, app_name):
    return [
        'poetry', 'run', 'ingestion-listener', config_uri, '--app-name', app_name
    ]


def ingestion_listener_process(config_uri, app_name, echo=True):
    """ Uses Popen to start up the ingestion-listener. """
    args = ingestion_listener_compute_command(config_uri, app_name)

    process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    if echo:
        PRINT('Starting Ingestion Listener...')

    return process


def redis_server_process(echo=False):
    """ Handler that spins up a Redis server on port 6379 (default)"""
    args = [
        'redis-server',
        '--daemonize',
        'yes'
    ]
    process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if not echo:
        process.stdout.close()
    if echo:
        print('Started Redis Server at redis://localhost:6379')
    return process


def main():
    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is specified wrong here.
        description="Run development servers", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--clear', action="store_true", help="Clear existing data")
    parser.add_argument('--init', action="store_true", help="Init database")
    parser.add_argument('--load', action="store_true", help="Load test set")
    parser.add_argument('--datadir', default=None, help="path to datadir")
    parser.add_argument('--no_ingest', action="store_true", default=False, help="Don't start the ingestion process.")
    args = parser.parse_args()

    run(app_name=args.app_name, config_uri=args.config_uri, datadir=args.datadir,
        # Ingestion is disabled. snovault has no such concept. -kmp 17-Feb-2023
        clear=args.clear, init=args.init, load=args.load, ingest=not args.no_ingest)


def run(app_name, config_uri, datadir, clear=False, init=False, load=False, ingest=True):

    #project = app_project(initialize=True)
    project = app_project()

    logging.basicConfig(format='')
    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger(project.NAME).setLevel(logging.INFO)

    # get the config and see if we want to connect to non-local servers
    # TODO: This variable seems to not get used? -kmp 25-Jul-2020
    config = get_appsettings(config_uri, app_name)

    if sqlalchemy_url := config.get("sqlalchemy.url", None):
        # New as of 2024-07-30 (dmichaels).
        # Handle sqlalchemy.url property defined in development.ini that looks something like this:
        # sqlalchemy.url = postgresql://postgres@localhost:5442/postgres?host=/tmp/snovault/pgdata
        # This allows us to get the temporary data directory (from the URL host query-string, for both
        # Postgres and ElasticSearch, e.g. /tmp/snovault) and the Postgres port (from the URL port),
        # so that we can easily change where Postgres is running to support (for example) running
        # both smaht-portal and cgap-portal locally simultaneously. This also obviates the need
        # in the portal makefiles to parse out the port from this (sqlalchemy.url) property to
        # set the SNOVAULT_DB_TEST_PORT environment variable as was currently done.
        sqlalchemy_url_parsed = url_parse(sqlalchemy_url)
        sqlalchemy_url_port = sqlalchemy_url_parsed.port
        sqlalchemy_url_query = url_parse_query(sqlalchemy_url_parsed.query)
        if sqlalchemy_url_host := sqlalchemy_url_query.get("host", [None])[0]:
            if sqlalchemy_url_host.endswith("/pgdata"):
                sqlalchemy_url_host = sqlalchemy_url_host[:-len("/pgdata")]
        if (datadir is None) and sqlalchemy_url_host:
            datadir = sqlalchemy_url_host
        if (os.environ.get("SNOVAULT_DB_TEST_PORT", None) is None) and sqlalchemy_url_port:
            os.environ["SNOVAULT_DB_TEST_PORT"] =  str(sqlalchemy_url_port)
    if not datadir:
        datadir = DEFAULT_DATA_DIR

    datadir = os.path.abspath(datadir)
    pgdata = os.path.join(datadir, 'pgdata')
    esdata = os.path.join(datadir, 'esdata')
    # ----- comment out from HERE...
    if clear:
        for dirname in [pgdata, esdata]:
            if os.path.exists(dirname):
                shutil.rmtree(dirname)
    if init:
        postgresql_fixture.initdb(pgdata, echo=True)
    # ----- ... to HERE to disable recreation of test db
    # ----- may have to `rm /tmp/snovault/pgdata/postmaster.pid`

    @atexit.register
    def cleanup_process():
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                for line in process.stdout:
                    sys.stdout.write(line.decode('utf-8'))
            except IOError:
                pass
            process.wait()

    processes = []

    # For now - required components
    postgres = postgresql_fixture.server_process(pgdata, echo=True, port=os.environ.get("SNOVAULT_DB_TEST_PORT"))
    processes.append(postgres)

    es_server_url = config.get('elasticsearch.server', "localhost")

    if '127.0.0.1' in es_server_url or 'localhost' in es_server_url:
        # Bootup local ES server subprocess; otherwise assume we are connecting to a remote ES cluster.
        # The elasticsearch.server.actual_port property is useful (only) for running a localhost ElasticSearch
        # proxy in order to observe traffic (requests/responses) between portal and ElasticSearch with a tool like
        # mitmweb; e.g. setting elasticsearch.server.actual_port to 9201 and elasticsearch.server to localhost:9200
        # will cause ElasticSearch to actually run on port 9201 but will cause portal to talk to it via port 9200,
        # and then we can run mitmweb --mode reverse:http://localhost:9201 -p 9200 --web-port 8081 which will
        # allow us to browse to http://localhost:8081 locally to observe all of the ElasticSearch traffic.
        if (es_port := config.get('elasticsearch.server.actual_port', None)) and es_port.isdigit():
            es_port = int(es_port)
        elif ((colon := es_server_url.rfind(":")) > 0) and (es_port := es_server_url[colon + 1:]).isdigit():
            es_port = int(es_port)
        else:
            es_port = None
        transport_ports = config.get('elasticsearch.server.transport_ports', None)
        elasticsearch = elasticsearch_fixture.server_process(esdata, port=es_port, echo=True,
                                                             transport_ports=transport_ports)
        processes.append(elasticsearch)
    elif not config.get('indexer.namespace'):
        raise Exception(
            'It looks like are connecting to remote elasticsearch.server but no indexer.namespace is defined.')
    elif not config.get("elasticsearch.aws_auth", False):
        # TODO detect if connecting to AWS or not before raising an Exception.
        PRINT(
            'WARNING - elasticsearch.aws_auth is set to false.'
            ' Connection will fail if connecting to remote ES cluster on AWS.')

    nginx = nginx_server_process(echo=True)
    processes.append(nginx)

    app = get_app(config_uri, app_name)
    settings = app.registry.settings

    # Optional components
    if 'redis.server' in settings:
        redis = redis_server_process(echo=True)
        processes.append(redis)

    if ingest:
        ingestion_listener = ingestion_listener_process(config_uri, app_name)
        processes.append(ingestion_listener)

    # clear queues and initialize indices before loading data. No indexing yet.
    # this is needed for items with properties stored in ES
    if init:
        create_mapping.run(app, skip_indexing=True, purge_queue=False)

    if init and load:
        load_test_data = app.registry.settings.get('load_test_data')
        load_test_data = DottedNameResolver().resolve(load_test_data)
        load_res = load_test_data(app)
        if load_res:  # None if successful
            raise load_res

        # now clear the queues and queue items for indexing
        create_mapping.run(app, check_first=True, strict=True, purge_queue=False)
        # To test prod setup:
        # create_mapping.reindex_by_type_staggered(app)

    PRINT('Started. ^C to exit.')

    stdouts = [p.stdout for p in processes]

    # Ugly should probably use threads instead
    while True:
        readable, writable, err = select.select(stdouts, [], stdouts, 5)
        for stdout in readable:
            for line in iter(stdout.readline, b''):
                sys.stdout.write(line.decode('utf-8'))
        if err:
            for stdout in err:
                for line in iter(stdout.readline, b''):
                    sys.stdout.write(line.decode('utf-8'))
            break


if __name__ == '__main__':
    main()
