import pytest


def pytest_configure():
    import logging
    logging.basicConfig()


@pytest.fixture
def external_tx():
    pass


@pytest.fixture(scope='session')
def app_settings(wsgi_server_host_port, elasticsearch_server, postgresql_server, aws_auth):
    from snovault.tests.testappfixtures import _app_settings
    settings = _app_settings.copy()
    settings['create_tables'] = True
    settings['elasticsearch.server'] = elasticsearch_server
    settings['sqlalchemy.url'] = postgresql_server
    settings['collection_datastore'] = 'elasticsearch'
    settings['item_datastore'] = 'elasticsearch'
    settings['snovault.elasticsearch.index'] = 'snovault'
    settings['indexer'] = True
    settings['should_index'] = True
    settings['indexer.processes'] = 2

    # use aws auth to access elasticsearch
    if aws_auth:
        settings['elasticsearch.aws_auth'] = aws_auth

    return settings


@pytest.yield_fixture(scope='session')
def app(app_settings):
    from snowflakes import main
    from snovault.elasticsearch import create_mapping
    app = main({}, **app_settings)

    create_mapping.run(app, skip_indexing=True)
    yield app

    from snovault import DBSESSION
    DBSession = app.registry[DBSESSION]
    # Dispose connections so postgres can tear down.
    DBSession.bind.pool.dispose()


@pytest.mark.fixture_cost(500)
@pytest.yield_fixture(scope='session')
def workbook(app):
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST',
    }
    testapp = TestApp(app, environ)

    from ...loadxl import load_all
    from pkg_resources import resource_filename
    inserts = resource_filename('snowflakes', 'tests/data/inserts/')
    docsdir = [resource_filename('snowflakes', 'tests/data/documents/')]
    load_all(testapp, inserts, docsdir)
    from timeit import default_timer as timer
    start = timer()
    testapp.post_json('/index', {})
    stop = timer()
    print("indexing time is %s" % (stop-start))
    yield
    # XXX cleanup
