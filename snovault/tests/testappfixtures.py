import os
import pytest
import webtest

from ..interfaces import DBSESSION
from .. import main  # Function main actually defined in __init__.py (should maybe be defined elsewhere)

_app_settings = {
    'collection_datastore': 'database',
    'item_datastore': 'database',
    'load_test_only': True,
    'testing': True,
    'mpindexer': False,
    'pyramid.debug_authorization': True,
    'postgresql.statement_timeout': 20,
    'retry.attempts': 3,
    'production': True,
    'structlog.dir': '/tmp/',
    'multiauth.policies': 'session remoteuser accesskey webuser',
    'multiauth.groupfinder': 'snovault.tests.authorization.groupfinder',
    'multiauth.policy.session.use': 'snovault.tests.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.session.base': 'pyramid.authentication.SessionAuthenticationPolicy',
    'multiauth.policy.session.namespace': 'mailto',
    'multiauth.policy.remoteuser.use': 'snovault.tests.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.remoteuser.namespace': 'remoteuser',
    'multiauth.policy.remoteuser.base': 'pyramid.authentication.RemoteUserAuthenticationPolicy',
    'multiauth.policy.accesskey.use': 'snovault.tests.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.accesskey.namespace': 'accesskey',
    'multiauth.policy.accesskey.base': 'snovault.tests.authentication.BasicAuthAuthenticationPolicy',
    'multiauth.policy.accesskey.check': 'snovault.tests.authentication.basic_auth_check',
    'multiauth.policy.webuser.use':  'snovault.tests.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.webuser.namespace': 'webuser',
    'multiauth.policy.webuser.base': 'snovault.tests.authentication.WebUserAuthenticationPolicy'
}


@pytest.fixture(scope='session')
def app_settings(request, wsgi_server_host_port, conn, DBSession):
    settings = _app_settings.copy()
    settings[DBSESSION] = DBSession
    return settings


@pytest.fixture(scope='session')
def app(app_settings):
    '''WSGI application level functional testing.
       will have to make snovault dummy main app
    '''
    return main({}, **app_settings)


@pytest.fixture
def testapp(app):
    '''TestApp with JSON accept header.
    '''
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST',
    }
    return webtest.TestApp(app, environ)


@pytest.fixture
def anontestapp(app):
    '''TestApp with JSON accept header.
    '''
    environ = {
        'HTTP_ACCEPT': 'application/json',
    }
    return webtest.TestApp(app, environ)


@pytest.fixture
def authenticated_testapp(app):
    '''TestApp with JSON accept header for non-admin user.
    '''
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST_AUTHENTICATED',
    }
    return webtest.TestApp(app, environ)


@pytest.fixture
def embed_testapp(app):
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'EMBED',
    }
    return webtest.TestApp(app, environ)


@pytest.fixture
def indexer_testapp(app):
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'INDEXER',
    }
    return webtest.TestApp(app, environ)
