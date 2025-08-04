import uuid
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
    'g.recaptcha.key': 'dummy-recaptcha',
    'auth0.client': 'dummy-client',
    'auth0.domain': 'dummy.domain',
    'accession_factory': 'snovault.server_defaults.test_accession',
    'auth0.options': {
        'auth': {
            'sso': False,
            'redirect': False,
            'responseType': 'token',
            'params': {
                'scope': 'openid email',
                'prompt': 'select_account'
            }
        },
        'allowedConnections': [
            'github', 'google-oauth2', 'partners'
        ]
    },
    'multiauth.policies': 'session remoteuser accesskey auth0',
    'multiauth.groupfinder': 'snovault.authorization.groupfinder',
    'multiauth.policy.session.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.session.base': 'pyramid.authentication.SessionAuthenticationPolicy',
    'multiauth.policy.session.namespace': 'mailto',
    'multiauth.policy.remoteuser.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.remoteuser.namespace': 'remoteuser',
    'multiauth.policy.remoteuser.base': 'pyramid.authentication.RemoteUserAuthenticationPolicy',
    'multiauth.policy.accesskey.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.accesskey.namespace': 'accesskey',
    'multiauth.policy.accesskey.base': 'snovault.authentication.BasicAuthAuthenticationPolicy',
    'multiauth.policy.accesskey.check': 'snovault.authentication.basic_auth_check',
    'multiauth.policy.auth0.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.auth0.namespace': 'auth0',
    'multiauth.policy.auth0.base': 'snovault.authentication.Auth0AuthenticationPolicy',
}


@pytest.fixture(scope="session")
def basic_app_settings():
    return _app_settings.copy()


@pytest.fixture(scope='session')
def app_settings(request, wsgi_server_host_port, conn, DBSession, basic_app_settings):
    settings = basic_app_settings
    assert DBSESSION not in settings
    settings[DBSESSION] = DBSession
    return settings


@pytest.fixture(scope='session')
def app(app_settings):
    """ WSGI application level functional testing.
        will have to make snovault dummy main app """
    return main({}, **app_settings)


@pytest.fixture(scope='session')
def encrypted_app(app_settings):
    """ WSGI application level functional testing with encrypted buckets.
        Note that this also forced use of s3 blob storage.
        Setting blob_bucket in registry.settings == enabling S3blobstorage (and disable DB blobs)
    """
    app_settings_copy = dict(app_settings, **{
        's3_encrypt_key_id': str(uuid.uuid4()),  # moto does not check this is valid
        'blob_bucket': 'encoded-4dn-blobs'  # note that this bucket exists but is mocked out
    })
    return main({}, **app_settings_copy)


@pytest.fixture
def encrypted_testapp(encrypted_app):
    """ TestApp with S3_ENCRYPT_KEY_ID set (encrypted buckets) """
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST',
    }
    return webtest.TestApp(encrypted_app, environ)


@pytest.fixture(scope='session')
def testapp(app):
    """ TestApp with JSON accept header. """
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST',
    }
    return webtest.TestApp(app, environ)


@pytest.fixture
def anontestapp(app):
    """ TestApp with JSON accept header. """
    environ = {
        'HTTP_ACCEPT': 'application/json',
    }
    return webtest.TestApp(app, environ)


@pytest.fixture
def htmltestapp(app):
    """TestApp for TEST user, accepting text/html content."""
    environ = {
        'HTTP_ACCEPT': 'text/html',
        'REMOTE_USER': 'TEST',
    }
    test_app = webtest.TestApp(app, environ)
    return test_app


@pytest.fixture
def authenticated_testapp(app):
    """ TestApp with JSON accept header for non-admin user. """
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
