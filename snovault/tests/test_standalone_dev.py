import pytest

from pyramid import paster
from unittest import mock
from ..standalone_dev import make_standalone_app, make_dev_vapp
from .. import standalone_dev as standalone_dev_module

pytestmark = [pytest.mark.working, pytest.mark.unit]


class MockWsgiApp():
    def __init__(self, name, config_uri):
        self.name_for_testing = name
        self.config_uri_for_testing = config_uri


class MockConfigLoader:
    def __init__(self, config_uri):
        self.config_uri = config_uri

    def get_wsgi_app(self, name, options=None):
        assert options is None  # The case we're expecting doesn't pass this argument
        return MockWsgiApp(name=name, config_uri=self.config_uri)


def test_make_standalone_app():

    with mock.patch.object(paster, "get_config_loader", MockConfigLoader):

        with pytest.raises(TypeError):  # positional arguments not allowed
            testapp = make_standalone_app('foo')  # noQA - this is a negative test case

        testapp = make_standalone_app()
        assert testapp.name_for_testing == 'app'
        assert testapp.config_uri_for_testing == 'development.ini'

        testapp = make_standalone_app(name='foo')
        assert testapp.name_for_testing == 'foo'
        assert testapp.config_uri_for_testing == 'development.ini'

        testapp = make_standalone_app(config_file='foo.ini')
        assert testapp.name_for_testing == 'app'
        assert testapp.config_uri_for_testing == 'foo.ini'

        testapp = make_standalone_app(name='foo', config_file='foo.ini')
        assert testapp.name_for_testing == 'foo'
        assert testapp.config_uri_for_testing == 'foo.ini'


class MockVirtualApp:

    def __init__(self, app, environ):
        self.app_for_testing = app
        self.environ_for_testing = environ


def test_make_dev_vapp():

    with mock.patch.object(standalone_dev_module, "VirtualApp", MockVirtualApp):
        with mock.patch.object(paster, "get_config_loader", MockConfigLoader):

            vapp = make_dev_vapp(app='something')
            assert vapp.app_for_testing == 'something'  # If we give something, it's just passed straight through

            vapp = make_dev_vapp()
            assert isinstance(vapp, MockVirtualApp)
            assert isinstance(vapp.app_for_testing, MockWsgiApp)
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.app_for_testing.config_uri_for_testing == 'development.ini'
            assert vapp.environ_for_testing == {"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST"}

            # The default is {"HTTP_ACCEPT": "application/json"}. If overridden, that must be included explicitly,
            # just in case the caller wants to omit it.
            some_environ = {"SOMETHING": "ELSE"}

            vapp = make_dev_vapp(environ=some_environ)
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.environ_for_testing == {"REMOTE_USER": "TEST", "SOMETHING": "ELSE"}

            vapp = make_dev_vapp(environ={})
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.environ_for_testing == {"REMOTE_USER": "TEST"}

            some_user = "some_user@cgap.hms.harvard.edu"

            vapp = make_dev_vapp(environ=some_environ, remote_user=some_user)
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.environ_for_testing == {"REMOTE_USER": some_user, "SOMETHING": "ELSE"}

            vapp = make_dev_vapp(remote_user=some_user)
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.environ_for_testing == {"HTTP_ACCEPT": "application/json", "REMOTE_USER": some_user}

            # It's possible also to get no remote user, by expressly passing None, since the default is TEST.
            # In that case, we expect no binding for REMOTE_USER in the environ:

            vapp = make_dev_vapp(remote_user=None)
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.environ_for_testing == {"HTTP_ACCEPT": "application/json"}

            vapp = make_dev_vapp(remote_user=None, environ={})
            assert vapp.app_for_testing.name_for_testing == 'app'
            assert vapp.environ_for_testing == {}
