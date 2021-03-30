from dcicutils.misc_utils import VirtualApp
from pyramid.paster import get_app


def make_standalone_app():
    return get_app('development.ini', 'app')  # this file should exist in the main portal repo


def make_dev_vapp(remote_user=None, environ=None, app=None):
    """ Creates a VirtualApp simulating the TEST user by default.
        Intended for use with the local deployment (pserve development.ini).
    """

    environ = environ or {'HTTP_ACCEPT': 'application/json'}

    environ['REMOTE_USER'] = remote_user or 'TEST'

    app = app or make_standalone_app()
    return VirtualApp(app, environ)
