from dcicutils.misc_utils import VirtualApp
from pyramid.paster import get_app


def make_standalone_app(*, config_file='development.ini', name='app'):
    """
    Creates an 'app' suitable for use in creating a testapp or VirtualApp.

    Args:
        name: a string naming the app (default 'app')
        config_file: a string naming the configuration file (default 'development.ini')

    Returns:

        An object, probably of type pyramid.router.Router, suitable for use in creating a
        webtest.TestApp or dcicutils.misc_utils.VirtualApp.

    """

    return get_app(config_file, name)


def make_dev_vapp(*, remote_user='TEST', environ=None, app=None, anonymous=False):
    """
    Creates a VirtualApp that, by default, simulates the TEST user of the development config,
    i.e., deployment on localhost (pserve development.ini).

    Args:
        remote_user: an email address or a reserved system token such as 'TEST' (the default).
        environ: a dictionary of environment bindings. The default is {"HTTP_ACCEPT": "application/json"}
        app: an app object to use instead of the default app newly generated on each call by by make_standalone_app()

    Returns:

        an object of type dcicutils.misc_utils.VirtualApp

    """

    environ = (environ if environ is not None else {'HTTP_ACCEPT': 'application/json'}).copy()

    if remote_user:
        environ['REMOTE_USER'] = remote_user

    app = app or make_standalone_app()
    return VirtualApp(app, environ)
