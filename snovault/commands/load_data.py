import argparse
import logging
import structlog

from dcicutils.env_utils import permit_load_data
from dcicutils.common import APP_CGAP
from dcicutils.misc_utils import PRINT

from pyramid.paster import get_app
from pyramid.path import DottedNameResolver
from .. import configure_dbsession


log = structlog.getLogger(__name__)


EPILOG = __doc__


# should be overridden in downstream application to pass a different app
def load_data_should_proceed(env, allow_prod, app=None):
    """ Returns True on whether or not load_data should proceed.

    :param env: env we are on
    :param allow_prod: prod argument from argparse, defaults to False
    :param app: app type, one of cgap, fourfront (enums from dcicutils.common)
    :return: True if load_data should continue, False otherwise
    """
    if not app:
        app = APP_CGAP  # this fallback is somewhat reasonable
    return permit_load_data(envname=env, allow_prod=allow_prod, orchestrated_app=app)  # noqa


def main(simulated_args=None):
    logging.basicConfig()
    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('encoded').setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is specified wrong here.
        description="Load Data", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--prod', action='store_true',
                        help="must be set to confirm this action is intended to happen on a production server")
    parser.add_argument('--overwrite', action='store_true',
                        help="must be set to update existing uuids with patch")
    args = parser.parse_args(simulated_args)

    # get the pyramids app
    app = get_app(args.config_uri, args.app_name)

    # create db schema
    configure_dbsession(app)

    env = app.registry.settings.get('env.name', '')

    load_test_data = app.registry.settings.get('load_test_data')
    allow_prod = args.prod
    PRINT("load_data: load_test_data function is %s" % (load_test_data))
    load_test_data = DottedNameResolver().resolve(load_test_data)

    if load_data_should_proceed(env, allow_prod):
        load_test_data(app, args.overwrite)


if __name__ == "__main__":
    main()
