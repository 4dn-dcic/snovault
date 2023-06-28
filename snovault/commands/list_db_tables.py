import argparse
import logging
import structlog
# import transaction

from dcicutils.env_utils import is_stg_or_prd_env
from dcicutils.lang_utils import disjoined_list
from dcicutils.misc_utils import PRINT, get_error_message
from pyramid.paster import get_app
# from snovault import DBSESSION
# from snovault.storage import Base
# from snovault.elasticsearch.create_mapping import run as run_create_mapping
# from sqlalchemy import MetaData
from typing import Optional, List
# from zope.sqlalchemy import mark_changed
from .. import configure_dbsession
from ..sqlalchemy_tools import PyramidAppManager
from ..project_app import app_project


logger = structlog.getLogger(__name__)


EPILOG = __doc__


def list_db_tables(app):
    """
    Given a pyramids app that has a configured DB session, will list the contents of all DB tables

    Args:
        app: Pyramid application

    Returns:
        bool: True if successful, False if error encountered
    """

    app_manager = PyramidAppManager(app)

    with app_manager.connection() as connection:
        for table_name in app_manager.ordered_table_names:
            n = connection.execute(f"SELECT COUNT(*) FROM {table_name};").one()
            print(f" Table {table_name}: {n}")


SKIPPING_LIST_ATTEMPT = 'Skipping the attempt to list DB.'


def run_list_db_tables(app, only_envs: Optional[List[str]] = None, skip_es: bool = False,
                    allow_prod: bool = False) -> bool:
    """
    This function lists information from the DB.

    For safety, this function will return without side-effect if ...
    - The current environment is any production system (and allow_prod is not given).
    - The current environment is not a member of the `only_envs` argument (list).

    Args:
        app: Pyramid application
        only_envs (list): a list of env names that are the only envs where this action will run
        allow_prod (bool): if True, allows running on envs that are set to the staging or prod
                           env in the GLOBAL_ENV_BUCKET (main.ecosystem)

    Returns:
        bool: True if DB was listed.
    """
    current_env = app.registry.settings.get('env.name', 'local')

    if is_stg_or_prd_env(current_env) and not allow_prod:
        logger.error(f"list-db-tables: This action cannot be performed on env {current_env}"
                     f" because it is a production-class (stg or prd) environment."
                     f" {SKIPPING_LIST_ATTEMPT}")
        return False

    if only_envs and current_env not in only_envs:
        logger.error(f"list-db-tables: The current environment, {current_env}, is not {disjoined_list(only_envs)}."
                     f" {SKIPPING_LIST_ATTEMPT}")
        return False

    logger.info('list-db-tables: Listing DB tables...')
    try:
        list_db_tables(app)
    except Exception as e:
        logger.info(f"list-db-tables failed. {get_error_message(e)}")
        return False
    logger.info("list-db-tables succeeded.")
    return True


def main(simulated_args=None):
    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is specified wrong here.
        description='List DB Contents', epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--config_uri', help='path to configfile', default='development.ini')
    parser.add_argument('--app-name', help='Pyramid app name in configfile', default='app')
    parser.add_argument('--only-if-env', '--only-if-envs', dest='only_envs', default=None,
                        help=("A comma-separated list of envs where this action is allowed to run."
                              " If omitted, any env is OK to run."))
    parser.add_argument("--confirm", action="store_true", dest="confirm", default=None,
                        help="Specify --confirm to require interactive confirmation.")
    parser.add_argument("--no-confirm", action="store_false", dest="confirm", default=None,
                        help="Specify --no-confirm to suppress interactive confirmation.")
    parser.add_argument('--allow-prod', action='store_true', default=False,
                        help='DANGER: If set, will allow running this command on an env that is staging or prod')
    parser.add_argument('--log', action='store_true', default=False,
                        help='Set loglevel to DEBUG. Otherwise it will be ERROR.')
    args = parser.parse_args(simulated_args)

    confirm = args.confirm
    app_name = args.app_name
    config_uri = args.config_uri
    only_envs = args.only_envs
    allow_prod = args.allow_prod
    log = args.log

    logging.basicConfig()
    #project = app_project(initialize=True)
    project = app_project()
    # Loading app will have configured from config file. Reconfigure here:
    if log:
        logging.getLogger(project.NAME).setLevel(logging.DEBUG)
    else:
        logging.getLogger(project.NAME).setLevel(logging.ERROR)

    if confirm is None:
        confirm = False   # not only_envs  # If only_envs is supplied, we have better protection so don't need to confirm

    # get the pyramids app
    app = get_app(config_uri, app_name)

    # create db schema
    configure_dbsession(app)

    only_envs = [x for x in (only_envs or "").split(',') if x]

    if confirm:
        env_to_confirm = app.registry.settings.get('env.name', 'local')
        env_confirmation = input(f'This will list DB contents for environment {env_to_confirm}.\n'
                                 f' Type the env name to confirm: ')
        if env_confirmation != env_to_confirm:
            PRINT(f"NOT confirmed. {SKIPPING_LIST_ATTEMPT}")
            return

    # actually run. split this out for easy testing
    run_list_db_tables(app=app, only_envs=only_envs, allow_prod=allow_prod)


if __name__ == '__main__':
    main()
