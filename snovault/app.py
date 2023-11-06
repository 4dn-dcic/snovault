import base64
import codecs
import hashlib
import json
import os
import psycopg2
import psycopg2.extensions
import subprocess
import zope.sqlalchemy

from dcicutils.misc_utils import ignored, ignorable
from pyramid.config import Configurator
from pyramid.path import AssetResolver, caller_package
from pyramid.session import SignedCookieSessionFactory
from pyramid.settings import asbool
from .local_roles import LocalRolesAuthorizationPolicy
from sqlalchemy import engine_from_config, event, orm  # , text as psql_text
from webob.cookies import JSONSerializer

from .interfaces import DBSESSION

from .elasticsearch import APP_FACTORY
from .json_renderer import json_renderer
from .storage import Base


STATIC_MAX_AGE = 0


def json_asset(spec, **kw):
    utf8 = codecs.getreader("utf-8")
    asset = AssetResolver(caller_package()).resolve(spec)
    return json.load(utf8(asset.stream()), **kw)


def changelogs(config):
    config.add_static_view(
        'profiles/changelogs', 'schemas/changelogs', cache_max_age=STATIC_MAX_AGE)


def configure_engine(settings):
    engine_url = settings['sqlalchemy.url']
    engine_opts = {}
    if engine_url.startswith('postgresql'):
        if settings.get('indexer_worker'):
            application_name = 'indexer_worker'
        elif settings.get('indexer'):
            application_name = 'indexer'
        else:
            application_name = 'app'
        engine_opts = dict(
            isolation_level='REPEATABLE READ',
            json_serializer=json_renderer.dumps,
            connect_args={'application_name': application_name}
        )
    engine = engine_from_config(settings, 'sqlalchemy.', **engine_opts)
    if engine.url.drivername == 'postgresql':
        timeout = settings.get('postgresql.statement_timeout')
        if timeout:
            timeout = int(timeout) * 1000
            set_postgresql_statement_timeout(engine, timeout)
    return engine


def set_postgresql_statement_timeout(engine, timeout: int = 20 * 1000):
    """
    Prevent Postgres waiting indefinitely for a lock.

    :param engine: a database engine
    :param timeout: a number of milliseconds to set for as statement_timeout
    """

    @event.listens_for(engine, 'connect')
    def connect(dbapi_connection, connection_record):
        ignored(connection_record)
        timeout_ms = timeout
        if not isinstance(timeout_ms, int):
            # This coercion will truncate 3.5 to 3, but so would the %d below,
            # and we have long used that. But the real purpose of introducing
            # this coercion is to get a ValueError if a string other than a
            # representation of a number slips through, to seal out accidental injection.
            # -kmp 6-Apr-2023
            timeout_ms = int(timeout_ms)
        cursor = dbapi_connection.cursor()
        try:
            # cursor: psycopg2.extensions.cursor
            # This call to psycopg2.extensions.cursor.execute expects a real string. Giving it an sqlalchemy.text
            # object will fail because something will try to do a boolean test, probably "if thing_to_execute:..."
            # and __bool__ is not defined on sqlalchemy.txt
            # Bottom line: Cannot wrap this string with psql_text(...) like we do elsewhere. It's not ready.
            # Might be we could do such a wrapper if we called execute on some other object.
            cursor.execute("SET statement_timeout = %d;" % timeout_ms)
        except psycopg2.Error:
            dbapi_connection.rollback()
        finally:
            cursor.close()
            dbapi_connection.commit()


def json_from_path(path, default=None):
    if path is None:
        return default
    return json.load(open(path))


def configure_dbsession(config, clear_data=False):
    """
    Create a sqlalchemy engine and a session that uses it, the latter of which
    is added to the registry. Handle some extra registration
    """
    settings = config.registry.settings
    DBSession = settings.pop(DBSESSION, None)

    # handle creating the database engine separately with indexer_worker
    if DBSession is None and not settings.get('indexer_worker'):
        engine = configure_engine(settings)

        # useful for test instances where we want to clear the data
        if clear_data:
            Base.metadata.drop_all(engine)

        if asbool(settings.get('create_tables', False)):
            Base.metadata.create_all(engine)

        DBSession = orm.scoped_session(orm.sessionmaker(bind=engine))
        zope.sqlalchemy.register(DBSession)

    config.registry[DBSESSION] = DBSession


def session(config):
    """ To create a session secret on the server:

    $ cat /dev/urandom | head -c 256 | base64 > session-secret.b64
    """
    settings = config.registry.settings
    if 'session.secret' in settings:
        secret = settings['session.secret'].strip()
        if secret.startswith('/'):
            secret = open(secret).read()
            secret = base64.b64decode(secret)
    else:
        secret = os.urandom(256)
    # auth_tkt has no timeout set
    # cookie will still expire at browser close
    if 'session.timeout' in settings:
        timeout = int(settings['session.timeout'])
    else:
        timeout = 60 * 60 * 24
    session_factory = SignedCookieSessionFactory(
        secret=secret,
        timeout=timeout,
        reissue_time=2**32,  # None does not work
        serializer=JSONSerializer(),
    )
    config.set_session_factory(session_factory)


def app_version(config):
    try:
        # For each of the next two subprocess calls, if there's an error, 'git' call will write to stderr,
        # but we don't care about that output, so we have muffled it by directing it to /dev/null.
        # If you're debugging this and want to know what's being ignored, comment out the 'stderr=' line.
        # -kmp 8-Feb-2020
        version = subprocess.check_output(
            ['git', '-C', os.path.dirname(__file__), 'describe'],
            stderr=subprocess.DEVNULL,
        ).decode('utf-8').strip()
        diff = subprocess.check_output(
            ['git', '-C', os.path.dirname(__file__), 'diff', '--no-ext-diff'],
            stderr=subprocess.DEVNULL,
        )
        if diff:
            version += '-patch' + hashlib.sha1(diff).hexdigest()[:7]
    except Exception:
        version = os.environ.get("ENCODED_VERSION", "test_version")
    config.registry.settings['snovault.app_version'] = version


def main(global_config, **local_config):
    """
    This function returns a Pyramid WSGI application.
    """
    settings = global_config
    settings.update(local_config)
    settings['snovault.jsonld.namespaces'] = json_asset('encoded:schemas/namespaces.json')
    settings['snovault.jsonld.terms_namespace'] = 'https://www.encodeproject.org/terms/'
    settings['snovault.jsonld.terms_prefix'] = 'encode'
    hostname_command = settings.get('hostname_command', '').strip()
    if hostname_command:
        hostname = subprocess.check_output(hostname_command, shell=True).strip()
        settings.setdefault('persona.audiences', '')
        settings['persona.audiences'] += '\nhttp://%s' % hostname
        settings['persona.audiences'] += '\nhttp://%s:6543' % hostname

    config = Configurator(settings=settings)
    config.registry[APP_FACTORY] = main  # used by mp_indexer
    config.include(app_version)

    config.include('pyramid_multiauth')  # must be before calling set_authorization_policy
    # Override default authz policy set by pyramid_multiauth
    config.set_authorization_policy(LocalRolesAuthorizationPolicy())
    config.include(session)
    config.include('.persona')

    config.include(configure_dbsession)
    config.include('snovault')
    config.commit()  # commit so search can override listing

    config.include('.renderers')

    if 'elasticsearch.server' in config.registry.settings:
        config.include('snovault.elasticsearch')

    config.include(changelogs)

    if asbool(settings.get('testing', False)):
        config.include('.tests.testing_views')

    # Load upgrades last so that all views (including testing views) are
    # registered.
    config.include('.upgrade')

    app = config.make_wsgi_app()

    workbook_filename = settings.get('load_workbook', '')
    load_test_only = asbool(settings.get('load_test_only', False))
    docsdir = settings.get('load_docsdir', None)
    if docsdir is not None:
        docsdir = [path.strip() for path in docsdir.strip().split('\n')]
    if workbook_filename:
        ignorable(docsdir, load_test_only)  # The commented-out reference to load_workbook below would use these vars.
        raise NotImplementedError("The load_workbook option is not implemented.")
        # TODO: load_workbook is undefined. where is it supposed to come from? -kmp 7-Aug-2022
        #       The definition would be in fourfront or cgap in src/encoded/__init__.py, which has a function
        #       similar to our 'main' here but with more functionality. Need to rethink that modularity so there
        #       is not code duplication and code that won't work. Could backport load_workbook to here, for example.
        #       -kmp 15-Sep-2022
        # load_workbook(app, workbook_filename, docsdir, test=load_test_only)

    # TODO: Maybe we should keep better track of what settings are used and not, and warn about unused options?
    return app
