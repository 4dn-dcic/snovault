import base64
import codecs
import hashlib
import json
import os
import psycopg2
import subprocess
import zope.sqlalchemy

from pyramid.path import AssetResolver, caller_package
from pyramid.session import SignedCookieSessionFactory
from pyramid.settings import asbool
from pyramid_localroles import LocalRolesAuthorizationPolicy
from sqlalchemy import engine_from_config, event, orm
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


def set_postgresql_statement_timeout(engine, timeout=20 * 1000):
    """
    Prevent Postgres waiting indefinitely for a lock.
    """

    @event.listens_for(engine, 'connect')
    def connect(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET statement_timeout TO %d" % timeout)
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
    except:
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
        load_workbook(app, workbook_filename, docsdir, test=load_test_only)

    return app
