import logging
import pytest
import sqlalchemy
import transaction as transaction_management
import webtest
import webtest.http
import zope.sqlalchemy

from contextlib import contextmanager
from transaction.interfaces import ISynchronizer
from urllib.parse import quote
from zope.interface import implementer

from ..app import configure_engine
from ..storage import Base
from .elasticsearch_fixture import server_process as elasticsearch_server_process
from .postgresql_fixture import initdb, server_process as postgres_server_process


def pytest_configure():
    logging.basicConfig(format='')
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

    class Shorten(logging.Filter):
        max_len = 500

        def filter(self, record):
            if record.msg == '%r':
                record.msg = record.msg % record.args
                record.args = ()
            if len(record.msg) > self.max_len:
                record.msg = record.msg[:self.max_len] + '...'
            return True

    logging.getLogger('sqlalchemy.engine.base.Engine').addFilter(Shorten())


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def engine_url(request):
    # Ideally this would use a different database on the same postgres server
    tmpdir = request.config._tmpdirhandler.mktemp('postgresql-engine', numbered=True)
    tmpdir = str(tmpdir)
    initdb(tmpdir)
    process = postgres_server_process(tmpdir)

    yield 'postgresql://postgres@:5432/postgres?host=%s' % quote(tmpdir)

    if process.poll() is None:
        process.terminate()
        process.wait()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def postgresql_server(request):
    tmpdir = request.config._tmpdirhandler.mktemp('postgresql', numbered=True)
    tmpdir = str(tmpdir)
    initdb(tmpdir)
    process = postgres_server_process(tmpdir)

    yield 'postgresql://postgres@:5432/postgres?host=%s' % quote(tmpdir)

    if process.poll() is None:
        process.terminate()


@pytest.fixture(scope='session')
def elasticsearch_host_port():
    return webtest.http.get_free_port()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def elasticsearch_server(request, elasticsearch_host_port, remote_es):
    if not remote_es:
        # spawn a new one
        host, port = elasticsearch_host_port
        tmpdir = request.config._tmpdirhandler.mktemp('elasticsearch', numbered=True)
        tmpdir = str(tmpdir)
        process = elasticsearch_server_process(str(tmpdir), host=host, port=port)

        yield 'http://%s:%d' % (host, port)

        if process.poll() is None:
            process.terminate()
            process.wait()
    else:
        yield remote_es


# http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#joining-a-session-into-an-external-transaction
# By binding the SQLAlchemy Session to an external transaction multiple testapp
# requests can be rolled back at the end of the test.

@pytest.yield_fixture(scope='session')
def conn(engine_url):

    engine_settings = {
        'sqlalchemy.url': engine_url,
    }

    engine = configure_engine(engine_settings)
    conn = engine.connect()
    tx = conn.begin()
    try:
        Base.metadata.create_all(bind=conn)
        yield conn
    finally:
        tx.rollback()
        conn.close()
        engine.dispose()


@pytest.fixture(scope='session')
def _DBSession(conn):
    # ``server`` thread must be in same scope
    DBSession = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=conn), scopefunc=lambda: 0)
    zope.sqlalchemy.register(DBSession)
    return DBSession


@pytest.fixture(scope='session')
def DBSession(_DBSession, zsa_savepoints, check_constraints):
    return _DBSession


@pytest.yield_fixture
def external_tx(request, conn):
    # print('BEGIN external_tx')
    tx = conn.begin_nested()
    yield tx
    tx.rollback()
    # # The database should be empty unless a data fixture was loaded
    # for table in Base.metadata.sorted_tables:
    #     assert conn.execute(table.count()).scalar() == 0


@pytest.fixture
def transaction(request, external_tx, zsa_savepoints, check_constraints):
    transaction_management.begin()
    request.addfinalizer(transaction_management.abort)
    return transaction_management


@pytest.yield_fixture(scope='session')
def zsa_savepoints(conn):
    """ Place a savepoint at the start of the zope transaction

    This means failed requests rollback to the db state when they began rather
    than that at the start of the test.
    """

    @implementer(ISynchronizer)
    class Savepoints(object):
        def __init__(self, conn):
            self.conn = conn
            self.sp = None
            self.state = None

        def beforeCompletion(self, transaction):
            pass

        def afterCompletion(self, transaction):
            # txn be aborted a second time in manager.begin()
            if self.sp is None:
                return
            if self.state == 'commit':
                self.state = 'completion'
                self.sp.commit()
            else:
                self.state = 'abort'
                self.sp.rollback()
            self.sp = None
            self.state = 'done'

        def newTransaction(self, transaction):
            self.state = 'new'
            self.sp = self.conn.begin_nested()
            self.state = 'begun'
            transaction.addBeforeCommitHook(self._registerCommit)

        def _registerCommit(self):
            self.state = 'commit'

    zsa_savepoints = Savepoints(conn)

    transaction_management.manager.registerSynch(zsa_savepoints)

    yield zsa_savepoints
    transaction_management.manager.unregisterSynch(zsa_savepoints)


@pytest.fixture
def session(transaction, DBSession):
    """ Returns a setup session

    Depends on transaction as storage relies on some interaction there.
    """
    return DBSession()


@pytest.yield_fixture(scope='session')
def check_constraints(conn, _DBSession):
    '''Check deffered constraints on zope transaction commit.

    Deferred foreign key constraints are only checked at the outer transaction
    boundary, not at a savepoint. With the Pyramid transaction bound to a
    subtransaction check them manually.
    '''

    @implementer(ISynchronizer)
    class CheckConstraints(object):
        def __init__(self, conn):
            self.conn = conn
            self.state = None

        def beforeCompletion(self, transaction):
            pass

        def afterCompletion(self, transaction):
            pass

        def newTransaction(self, transaction):

            @transaction.addBeforeCommitHook
            def set_constraints():
                self.state = 'checking'
                session = _DBSession()
                session.flush()
                sp = self.conn.begin_nested()
                try:
                    self.conn.execute('SET CONSTRAINTS ALL IMMEDIATE')
                except:
                    sp.rollback()
                    raise
                else:
                    self.conn.execute('SET CONSTRAINTS ALL DEFERRED')
                finally:
                    sp.commit()
                    self.state = None

    check_constraints = CheckConstraints(conn)

    transaction_management.manager.registerSynch(check_constraints)

    yield check_constraints

    transaction_management.manager.unregisterSynch(check_constraints)


@pytest.yield_fixture
def execute_counter(conn, zsa_savepoints, check_constraints):
    """ Count calls to execute
    """

    class Counter(object):
        def __init__(self):
            self.reset()
            self.conn = conn

        def reset(self):
            self.count = 0

        @contextmanager
        def expect(self, count):
            start = self.count
            yield
            difference = self.count - start
            assert difference == count

    counter = Counter()

    @sqlalchemy.event.listens_for(conn, 'after_cursor_execute')
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        # Ignore the testing savepoints
        if zsa_savepoints.state != 'begun' or check_constraints.state == 'checking':
            return
        counter.count += 1

    yield counter

    sqlalchemy.event.remove(conn, 'after_cursor_execute', after_cursor_execute)


@pytest.yield_fixture
def no_deps(conn, DBSession):

    session = DBSession()

    @sqlalchemy.event.listens_for(session, 'after_flush')
    def check_dependencies(session, flush_context):
        assert not flush_context.cycles

    @sqlalchemy.event.listens_for(conn, "before_execute", retval=True)
    def before_execute(conn, clauseelement, multiparams, params):
        return clauseelement, multiparams, params

    yield

    sqlalchemy.event.remove(session, 'before_flush', check_dependencies)

@pytest.fixture(scope='session')
def wsgi_server_host_port():
    return webtest.http.get_free_port()


@pytest.fixture(scope='session')
def wsgi_server_app(app):
    return app


@pytest.mark.fixture_cost(100)
@pytest.yield_fixture(scope='session')
def wsgi_server(request, wsgi_server_app, wsgi_server_host_port):
    host, port = wsgi_server_host_port

    server = webtest.http.StopableWSGIServer.create(
        wsgi_server_app,
        host=host,
        port=port,
        threads=1,
        channel_timeout=60,
        cleanup_interval=10,
        expose_tracebacks=True,
    )
    assert server.wait()

    yield 'http://%s:%s' % wsgi_server_host_port

    server.shutdown()
