import logging
import pytest
import sqlalchemy
import transaction as transaction_management
import webtest
import webtest.http
import zope.sqlalchemy

from contextlib import contextmanager
from dcicutils.misc_utils import ignored
from dcicutils.qa_utils import notice_pytest_fixtures
from transaction.interfaces import ISynchronizer
from urllib.parse import quote
from zope.interface import implementer

from ..app import configure_engine
from ..storage import Base
from .elasticsearch_fixture import server_process as elasticsearch_server_process
from .postgresql_fixture import (
    initdb, server_process as postgres_server_process, SNOVAULT_DB_TEST_PORT, make_snovault_db_test_url,
)


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
def engine_url(tmpdir_factory):
    notice_pytest_fixtures(tmpdir_factory)

    # Ideally this would use a different database on the same postgres server
    tmpdir = tmpdir_factory.mktemp('postgresql-engine', numbered=True)
    tmpdir = str(tmpdir)
    initdb(tmpdir)
    process = postgres_server_process(tmpdir)

    yield make_snovault_db_test_url(datadir=tmpdir)

    if process.poll() is None:
        process.terminate()
        process.wait()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def postgresql_server(tmpdir_factory):
    notice_pytest_fixtures(tmpdir_factory)
    tmpdir = tmpdir_factory.mktemp('postgresql', numbered=True)
    tmpdir = str(tmpdir)
    initdb(tmpdir)
    process = postgres_server_process(tmpdir)

    yield make_snovault_db_test_url(datadir=tmpdir)

    if process.poll() is None:
        process.terminate()
        # Should there be a process.wait() here? -kmp 14-Mar-2020


@pytest.fixture(scope='session')
def elasticsearch_host_port():
    return webtest.http.get_free_port()


@pytest.mark.fixture_cost(10)
@pytest.yield_fixture(scope='session')
def elasticsearch_server(tmpdir_factory, elasticsearch_host_port, remote_es):
    notice_pytest_fixtures(tmpdir_factory, elasticsearch_host_port, remote_es)
    if not remote_es:
        # spawn a new one
        host, port = elasticsearch_host_port
        tmpdir = tmpdir_factory.mktemp('elasticsearch', numbered=True)
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
    notice_pytest_fixtures(engine_url)
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
    notice_pytest_fixtures(conn)
    # ``server`` thread must be in same scope
    DBSession = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=conn), scopefunc=lambda: 0)
    zope.sqlalchemy.register(DBSession)
    return DBSession


@pytest.fixture(scope='session')
def DBSession(_DBSession, zsa_savepoints, check_constraints):
    return _DBSession


@pytest.yield_fixture
def external_tx(request, conn):
    notice_pytest_fixtures(request)
    # print('BEGIN external_tx')
    tx = conn.begin_nested()
    yield tx
    tx.rollback()
    # # The database should be empty unless a data fixture was loaded
    # for table in Base.metadata.sorted_tables:
    #     assert conn.execute(table.count()).scalar() == 0


@pytest.fixture
def transaction(request, external_tx, zsa_savepoints, check_constraints):
    notice_pytest_fixtures(request, external_tx, zsa_savepoints, check_constraints)
    transaction_management.begin()
    request.addfinalizer(transaction_management.abort)
    return transaction_management


@pytest.yield_fixture(scope='session')
def zsa_savepoints(conn):
    """ Place a savepoint at the start of the zope transaction

    This means failed requests rollback to the db state when they began rather
    than that at the start of the test.
    """
    notice_pytest_fixtures(conn)
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
    notice_pytest_fixtures(transaction, DBSession)
    return DBSession()


@pytest.yield_fixture(scope='session')
def check_constraints(conn, _DBSession):
    """
    Check deferred constraints on zope transaction commit.

    Deferred foreign key constraints are only checked at the outer transaction
    boundary, not at a savepoint. With the Pyramid transaction bound to a
    subtransaction check them manually.
    """
    notice_pytest_fixtures(_DBSession)

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


class ExecutionWatcher(object):

    """
    An ExecutionWatcher mediates the counting of something, which can scoped by the .expect() context manager, as in:
            with <watcher>.expect(expect=n):
               ...watched stuff happens here...
    It will report an error if the .notify() method doesn't see exactly n events that later survive filtering.
    If the right number does not happen, the error message will say what events were seen in
    the (active) watched region.
    """

    def __init__(self, filter=None):
        """
        Creates an ExecutionWatcher object with a given filter.
        """

        # The ._active flag is used to know whether to be recording and also to make sure we don't do nested calls.
        # We are only recording if we are inside a 'with execute_counter.expect(...):' context, which is to say
        # this ExecutionWatcher is ordinarily created within a use of the `execute_counter` fixture. See documentation
        # on that fixture for details of use.

        self._active = False  # The wacher is not reentrant
        self.reset()
        self.filter = filter

    def reset(self):
        """
        This can be used to reinitialize counting, though the semantics of that are questionable.
        It MIGHT have limited utility in the case of a db rollback, but we really don't use that.
        """
        # TODO: It might be that this method could usefully go away.
        self.events = []

    def notice(self, event):
        """
        Whatever it is that's being instrumented should send events here to get them counted.
        Any filter will be done later, so that if events are being filtered that shouldn't be,
        they will still show up in the error message.
        """
        if self._active:
            self.events.append(event)

    @contextmanager
    def expect(self, expected_count):
        """
        This context manager declares that within a certain scope, only a certain number of events are expected.
        See documentation on the
        """
        if self._active:
            raise RuntimeError("Attempt to enter execute_counter.expect(...) while it is already executing.")
        self._active = True
        self.reset()  # Probably redundant but just in case.
        yield
        annotated_events = []
        counted_events = 0
        for event in self.events:
            to_count = (not self.filter) or self.filter(event)
            if to_count:
                counted_events += 1
            annotated_events.append({'counted': to_count, 'event': event})
        assert counted_events == expected_count, (
                "Counter mismatch. Expected %s but got %s:\n%s"
                % (expected_count, counted_events, "\n".join([
            "{marker} {event}".format(marker="*" if ae['counted'] else " ", event=ae['event'])
            for ae in annotated_events])))
        self._active = False


@pytest.yield_fixture
def execute_counter(conn, zsa_savepoints, check_constraints, filter=None):
    """
    This fixture gives you a context manager that can be used to count calls to the SQLAlchemy 'execute' operation.

    Using this allows you to find out how many queries are needed to do a particular inquiry or set of inquiries
    using the ORM. Such a test is by nature a bit fragile, since it is bypassing abstraction boundaries, but I think
    the intent is to let the caller check whether joins are happening correctly. If they are not, the likely result
    is that you'll see more than the expected number of database queries.

    The intended use is in testing, where the fixture gives you a counter object that has a .expect method you can
    use as a context manager in order to bound the region of code you want to count. For example:

       def test_something(execute_counter):
           ... do some setup ...
           execute_counter.expect(2):  # <-- this is where you say how many DB executes you expect
               ... some ORM operation ...

    You'll get an error if the expectation is violated, and it will tell you what the operations were so that you can
    sort out whether you think it's an error or just a legit change in the underlying implementation.

    Note that counters are not reentrant. That is, you can't nest these calls with the same fixture. (If you call
    something else that has its own fixture, it should be fine.) There are no cases we care about where nesting was
    needed, so it simplifies the implementation.

    There is also now a filter argument to the execute_counter fixture that will allow, in the future if it is needed,
    for the possibility that some execute operations would be ignored (not counted) during the measured interval.
    (That was created for debugging and didn't end up being used, but still might be useful in the future.)
    """
    notice_pytest_fixtures(conn, zsa_savepoints, check_constraints)

    watcher = ExecutionWatcher(filter=filter)

    @sqlalchemy.event.listens_for(conn, 'after_cursor_execute')
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        ignored(conn, cursor)
        # Ignore the testing savepoints
        if zsa_savepoints.state == 'begun' and check_constraints.state != 'checking':
            watcher.notice({
                "zsa_savepoints-state": zsa_savepoints.state,
                "check_constraints-state": check_constraints.state,
                "statement": statement,
                "parameters": parameters,
                "context": context,
                "executemany": executemany,
            })

    yield watcher

    sqlalchemy.event.remove(conn, 'after_cursor_execute', after_cursor_execute)


@pytest.yield_fixture
def no_deps(conn, DBSession):
    notice_pytest_fixtures(conn, DBSession)

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
    notice_pytest_fixtures(app)
    return app


@pytest.mark.fixture_cost(100)
@pytest.yield_fixture(scope='session')
def wsgi_server(request, wsgi_server_app, wsgi_server_host_port):
    notice_pytest_fixtures(request, wsgi_server_app, wsgi_server_host_port)
    host, port = wsgi_server_host_port

    server = webtest.http.StopableWSGIServer.create(
        wsgi_server_app,
        host=host,
        port=port,
        threads=1,
        channel_timeout=60,
        cleanup_interval=10,
        expose_tracebacks=True,
        clear_untrusted_proxy_headers=True,
    )
    assert server.wait()

    yield 'http://%s:%s' % wsgi_server_host_port

    server.shutdown()
