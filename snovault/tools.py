import contextlib
import webtest

from .interfaces import DBSESSION
from .elasticsearch import create_mapping


def make_testapp(app, *, http_accept=None, remote_user=None):
    """
    By default, makes a testapp with environ={"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST"}.
    Arguments can be used to override these defaults. An explicit None accepts the default.

    :param app: a Pyramid app object.
    :param http_accept: The value of HTTP_ACCEPT in the testapp's environ, or None accepting 'application/json'.
    :param remote_user: The value of REMOTE_USER in the testapp's environ, or None accepting 'TEST'.
    """
    environ = {
        'HTTP_ACCEPT': http_accept or 'application/json',
        'REMOTE_USER': remote_user or 'TEST',
    }
    testapp = webtest.TestApp(app, environ)
    return testapp


def make_htmltestapp(app):
    """Makes a testapp with environ={"HTTP_ACCEPT": "text/html", "REMOTE_USER": "TEST"}"""
    return make_testapp(app, http_accept='text/html')


def make_authenticated_testapp(app):
    """Makes a testapp with environ={"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST_AUTHENTICATED"}"""
    return make_testapp(app, remote_user='TEST_AUTHENTICATED')


def make_submitter_testapp(app):
    """Makes a testapp with environ={"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST_SUBMITTER"}"""
    return make_testapp(app, remote_user='TEST_SUBMITTER')


def make_indexer_testapp(app):
    """Makes a testapp with environ={"HTTP_ACCEPT": "application/json", "REMOTE_USER": "INDEXER"}"""
    return make_testapp(app, remote_user='INDEXER')


def make_embed_testapp(app):
    """Makes a testapp with environ={"HTTP_ACCEPT": "application/json", "REMOTE_USER": "EMBED"}"""
    return make_testapp(app, remote_user='EMBED')


class NoNestedCommit(BaseException):
    """
    This is a pseudo-error class to be used as a special control construct
    only for the purpose of implementing begin_nested.
    """
    pass


@contextlib.contextmanager
def begin_nested(*, app, commit=True):
    session = app.registry[DBSESSION]
    connection = session.connection().connect()
    try:
        with connection.begin_nested():
            yield
            if not commit:
                raise NoNestedCommit()  # Raising an error will bypass an attempt to commit
    except NoNestedCommit:
        pass


@contextlib.contextmanager
def local_collections(*, app, collections):
    with begin_nested(app=app, commit=False):
        create_mapping.run(app, collections=collections, skip_indexing=True, purge_queue=True)
        yield
