import contextlib
import json
import time
import webtest

from dcicutils.lang_utils import n_of
from dcicutils.misc_utils import ignored
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
                try:
                    raise NoNestedCommit()  # Raising an error will bypass an attempt to commit
                except BaseException:
                    pass
    except NoNestedCommit:
        pass


@contextlib.contextmanager
def local_collections(*, app, collections):
    with begin_nested(app=app, commit=False):
        create_mapping.run(app, collections=collections, skip_indexing=True, purge_queue=True)
        yield


def index_n_items_for_testing(indexer_testapp, n, *, max_tries=10, wait_seconds=1, initial_wait_seconds=None):
    tries_so_far = 0
    total_items_seen = 0
    current_wait_seconds = 0.5 * wait_seconds if initial_wait_seconds is None else initial_wait_seconds
    while True:
        time.sleep(current_wait_seconds)
        indexing_record = indexer_testapp.post_json('/index', {'record': True}).json
        items_this_time = indexing_record.get('indexing_count')
        assert items_this_time is not None, f"Expected an indexing_record, but got {json.dumps(indexing_record)}."
        total_items_seen += items_this_time
        if total_items_seen >= n:
            break
        assert tries_so_far < max_tries, (
            f"Attempt to index {n_of(n, 'item')}. Tried {max_tries} times, but saw only {total_items_seen}.")
        tries_so_far += 1
        current_wait_seconds = wait_seconds


def delay_rerun(*args):
    """ Rerun function for flaky """
    ignored(args)
    time.sleep(10)
    return True


def make_es_count_checker(n, *, es, namespaced_index):
    def es_count_checker():
        indexed_count = es.count(index=namespaced_index).get('count')
        assert indexed_count == n
        return n
    return es_count_checker
