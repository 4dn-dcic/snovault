import contextlib


@contextlib.contextmanager
def call_fixture(yield_fixture, *args, **kwargs):
    for fixture in yield_fixture(*args, **kwargs):
        yield fixture
