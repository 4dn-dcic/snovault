from urllib3.util import parse_url

from .helpers import call_fixture
from .postgresql_fixture import (
    SNOVAULT_DB_TEST_DBNAME, SNOVAULT_DB_TEST_HOSTNAME, SNOVAULT_DB_TEST_PORT, SNOVAULT_DB_TEST_USERNAME,
)
from .serverfixtures import postgresql_server, engine_url


def _check_postgresql_url(url):
    assert url.scheme == 'postgresql'
    assert url.hostname == SNOVAULT_DB_TEST_HOSTNAME
    assert url.port == SNOVAULT_DB_TEST_PORT
    assert url.auth == SNOVAULT_DB_TEST_USERNAME  # No password. That goes elsewhere
    assert url.path == '/' + SNOVAULT_DB_TEST_DBNAME
    assert url.query.startswith("host=")  # The next character is either / or %2F (hopefully the latter)


def test_postgresql_server(request):
    with call_fixture(postgresql_server, request) as ps:
        url = parse_url(ps)
        _check_postgresql_url(url)


def test_engine_url(request):
    with call_fixture(engine_url, request) as eu:
        url = parse_url(eu)
        _check_postgresql_url(url)
