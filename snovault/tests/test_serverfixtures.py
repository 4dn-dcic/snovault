import pytest
from urllib3.util import parse_url

from dcicutils.qa_utils import notice_pytest_fixtures
from .postgresql_fixture import (
    SNOVAULT_DB_TEST_DBNAME, SNOVAULT_DB_TEST_HOSTNAME, SNOVAULT_DB_TEST_PORT, SNOVAULT_DB_TEST_USERNAME,
)


def _check_postgresql_url(url):
    assert url.scheme == 'postgresql'
    assert url.hostname == SNOVAULT_DB_TEST_HOSTNAME
    assert url.port == SNOVAULT_DB_TEST_PORT
    assert url.auth == SNOVAULT_DB_TEST_USERNAME  # No password. That goes elsewhere
    assert url.path == '/' + SNOVAULT_DB_TEST_DBNAME
    assert url.query.startswith("host=")  # The next character is either / or %2F (hopefully the latter)


def test_postgresql_server(postgresql_server, request):
    notice_pytest_fixtures(request)
    url = parse_url(postgresql_server)
    _check_postgresql_url(url)


def test_engine_url(engine_url, request):
    notice_pytest_fixtures(request)
    url = parse_url(engine_url)
    _check_postgresql_url(url)
