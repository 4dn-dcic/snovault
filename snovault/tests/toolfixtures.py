import pytest
from dcicutils.qa_utils import notice_pytest_fixtures
from ..interfaces import BLOBS, CALCULATED_PROPERTIES, COLLECTIONS, CONNECTION, STORAGE, ROOT, TYPES, UPGRADER
from ..elasticsearch import ELASTIC_SEARCH

# Fixtures  for app


@pytest.fixture
def registry(app):
    notice_pytest_fixtures(app)
    return app.registry


@pytest.fixture
def blobs(registry):
    notice_pytest_fixtures(registry)
    return registry[BLOBS]


@pytest.fixture
def calculated_properties(registry):
    notice_pytest_fixtures(registry)
    return registry[CALCULATED_PROPERTIES]


@pytest.fixture
def collections(registry):
    notice_pytest_fixtures(registry)
    return registry[COLLECTIONS]


@pytest.fixture
def connection(registry):
    notice_pytest_fixtures(registry)
    return registry[CONNECTION]


@pytest.fixture
def elasticsearch(registry):
    notice_pytest_fixtures(registry)
    return registry[ELASTIC_SEARCH]


@pytest.fixture
def storage(registry):
    notice_pytest_fixtures(registry)
    return registry[STORAGE]


@pytest.fixture
def root(registry):
    notice_pytest_fixtures(registry)
    return registry[ROOT]


@pytest.fixture
def types(registry):
    notice_pytest_fixtures(registry)
    return registry[TYPES]


@pytest.fixture
def upgrader(registry):
    notice_pytest_fixtures(registry)
    return registry[UPGRADER]
