import pytest
from ..interfaces import BLOBS, CALCULATED_PROPERTIES, COLLECTIONS, CONNECTION, STORAGE, ROOT, TYPES, UPGRADER
from ..elasticsearch import ELASTIC_SEARCH

# Fixtures  for app


@pytest.fixture
def registry(app):
    return app.registry


@pytest.fixture
def blobs(registry):
    return registry[BLOBS]


@pytest.fixture
def calculated_properties(registry):
    return registry[CALCULATED_PROPERTIES]


@pytest.fixture
def collections(registry):
    return registry[COLLECTIONS]


@pytest.fixture
def connection(registry):
    return registry[CONNECTION]


@pytest.fixture
def elasticsearch(registry):
    return registry[ELASTIC_SEARCH]


@pytest.fixture
def storage(registry):
    return registry[STORAGE]


@pytest.fixture
def root(registry):
    return registry[ROOT]


@pytest.fixture
def types(registry):
    return registry[TYPES]


@pytest.fixture
def upgrader(registry):
    return registry[UPGRADER]
