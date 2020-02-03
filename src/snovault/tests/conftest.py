import pytest

pytest_plugins = [
    'snovault.tests.serverfixtures',
    'snovault.tests.testappfixtures',
    'snovault.tests.toolfixtures',
    'snovault.tests.pyramidfixtures',
]


# required so that db transactions are properly rolled back in tests
@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


def pytest_configure():
    import logging
    logging.basicConfig()
    logging.getLogger('snovault').setLevel(logging.INFO)
