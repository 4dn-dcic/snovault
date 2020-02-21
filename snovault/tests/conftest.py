import os
import time
import pytest
import logging
from moto import mock_sqs
import subprocess


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


@pytest.yield_fixture(scope='session', autouse=True)
def start_moto_server_sqs():
    """
    Spins off a moto server running sqs.
    """
    server_args = ['moto_server', 'sqs', '-p3000']
    server = subprocess.Popen(server_args, stdout=subprocess.PIPE)
    os.environ['SQS_URL'] = 'http://localhost:3000'
    time.sleep(5)  # let server start up

    yield

    del os.environ['SQS_URL']
    server.terminate()


def pytest_configure():
    logging.basicConfig()
    logging.getLogger('snovault').setLevel(logging.INFO)
