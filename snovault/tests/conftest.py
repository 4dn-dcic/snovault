import os
import time
import tempfile
import pytest
import logging
import subprocess
from unittest import mock


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


def check_server_is_up(server, output):
    """ Polls the moto server to see if it is actually up

        :args server: process object from subprocess.Popen to check
        :returns: True if server is up, False if failed
    """
    tries = 5
    while tries > 0:
        output.seek(0)  # should be first thing to be output.
        out = output.read()
        if 'Running' in out.decode('utf-8'):
            return True
        tries -= 1
        time.sleep(1)  # give it a sec
    return False


@pytest.yield_fixture(scope='session', autouse=True)
def start_moto_server_sqs():
    """
    Spins off a moto server running sqs, yields to the tests and cleans up.
    """
    os.environ['SQS_URL'] = 'http://localhost:3000'  # must exists globally because of MPIndexer
    server_output = tempfile.TemporaryFile()
    server = None
    try:
        try:
            server_args = ['moto_server', 'sqs', '-p3000']
            server = subprocess.Popen(server_args, stdout=server_output, stderr=server_output)
            assert check_server_is_up(server, server_output)
        except AssertionError:
            raise AssertionError('Could not get moto server up')
        except Exception as e:
            raise Exception('Encountered an exception bringing up the server: %s' % str(e))

        yield  # run tests

    finally:
        del os.environ['SQS_URL']
        if server:
            server.terminate()


def pytest_configure():
    logging.basicConfig()
    logging.getLogger('snovault').setLevel(logging.INFO)
