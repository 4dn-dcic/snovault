import logging
import pytest
from unittest.mock import patch
from botocore.credentials import Credentials
from moto import mock_aws
from ..project_defs import C4ProjectRegistry  # noQA
from ..elasticsearch.indexer_queue import QueueManager


# required so that db transactions are properly rolled back in tests
@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture(autouse=True, scope='session')
def moto_with_fake_credentials():
    """
    Auto-applied fixture that mocks AWS and injects static credentials,
    avoiding SSO, credential_process, and expired token issues.
    Works with Moto.
    """
    # Real credentials object to prevent MagicMock issues
    creds = Credentials(
        access_key='test-access-key',
        secret_key='test-secret-key',
        token='test-session-token'
    )

    with mock_aws():
        with patch('boto3.Session.get_credentials', return_value=creds), \
             patch('botocore.credentials.CredentialResolver.load_credentials', return_value=creds):
            yield


# def _check_server_is_up(output):
#     """ Polls the given output file to detect
#
#         :args output: file to which server is piping out
#         :returns: True if server is up, False if failed
#     """
#     tries = 5
#     while tries > 0:
#         output.seek(0)  # should be first thing to be output.
#         out = output.read()
#         if 'Running' in out.decode('utf-8'):
#             return True
#         tries -= 1
#         time.sleep(1)  # give it a sec
#     return False
#
#
# @pytest.fixture(scope='session', autouse=True)
# def start_moto_server_sqs():
#     """
#     Spins off a moto server running sqs, yields to the tests and cleans up.
#     """
#     delete_sqs_url = 'SQS_URL' not in os.environ
#     old_sqs_url = os.environ.get('SQS_URL', None)
#     server_output = tempfile.TemporaryFile()
#     server = None
#     try:
#         try:
#             os.environ['SQS_URL'] = 'http://localhost:3000'  # must exists globally because of MPIndexer
#             server_args = ['poetry', 'run', 'moto_server', 'sqs', '-p3000']
#             server = subprocess.Popen(server_args, stdout=server_output, stderr=server_output)
#             time.sleep(5)
#             assert _check_server_is_up(server_output)
#         except AssertionError:
#             server_output.seek(0)
#             raise AssertionError(server_output.read().decode('utf-8'))
#         except Exception as e:
#             raise Exception('Encountered an exception bringing up the server: %s' % str(e))
#
#         yield  # run tests
#
#     finally:
#         if delete_sqs_url:
#             del os.environ['SQS_URL']
#         else:
#             os.environ['SQS_URL'] = old_sqs_url
#         if server:
#             server.terminate()


def pytest_configure():
    logging.basicConfig()
    logging.getLogger('snovault').setLevel(logging.INFO)
    QueueManager.PURGE_QUEUE_SLEEP_FOR_SAFETY = True
