import logging
import pytest
import os
from moto import mock_aws
from ..project_defs import C4ProjectRegistry  # noQA
from ..elasticsearch.indexer_queue import QueueManager


@pytest.fixture(scope='session')
def mock_aws_env():
    """
    Mocks AWS services and sets static fake credentials to avoid SSO/token errors.
    """
    # Set static credentials to avoid boto trying credential_process, SSO, etc.
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    # Start moto mock
    with mock_aws():  # Add others like "dynamodb", "sqs" as needed
        yield

    # Optional: Clean up (in case you set these globally)
    for var in [
        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 
        'AWS_SECURITY_TOKEN', 'AWS_SESSION_TOKEN', 
        'AWS_DEFAULT_REGION'
    ]:
        os.environ.pop(var, None)


# required so that db transactions are properly rolled back in tests
@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


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
