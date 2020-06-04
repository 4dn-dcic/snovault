import pytest
import tempfile


# pytest_plugins = [
#     'snovault.tests.serverfixtures',
#     'snovault.tests.testappfixtures',
#     'snovault.tests.toolfixtures',
#     'snovault.tests.pyramidfixtures',
# ]


def pytest_addoption(parser):
    parser.addoption("--es", action="store", default="", dest='es',
        help="use a remote es for testing")
    parser.addoption("--aws-auth", action="store_true",
        help="connect using aws authorization")


@pytest.fixture(scope='session')
def remote_es(request):
    return request.config.getoption("--es")


@pytest.fixture(scope='session')
def aws_auth(request):
    return request.config.getoption("--aws-auth")


def pytest_configure():
    tempfile.tempdir = '/tmp'
