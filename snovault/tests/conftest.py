import os
import time
import shutil
import pytest
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


@pytest.fixture
def boto_config_for_moto():
    """ Config string to be written to use moto """
    return (
        "[Boto]\nis_secure = False\nhttps_validate_certificates = False\nproxy_port = 3000\nproxy = 127.0.0.1"
    )


@pytest.yield_fixture(autouse=True)
def start_moto_server_sqs(boto_config_for_moto):
    """
    Writes a boto config file to ~/.boto.
    Spins off a moto server running sqs.
    At the end of the test session kill the moto_server and restore the config.
    """
    home = os.path.expanduser('~')  # resolve paths, get locations
    boto_config_location = os.path.join(home, '.boto')
    boto_config_orig_location = os.path.join(home, '.boto.orig')

    if os.path.exists(boto_config_location):  # persist old config
        shutil.move(boto_config_location, boto_config_orig_location)
    with open(boto_config_location, 'w+') as f:
        f.write(boto_config_for_moto)  # write new config

    server_args = ['moto_server', 'sqs', '-p3000']
    server = subprocess.Popen(server_args)
    time.sleep(5)  # let server start up

    yield

    if os.path.exists(boto_config_orig_location):  # restore original if need be
        shutil.move(boto_config_orig_location, boto_config_location)
    else:
        os.remove(boto_config_location)
    server.terminate()


def pytest_configure():
    import logging
    logging.basicConfig()
    logging.getLogger('snovault').setLevel(logging.INFO)
