import os
import pytest
import subprocess
from .postgresql_fixture import SNOVAULT_DB_TEST_PORT, DEFAULT_SNOVAULT_DB_TEST_PORT
from ..util import environ_bindings

def test_snovault_db_test_port():

    command1 = "source %s/bin/activate" % os.environ['VIRTUAL_ENV']
    command2 = "python -c 'from snovault.tests.postgresql_fixture import SNOVAULT_DB_TEST_PORT; print(SNOVAULT_DB_TEST_PORT)'"

    print("os.environ.get('SNOVAULT_DB_TEST_PORT') = %r" % os.environ.get('SNOVAULT_DB_TEST_PORT'))

    def _check_db_port():
        # We need to load the module freshly to find out if this setting is working, so we ask a subprocess.
        command = ["bash", "-c", '%s && %s' % (command1, command2)]
        output = subprocess.check_output(command)
        return output.decode('utf-8').strip()

    with environ_bindings(SNOVAULT_DB_TEST_PORT=None):
        actual_port = _check_db_port()
        default_port = str(DEFAULT_SNOVAULT_DB_TEST_PORT)
        print('actual_port=', actual_port)
        print('default_port=', default_port)
        assert actual_port == default_port

    custom_port = "314159"
    with environ_bindings(SNOVAULT_DB_TEST_PORT=custom_port):
        actual_port = _check_db_port()
        print('actual_port=', actual_port)
        print('custom_port=', custom_port)
        assert actual_port == custom_port
