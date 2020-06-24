import os
import pytest
import subprocess
from .postgresql_fixture import SNOVAULT_DB_TEST_PORT, DEFAULT_SNOVAULT_DB_TEST_PORT
from dcicutils.qa_utils import override_environ


def test_snovault_db_test_port():

    # Will made this point during code review (18-Jun-2020):
    #  This makes virtualenv a requirement for successfully running the tests.
    #  Maybe that's something we want to do, but wanted to point it out
    #  just in case.
    command1 = "source %s/bin/activate" % os.environ['VIRTUAL_ENV']
    command2 = "python -c 'from snovault.tests.postgresql_fixture import SNOVAULT_DB_TEST_PORT; print(SNOVAULT_DB_TEST_PORT)'"

    print("os.environ.get('SNOVAULT_DB_TEST_PORT') = %r" % os.environ.get('SNOVAULT_DB_TEST_PORT'))

    def _check_db_port():
        # We need to load the module freshly to find out if this setting is working, so we ask a subprocess.
        command = ["bash", "-c", '%s && %s' % (command1, command2)]
        output = subprocess.check_output(command)
        return output.decode('utf-8').strip()

    with override_environ(SNOVAULT_DB_TEST_PORT=None):
        actual_port = _check_db_port()
        default_port = str(DEFAULT_SNOVAULT_DB_TEST_PORT)
        print('actual_port=', actual_port)
        print('default_port=', default_port)
        assert actual_port == default_port

    custom_port = "314159"
    with override_environ(SNOVAULT_DB_TEST_PORT=custom_port):
        actual_port = _check_db_port()
        print('actual_port=', actual_port)
        print('custom_port=', custom_port)
        assert actual_port == custom_port
