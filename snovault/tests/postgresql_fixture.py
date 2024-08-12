import os.path
import sys
import subprocess

import atexit
import shutil
import tempfile

from urllib.parse import urlencode


def initdb(datadir, prefix='', echo=False):
    init_args = [
        os.path.join(prefix, 'initdb'),
        '-D', datadir,
        '-U', 'postgres',
        '--auth=trust',
    ]
    output = subprocess.check_output(
        init_args,
        close_fds=True,
        stderr=subprocess.STDOUT,
    )
    if echo:
        print(output.decode('utf-8'))


DEFAULT_SNOVAULT_DB_TEST_PORT = 5440

_custom_port = os.environ.get("SNOVAULT_DB_TEST_PORT", None)
try:
    SNOVAULT_DB_TEST_PORT = int(_custom_port) if _custom_port else DEFAULT_SNOVAULT_DB_TEST_PORT
except (ValueError, TypeError):
    print("Bad SNOVAULT_DB_TEST_PORT=%r. Using %s." % (_custom_port, DEFAULT_SNOVAULT_DB_TEST_PORT))
    SNOVAULT_DB_TEST_PORT = DEFAULT_SNOVAULT_DB_TEST_PORT


# These have some wired behavior in the server_process function below. It may not work
# to override them, since the 'postgres' command doesn't take a username parameter.
# Permissions and other issues might come into play, since the db is initialized for
# a postgres/postgres username/dbname, and to use another db might require additional
# initialization. -kmp 14-Mar-2020

SNOVAULT_DB_TEST_HOSTNAME = ''
SNOVAULT_DB_TEST_USERNAME = 'postgres'
SNOVAULT_DB_TEST_DBNAME = 'postgres'
SNOVAULT_DB_TEST_DATADIR = None


def make_snovault_db_test_url(username=SNOVAULT_DB_TEST_USERNAME,
                              port=SNOVAULT_DB_TEST_PORT,
                              hostname=SNOVAULT_DB_TEST_HOSTNAME,
                              dbname=SNOVAULT_DB_TEST_DBNAME,
                              datadir=SNOVAULT_DB_TEST_DATADIR):
    query_string = "?" + urlencode({"host": datadir}) if datadir else ""
    return "postgresql://%s@%s:%s/%s%s" % (username, hostname, port, dbname, query_string)


def server_process(datadir, prefix='', echo=False, port=None):

    postgres_command = os.path.join(prefix, 'postgres')

    command = [
        postgres_command,
        '-D', datadir,
        '-F',  # no fsync
        '-h', SNOVAULT_DB_TEST_HOSTNAME,
        '-k', datadir,
        '-p', str(SNOVAULT_DB_TEST_PORT) if port is None else port,
    ]
    process = subprocess.Popen(
        command,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    success_line = b'database system is ready to accept connections\n'

    lines = []
    for line in iter(process.stdout.readline, b''):
        if echo:
            sys.stdout.write(line.decode('utf-8'))
        lines.append(line)
        if line.endswith(success_line):
            break
    else:
        code = process.wait()
        msg = ('Process return code: %d\n' % code) + b''.join(lines).decode('utf-8')
        raise Exception(msg)

    if not echo:
        process.stdout.close()

    if echo:
        print('Created: %s' % make_snovault_db_test_url(datadir=datadir))

    return process


def main():
    datadir = tempfile.mkdtemp()

    def clean_datadir():
        shutil.rmtree(datadir)
        print('Cleaned dir: %s' % datadir)

    print('Starting in dir: %s' % datadir)
    try:
        process = server_process(datadir, echo=True)
    except BaseException:
        # If there's an error setting up the server process, rush an early cleanup.
        clean_datadir()
        raise

    @atexit.register
    def cleanup_process():
        try:
            if process.poll() is None:
                process.terminate()
                for output_line in process.stdout:
                    sys.stdout.write(output_line.decode('utf-8'))
                process.wait()
        finally:
            clean_datadir()

    print('Started. ^C to exit.')

    try:
        for line in iter(process.stdout.readline, b''):
            sys.stdout.write(line.decode('utf-8'))
    except KeyboardInterrupt:
        raise SystemExit(0)


if __name__ == '__main__':
    main()
