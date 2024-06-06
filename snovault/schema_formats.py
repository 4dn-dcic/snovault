import re

from .schema_utils import format_checker
from .server_defaults import (
    ACCESSION_TEST_PREFIX,
    GET_ACCESSION_PREFIX
)


# Codes we allow for testing go here.
ACCESSION_TEST_CODES = "BS|ES|EX|FI|FS|IN|SR|WF"

accession_re = re.compile(r'^%s[1-9A-Z]{9}$' % GET_ACCESSION_PREFIX())

test_accession_re = re.compile(r'^%s(%s)[0-9]{4}([0-9][0-9][0-9]|[A-Z][A-Z][A-Z])$' % (
    ACCESSION_TEST_PREFIX, ACCESSION_TEST_CODES))

uuid_re = re.compile(r'(?i)[{]?(?:[0-9a-f]{4}-?){8}[}]?')


@format_checker.checks("uuid")
def is_uuid(instance):
    # Python's UUID ignores all dashes, whereas Postgres is more strict
    # http://www.postgresql.org/docs/9.2/static/datatype-uuid.html
    return bool(uuid_re.match(instance))


def is_accession(instance):
    """Just a pattern checker."""
    # Unfortunately we cannot access the accessionType here
    return (
        accession_re.match(instance) is not None or
        test_accession_re.match(instance) is not None
    )
