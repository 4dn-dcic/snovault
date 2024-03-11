import random
import uuid

from dcicutils.misc_utils import exported, utc_now_str
from snovault.schema_validation import NO_DEFAULT
from pyramid.path import DottedNameResolver
from pyramid.threadlocal import get_current_request
from snovault.schema_utils import server_default
from .interfaces import COLLECTIONS  # , ROOT
from string import digits  # , ascii_uppercase
from .project_app import app_project
from .server_defaults_misc import add_last_modified, get_now
from .server_defaults_user import _userid, get_userid, get_user_resource

exported(
    COLLECTIONS,
    add_last_modified,
    get_now,
    get_userid,
    get_user_resource
)


ACCESSION_FACTORY = __name__ + ':accession_factory'
ACCESSION_TEST_PREFIX = 'TST'

# Only within snovault (only called from schema_formats.py) to get around
# app_project call at file scope (came up as circular import in smaht ingester).
# ACCESSION_PREFIX = app_project().ACCESSION_PREFIX
def GET_ACCESSION_PREFIX():
    return app_project().ACCESSION_PREFIX


def includeme(config):
    accession_factory = config.registry.settings.get('accession_factory')
    if accession_factory:
        factory = DottedNameResolver().resolve(accession_factory)
    else:
        factory = enc_accession
    config.registry[ACCESSION_FACTORY] = factory


# XXX: This stuff is all added based on the serverDefault identifier in the schemas
# removing it altogether will totally break our code


@server_default
def userid(instance, subschema):  # args required by jsonschema-serialize-fork
    return _userid()


@server_default
def now(instance, subschema):  # args required by jsonschema-serialize-fork
    return utc_now_str()


@server_default
def uuid4(instance, subschema):
    return str(uuid.uuid4())


@server_default
def accession(instance, subschema):
    if 'external_accession' in instance:
        return NO_DEFAULT
    request = get_current_request()
    factory = request.registry[ACCESSION_FACTORY]
    # With 17 576 000 options
    ATTEMPTS = 10
    for attempt in range(ATTEMPTS):
        new_accession = factory(subschema['accessionType'])
        if new_accession in request.root:
            continue
        return new_accession
    raise AssertionError("Free accession not found in %d attempts" % ATTEMPTS)


#FDN_ACCESSION_FORMAT = (digits, digits, digits, ascii_uppercase, ascii_uppercase, ascii_uppercase)
FDN_ACCESSION_FORMAT = ['ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789']*7

def enc_accession(accession_type):
    random_part = ''.join(random.choice(s) for s in FDN_ACCESSION_FORMAT)
    return GET_ACCESSION_PREFIX() + accession_type + random_part


TEST_ACCESSION_FORMAT = (digits, ) * 7


def test_accession(accession_type):
    """ Test accessions are generated on test.encodedcc.org
    """
    random_part = ''.join(random.choice(s) for s in TEST_ACCESSION_FORMAT)
    return 'TST' + accession_type + random_part
