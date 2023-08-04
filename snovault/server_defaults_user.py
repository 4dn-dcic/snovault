# Factored out of server_defaults to avoid circular dependencies now that get_userid is
# used in resources.Item.is_update_by_admin_user (commonized from fourfront/cgap-portal),
# since server_defaults imports schema_utils which imports resources which wants get_userid.

from snovault.schema_validation import NO_DEFAULT
from pyramid.threadlocal import get_current_request
from .interfaces import COLLECTIONS


def _userid():
    request = get_current_request()
    for principal in request.effective_principals:
        if principal.startswith('userid.'):
            return principal[7:]
    return NO_DEFAULT


def get_userid():
    """ Wrapper for the server_default 'userid' above so it is not called through SERVER_DEFAULTS in our code """
    return _userid()


def get_user_resource():
    request = get_current_request()
    userid_found = _userid()
    if userid_found == NO_DEFAULT:
        return NO_DEFAULT
    return request.registry[COLLECTIONS]['user'][userid_found]
