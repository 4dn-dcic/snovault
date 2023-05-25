# Authorization related functions which may be overriden by an implementing app,
# e.g. Foursight or CGAP portal, using the dcicutils project_utils mechanism.

from ..authorization import _create_principals

class SnovaultProjectAuthorization:

    def authorization_create_principals(self, login, user, collections):
        return _create_principals(login, user, collections)
