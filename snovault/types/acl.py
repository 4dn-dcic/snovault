# Considation of ACL related definitions.

from pyramid.security import Allow, Deny, Everyone
from typing import Any, List, Tuple, Union

Acl = List[Tuple[Any, Any, Union[str, List[str]]]]

ONLY_ADMIN_VIEW_ACL: Acl = [
    (Allow, 'group.admin', ['view', 'edit']),
    (Allow, 'group.read-only-admin', ['view']),
    (Allow, 'remoteuser.INDEXER', ['view']),
    (Allow, 'remoteuser.EMBED', ['view']),
    (Deny, Everyone, ['view', 'edit'])
]

PUBLIC_ACL: Acl = [
    (Allow, Everyone, ['view']),
] + ONLY_ADMIN_VIEW_ACL

DELETED_ACL: Acl = [
    (Deny, Everyone, 'visible_for_edit')
] + ONLY_ADMIN_VIEW_ACL

# Originally from user.py:

ONLY_ADMIN_VIEW_USER_DETAILS_ACL = [
    (Allow, 'group.admin', ['view', 'view_details', 'edit']),
    (Allow, 'remoteuser.INDEXER', ['view']),
    (Allow, 'remoteuser.EMBED', ['view']),
    (Deny, Everyone, ['view', 'view_details', 'edit']),
]

ONLY_OWNER_VIEW_PROFILE_ACL = [
    (Allow, 'role.owner', 'view'),
    # (Allow, 'role.owner', 'edit'),
    # (Allow, 'role.owner', 'view_details'),
] + ONLY_ADMIN_VIEW_USER_DETAILS_ACL

DELETED_USER_ACL = [
    (Deny, Everyone, 'visible_for_edit')
] + ONLY_ADMIN_VIEW_USER_DETAILS_ACL

