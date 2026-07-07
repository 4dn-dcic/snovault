"""
Unit tests for snovault.local_roles -- the pyramid_localroles-style authorization
layer that expands principals via ``__ac_local_roles__`` mappings found on the
context lineage. Security-sensitive (it feeds the principal set every ACL check
sees) and previously had no direct test. Lightweight lineage nodes stand in for
resources; no services required.
"""
import pytest

from pyramid.security import Allow

from ..local_roles import (
    local_principals,
    merged_local_principals,
    LocalRolesAuthorizationPolicy,
)


pytestmark = [pytest.mark.unit]


class Node:
    """ Minimal resource with a __parent__ lineage, optional local roles/ACL. """

    def __init__(self, parent=None, roles=None, block=False, acl=None):
        self.__parent__ = parent
        if roles is not None:
            self.__ac_local_roles__ = roles
        if block:
            self.__ac_local_roles_block__ = True
        if acl is not None:
            self.__acl__ = acl


class TestLocalPrincipals:

    def test_no_local_roles_returns_principals_unchanged(self):
        principals = ['userid.bob']
        result = local_principals(Node(), principals)
        assert result is principals

    def test_no_matching_principal_returns_principals_unchanged(self):
        principals = ['userid.bob']
        node = Node(roles={'userid.alice': ['role.owner']})
        result = local_principals(node, principals)
        assert result is principals

    def test_matching_principal_gains_local_roles(self):
        node = Node(roles={'userid.alice': ['role.owner', 'role.viewer']})
        result = local_principals(node, ['userid.alice', 'system.Everyone'])
        assert result == {'userid.alice', 'system.Everyone', 'role.owner', 'role.viewer'}

    def test_single_string_role_is_treated_as_singleton(self):
        node = Node(roles={'userid.alice': 'role.owner'})
        result = local_principals(node, ['userid.alice'])
        assert result == {'userid.alice', 'role.owner'}

    def test_callable_local_roles_is_invoked(self):
        node = Node(roles=lambda: {'userid.carol': ['role.admin']})
        result = local_principals(node, ['userid.carol'])
        assert result == {'userid.carol', 'role.admin'}

    def test_roles_accumulate_up_the_lineage(self):
        root = Node(roles={'userid.alice': ['role.lab_member']})
        child = Node(parent=root, roles={'userid.alice': 'role.owner'})
        result = local_principals(child, ['userid.alice'])
        assert result == {'userid.alice', 'role.owner', 'role.lab_member'}

    def test_block_stops_parent_roles_but_keeps_own(self):
        # __ac_local_roles_block__ on a node stops the walk *above* that node:
        # the blocking node's own roles still apply, its ancestors' do not.
        root = Node(roles={'userid.alice': ['role.lab_member']})
        child = Node(parent=root, roles={'userid.alice': 'role.owner'}, block=True)
        result = local_principals(child, ['userid.alice'])
        assert result == {'userid.alice', 'role.owner'}


class TestMergedLocalPrincipals:

    def test_no_local_roles_returns_principals_unchanged(self):
        principals = ['role.nonexistent']
        result = merged_local_principals(Node(), principals)
        assert result is principals

    def test_no_matching_role_returns_principals_unchanged(self):
        principals = ['role.nonexistent']
        node = Node(roles={'userid.alice': ['role.owner']})
        result = merged_local_principals(node, principals)
        assert result is principals

    def test_maps_roles_back_to_granting_principals(self):
        # merged_local_principals is the reverse mapping: given allowed roles,
        # add the principals whose local roles intersect them.
        root = Node(roles={'userid.alice': ['role.lab_member', 'role.viewer']})
        child = Node(parent=root, roles={'userid.alice': 'role.owner'})
        result = merged_local_principals(child, ['role.owner', 'role.viewer'])
        assert sorted(result) == ['role.owner', 'role.viewer', 'userid.alice']

    def test_returns_a_list(self):
        node = Node(roles={'userid.alice': ['role.owner']})
        result = merged_local_principals(node, ['role.owner'])
        assert isinstance(result, list)


class TestLocalRolesAuthorizationPolicy:

    def test_permits_via_local_role(self):
        ctx = Node(roles={'userid.alice': ['role.owner']},
                   acl=[(Allow, 'role.owner', 'edit')])
        policy = LocalRolesAuthorizationPolicy()
        assert bool(policy.permits(ctx, ['userid.alice'], 'edit')) is True

    def test_denies_principal_without_local_role(self):
        ctx = Node(roles={'userid.alice': ['role.owner']},
                   acl=[(Allow, 'role.owner', 'edit')])
        policy = LocalRolesAuthorizationPolicy()
        assert bool(policy.permits(ctx, ['userid.bob'], 'edit')) is False

    def test_principals_allowed_by_permission_includes_local_principals(self):
        ctx = Node(roles={'userid.alice': ['role.owner']},
                   acl=[(Allow, 'role.owner', 'edit')])
        policy = LocalRolesAuthorizationPolicy()
        allowed = policy.principals_allowed_by_permission(ctx, 'edit')
        assert sorted(allowed) == ['role.owner', 'userid.alice']
