"""
Unit tests for snovault.authorization -- the groupfinder permission resolution
used on every authenticated request. This is security-critical code that only had
indirect coverage. Lightweight fakes stand in for the request/collections, and
app_project() is patched for the full-user path.
"""
from types import SimpleNamespace
from unittest import mock

import pytest
from pyramid.security import Authenticated

from .. import authorization as authz
from ..authorization import groupfinder, _create_principals, is_admin_request
from snovault import COLLECTIONS


pytestmark = [pytest.mark.unit]


class FakeUser:
    def __init__(self, uuid, properties):
        self.uuid = uuid
        self.properties = properties


class FakeItemTypeMap:
    """ Mimics collections.by_item_type[item_type] dict-of-items access. """
    def __init__(self, mapping):
        self._mapping = mapping

    def __getitem__(self, key):
        return self._mapping[key]


class FakeCollections:
    def __init__(self, by_item_type):
        self.by_item_type = by_item_type


def make_request(collections=None, effective_principals=None):
    registry = {COLLECTIONS: collections}
    return SimpleNamespace(
        registry=registry,
        effective_principals=effective_principals or [],
    )


class TestGroupfinderSyntheticResults:

    def test_login_without_dot_returns_none(self):
        # Short circuits before the registry is even consulted.
        assert groupfinder('nodothere', make_request()) is None

    @pytest.mark.parametrize('localname', ['EMBED', 'INDEXER'])
    def test_embed_indexer_get_empty_principals(self, localname):
        request = make_request(collections=FakeCollections({}))
        assert groupfinder('remoteuser.%s' % localname, request) == []

    @pytest.mark.parametrize('localname', ['TEST', 'IMPORT', 'UPGRADE', 'INGESTION'])
    def test_admin_synthetic_results(self, localname):
        request = make_request(collections=FakeCollections({}))
        assert groupfinder('remoteuser.%s' % localname, request) == ['group.admin']

    def test_submitter_synthetic_result(self):
        request = make_request(collections=FakeCollections({}))
        assert groupfinder('remoteuser.TEST_SUBMITTER', request) == ['group.submitter']

    def test_authenticated_synthetic_result(self):
        request = make_request(collections=FakeCollections({}))
        assert groupfinder('remoteuser.TEST_AUTHENTICATED', request) == [Authenticated]


class TestGroupfinderUserLookup:

    def test_missing_user_returns_none(self):
        collections = FakeCollections({'user': FakeItemTypeMap({})})
        request = make_request(collections=collections)
        assert groupfinder('mailto.unknown@example.com', request) is None

    def test_deleted_user_returns_none(self):
        user = FakeUser('uuid-1', {'status': 'deleted', 'groups': ['admin']})
        collections = FakeCollections({'user': FakeItemTypeMap({'uuid-1': user})})
        request = make_request(collections=collections)
        assert groupfinder('remoteuser.uuid-1', request) is None

    def test_active_user_delegates_to_project_create_principals(self):
        user = FakeUser('uuid-1', {'status': 'current', 'groups': ['admin']})
        collections = FakeCollections({'user': FakeItemTypeMap({'uuid-1': user})})
        request = make_request(collections=collections)
        fake_project = SimpleNamespace(
            authorization_create_principals=lambda login, u, c: ['userid.uuid-1', 'group.admin'])
        with mock.patch.object(authz, 'app_project', return_value=fake_project):
            result = groupfinder('remoteuser.uuid-1', request)
        assert result == ['userid.uuid-1', 'group.admin']


class TestGroupfinderAccessKey:

    def _collections_with(self, access_key_props, user=None):
        access_key = SimpleNamespace(properties=access_key_props)
        user_map = {}
        if user is not None:
            user_map[user.uuid] = user
        return FakeCollections({
            'access_key': FakeItemTypeMap({'key-1': access_key}),
            'user': FakeItemTypeMap(user_map),
        })

    def test_missing_access_key_returns_none(self):
        collections = FakeCollections({'access_key': FakeItemTypeMap({})})
        request = make_request(collections=collections)
        assert groupfinder('accesskey.missing', request) is None

    @pytest.mark.parametrize('status', ['deleted', 'revoked'])
    def test_deleted_or_revoked_access_key_returns_none(self, status):
        collections = self._collections_with({'status': status, 'user': 'uuid-1'})
        request = make_request(collections=collections)
        assert groupfinder('accesskey.key-1', request) is None

    def test_valid_access_key_resolves_user(self):
        user = FakeUser('uuid-1', {'status': 'current', 'groups': []})
        collections = self._collections_with({'status': 'current', 'user': 'uuid-1'}, user=user)
        request = make_request(collections=collections)
        fake_project = SimpleNamespace(
            authorization_create_principals=lambda login, u, c: ['userid.uuid-1'])
        with mock.patch.object(authz, 'app_project', return_value=fake_project):
            result = groupfinder('accesskey.key-1', request)
        assert result == ['userid.uuid-1']


class TestCreatePrincipals:

    def test_userid_and_groups(self):
        user = FakeUser('uuid-abc', {'groups': ['admin', 'submitter']})
        assert _create_principals('login', user, collections=None) == [
            'userid.uuid-abc', 'group.admin', 'group.submitter'
        ]

    def test_no_groups(self):
        user = FakeUser('uuid-abc', {})
        assert _create_principals('login', user, collections=None) == ['userid.uuid-abc']


class TestIsAdminRequest:

    def test_admin_present(self):
        request = make_request(effective_principals=['group.admin', 'userid.x'])
        assert is_admin_request(request) is True

    def test_admin_absent(self):
        request = make_request(effective_principals=['group.submitter', 'userid.x'])
        assert is_admin_request(request) is False
