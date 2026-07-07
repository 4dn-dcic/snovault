"""
Unit tests for snovault.server_defaults_misc.add_last_modified -- the helper that
stamps system-managed last_modified metadata onto item properties. It has five
distinct branches (real userid, NO_DEFAULT, no-request-with-userid-override,
no-request-without-userid, custom field name) none of which were covered.

get_userid / get_now are patched at the module under test so no request or clock
is required.
"""
from unittest import mock

import pytest

from .. import server_defaults_misc as sdm
from ..schema_validation import NO_DEFAULT


pytestmark = [pytest.mark.unit]


FIXED_NOW = '2026-01-01T00:00:00.000000+00:00'


def test_add_last_modified_with_real_userid():
    with mock.patch.object(sdm, 'get_userid', return_value='uuid-123'), \
            mock.patch.object(sdm, 'get_now', return_value=FIXED_NOW):
        props = {}
        sdm.add_last_modified(props)
    assert props == {
        'last_modified': {'modified_by': 'uuid-123', 'date_modified': FIXED_NOW}
    }


def test_add_last_modified_no_default_userid_is_noop():
    # When get_userid resolves to NO_DEFAULT (no logged-in user) nothing is written.
    with mock.patch.object(sdm, 'get_userid', return_value=NO_DEFAULT), \
            mock.patch.object(sdm, 'get_now', return_value=FIXED_NOW):
        props = {}
        sdm.add_last_modified(props)
    assert props == {}


def test_add_last_modified_outside_request_with_userid_override():
    # get_userid raises AttributeError when there is no request in scope; an
    # explicit userid kwarg is then used instead.
    with mock.patch.object(sdm, 'get_userid', side_effect=AttributeError), \
            mock.patch.object(sdm, 'get_now', return_value=FIXED_NOW):
        props = {}
        sdm.add_last_modified(props, userid='override-user')
    assert props == {
        'last_modified': {'modified_by': 'override-user', 'date_modified': FIXED_NOW}
    }


def test_add_last_modified_outside_request_without_userid_is_noop():
    with mock.patch.object(sdm, 'get_userid', side_effect=AttributeError), \
            mock.patch.object(sdm, 'get_now', return_value=FIXED_NOW):
        props = {}
        sdm.add_last_modified(props)
    assert props == {}


def test_add_last_modified_custom_field_name_portion():
    # field_name_portion drives all three derived field names.
    with mock.patch.object(sdm, 'get_userid', return_value='u1'), \
            mock.patch.object(sdm, 'get_now', return_value=FIXED_NOW):
        props = {}
        sdm.add_last_modified(props, field_name_portion='text_edited')
    assert props == {
        'last_text_edited': {'text_edited_by': 'u1', 'date_text_edited': FIXED_NOW}
    }


def test_add_last_modified_preserves_existing_properties():
    with mock.patch.object(sdm, 'get_userid', return_value='u1'), \
            mock.patch.object(sdm, 'get_now', return_value=FIXED_NOW):
        props = {'existing': 'value'}
        sdm.add_last_modified(props)
    assert props['existing'] == 'value'
    assert 'last_modified' in props
