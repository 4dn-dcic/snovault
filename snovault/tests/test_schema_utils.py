import pytest
from snovault.schema_utils import (
    _update_resolved_data, _handle_list_or_string_value, resolve_merge_refs
)


pytestmark = [pytest.mark.working, pytest.mark.schema]


def test_schema_utils_update_resolved_data(mocker):
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')

    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'new value',
                'and': 'a ref',
                'inside': 'of a ref',
                '$merge': 'notxyz',
            }
        else:
            return {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists'],
                }
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    _update_resolved_data(resolved_data, 'xyz', {})
    assert resolved_data == {
        'a': 'new value',
        'and': 'a ref',
        'inside': 'of a ref',
        'the': 'notxyz values',
        'with': {
            'sub': 'dependencies',
            'and': [
                'lists'
            ]
        }
    }


def test_schema_utils_handle_list_or_string_value(mocker):
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'new value',
                'and': 'a ref',
                'inside': 'of a ref',
                '$merge': 'notxyz',
            }
        else:
            return {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists'],
                }
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    value = 'notxyz'
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'the': 'notxyz values',
        'with': {
            'sub': 'dependencies',
            'and': [
                'lists'
            ]
        }
    }
    resolved_data = {}
    value = ['notxyz', 'xyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'the': 'notxyz values',
        'with': {
            'sub': 'dependencies',
            'and': ['lists']
        },
        'a': 'new value',
        'and': 'a ref',
        'inside': 'of a ref'
    }
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'b',
            }
        else:
            return {
                'c': 'd'
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    value = ['notxyz', 'xyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'a': 'b',
        'c': 'd',
    }
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'b',
            }
        else:
            return {
                'a': 'override'
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    value = ['notxyz', 'xyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'a': 'b',
    }
    resolved_data = {}
    value = ['xyz', 'notxyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'a': 'override',
    }


def test_schema_utils_resolve_merge_refs_returns_copy_of_original_if_no_refs(mocker):
    resolver = None
    data = {'a': 'b'}
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    assert id(resolved_data) != id(data)
    data = {
        'a': 'b',
        'c': ['d', 'e', 1],
        'x': {
            'y': {
                'z': [
                    {
                        'p': 'r'
                    },
                    3.2,
                    True,
                    False,
                    None
                ]
            }
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    # Dicts are copies.
    assert id(resolved_data) != id(data)
    # Lists are copies.
    assert id(resolved_data['x']['y']['z'][0]) != id(data['x']['y']['z'][0])
    # Assignment doesn't reflect in original.
    resolved_data['x']['y']['z'][0]['p'] = 'new value'
    assert data['x']['y']['z'][0]['p'] == 'r'
    data = [
        'k',
        '5',
        '6',
        2,
        {},
    ]
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    assert id(resolved_data) != id(data)


def test_schema_utils_resolve_merge_refs_fills_in_refs(mocker):
    resolver = None
    data = {'a': 'b'}
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    resolve_merge_ref.return_value = {'a new value': 'that was resolved'}
    data = {'a': 'b', 'c': {'$merge': 'xyz'}}
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {'a': 'b', 'c': {'a new value': 'that was resolved'}}
    assert resolved_data == expected_data
    data = {
        'a': 'b',
        'c': {'$merge': 'xyz'},
        'sub': {
            'values': [
                'that',
                'were',
                'resolved',
                {
                    'if': {
                        '$merge': 'xyz',
                        'and': 'other',
                        'values': 'are',
                        'allowed': 'too',
                    }
                }
            ]
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {
        'a': 'b',
        'c': {'a new value': 'that was resolved'},
        'sub': {
            'values': [
                'that',
                'were',
                'resolved',
                {
                    'if': {
                        'a new value': 'that was resolved',
                        'and': 'other',
                        'values': 'are',
                        'allowed': 'too',
                    }
                }
            ]
        }
    }
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'new value',
                'and': 'a ref',
                'inside': 'of a ref',
                '$merge': 'notxyz',
            }
        else:
            return {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists'],
                }
            }
    resolve_merge_ref.side_effect = custom_resolver
    data = {
        'something_new': [
            {
                '$merge': 'notxyz'
            }
        ],
        'a': 'b',
        'c': {'$merge': 'xyz'},
        'sub': {
            'values': [
                'that',
                'were',
                'resolved',
                {
                    'if': {
                        '$merge': 'xyz',
                        'and': 'other',
                        'values': 'are',
                        'allowed': 'too',
                    }
                }
            ]
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {
        'something_new': [
            {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists']
                }
            }
        ],
        'a': 'b',
        'c': {
            'a': 'new value',
            'and': 'a ref',
            'inside': 'of a ref',
            'the': 'notxyz values',
            'with': {
                'sub': 'dependencies',
                'and': ['lists']
            }
        },
        'sub': {
            'values': [
                'that', 'were',
                'resolved', {
                    'if': {
                        'a': 'new value',
                        'and': 'other',
                        'inside': 'of a ref',
                        'the': 'notxyz values',
                        'with': {
                            'sub': 'dependencies',
                            'and': ['lists']
                        },
                        'values': 'are',
                        'allowed': 'too'
                    }
                }
            ]
        }
    }
    assert resolved_data == expected_data


def test_schema_utils_resolve_merge_refs_fills_allows_override_of_ref_property(mocker):
    resolver = None
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    resolve_merge_ref.return_value = {
        'a new value': 'that was resolved',
        'custom': 'original value',
        'and': 'something else',
    }
    data = {
        'a': {
            '$merge': 'xyz',
            'custom': 'override',
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {
        'a': {
            'a new value': 'that was resolved',
            'and': 'something else',
            'custom': 'override',
        }
    }
    assert resolved_data == expected_data


def test_schema_utils_resolve_merge_ref_in_real_schema():
    """ Tests that we can resolve a $merge object in our test schema """
    # TODO write me

