import pytest
from snovault.schema_utils import (
    _update_resolved_data, _handle_list_or_string_value,  # noQA - testing protected members
    resolve_merge_refs, validate, match_merge_syntax
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


def test_schema_utils_resolve_merge_ref_in_real_schema(testapp):
    """ Tests that we can resolve a $merge object in our test schema """
    collection_url = '/testing-linked-schema-fields'
    # test posting $merge quality field
    testapp.post_json(collection_url, {'quality': 5}, status=201)
    testapp.post_json(collection_url, {'quality': 'no'}, status=422)
    # test posting $merge linkTo
    atid = testapp.post_json('/testing-link-targets-sno', {'name': 'target'}, status=201).json['@graph'][0]['@id']
    testapp.post_json(collection_url, {'quality': 5,
                                       'linked_targets': [
                                           {'target': atid, 'test_description': 'test'}
                                       ]}, status=201)

def test_schema_utils_resolve_merge_ref_in_embedded_schema(testapp):
    """ Tests that we can resolve a $merge object in our test schema embedded in another schema """
    collection_url = '/testing-linked-schema-fields'
    atid = testapp.post_json('/testing-link-targets-sno', {'name': 'target'}, status=201).json['@graph'][0]['@id']
    atid = testapp.post_json(collection_url, {'quality': 5,
                                              'linked_targets': [
                                                  {'target': atid, 'test_description': 'test'}
                                              ]}, status=201).json['@graph'][0]['@id']
    atid = testapp.post_json('/testing-embedded-linked-schema-fields', {
        'link': atid
    }, status=201).json['@graph'][0]['@id']
    embedded = testapp.get(f'/{atid}?frame=embedded', status=200).json
    assert embedded['link']['quality'] == 5
    assert embedded['link']['linked_targets'][0]['test_description'] == 'test'


@pytest.mark.parametrize('invalid_date', [
    'not a date',
    'also-not-a-date',
    '10-5-2022',
    '10-05-almost',
    '10-05-2023f'
])
def test_schema_utils_validates_dates(testapp, invalid_date):
    """ Tests that our validator will validate dates """
    schema = {
        "type": "object",
        "properties": {
            "date_property": {
                "type": "string",
                "format": "date"
            }
        }
    }
    _, errors = validate(schema, {
        'date_property': invalid_date
    })
    date_error = str(errors[0])
    assert f"{invalid_date!r} is not a 'date'" in date_error


@pytest.mark.parametrize('invalid_date_time', [
    'not a date',
    'also-not-a-date',
    '10-5-2022',
    '10-05-almost',
    '10-05-2023f',
    '1424-45-93T15:32:12.9023368Z',
    '20015-10-23T15:32:12.9023368Z',
    '2001-130-23T15:32:12.9023368Z',
    '2001-10-233T15:32:12.9023368Z',
    '2001-10-23T153:32:12.9023368Z'
])
def test_schema_utils_validates_date_times(testapp, invalid_date_time):
    """ Tests that our validator will validate date-time """
    schema = {
        "type": "object",
        "properties": {
            "date_time_property": {
                "type": "string",
                "format": "date-time"
            }
        }
    }
    _, errors = validate(schema, {
        'date_time_property': invalid_date_time
    })
    date_error = str(errors[0])
    assert f"{invalid_date_time!r} is not a 'date-time'" in date_error


@pytest.mark.parametrize('ref', [
    'snovault:access_key.json#/properties/access_key_id',
    'snovault:schemas/access_key.json#/properties/access_key_id',
    'snovault:schemas/access_key2.json#/properties/access_key_id',
    'snovault:schemas/access_key.json#/properties/access_key_id2',
    'snovault:schemas/schema_one/access_key.json#/properties/object/access_key_id',
    'snovault:schemas/mixins.json#/access_key_id',
    ]
)
def test_schema_utils_merge_regex_matches(ref):
    """ Positive test for testing the merge regex match """
    assert match_merge_syntax(ref)


@pytest.mark.parametrize('ref', [
    'not a schema',
    'close:to/aschema',
    'closer:to.json#aschema',
    'snovault:schemas/schema_one',
    'missing-package.json#/properties/schema',
    'snovault:schemas/access_key.jsn#/properties/access_key_id',
    ]
)
def test_schema_utils_merge_regex_no_match(ref):
    """ Positive test for testing the merge regex match """
    assert not match_merge_syntax(ref)


def test_get_identifying_and_required_properties():

    from snovault.schema_utils import get_identifying_and_required_properties, load_schema

    schema = load_schema("snovault:schemas/access_key.json")
    identifying_properties, required_properties = get_identifying_and_required_properties(schema)
    assert identifying_properties == ["uuid"]
    assert required_properties == []

    schema = {
        "identifyingProperties": ["uuid", "another_id"],
        "required": ["some_required_property_a", "some_required_property_b"],
        "anyOf": [
            {"required": ["either_require_this_property_a", "or_this_one"]},
            {"required": ["or_require_this_property_a"]}
         ]
    }
    identifying_properties, required_properties = get_identifying_and_required_properties(schema)
    assert set(identifying_properties) == {"another_id", "uuid"}
    assert set(required_properties) == {"some_required_property_a", "some_required_property_b",
                                        "either_require_this_property_a", "or_require_this_property_a", "or_this_one"}

    schema = {
        "identifyingProperties": ["uuid", "another_id"],
        "anyOf": [
            {"required": "either_require_this_property_a", "junk_to_ignore": "abc"},
            {"required": "or_require_this_property_a"},
            {"junk_to_ignore": 123}
         ]
    }
    identifying_properties, required_properties = get_identifying_and_required_properties(schema)
    assert set(identifying_properties) == {"another_id", "uuid"}
    assert set(required_properties) == {"either_require_this_property_a", "or_require_this_property_a"}

    with pytest.raises(Exception):
        schema = {
            "required": ["some_required_property_a", "some_required_property_b"],
            "anyOf": [
                {"unexpected": "dummy"},
                {"required": "either_require_this_property_a"},
                {"required": "or_require_this_property_a"}
             ]
        }
        identifying_properties, required_properties = get_identifying_and_required_properties(schema)
        assert set(identifying_properties) == {}
        assert set(required_properties) == {"some_required_property_a", "some_required_property_b", "either_require_this_property_a", "or_require_this_property_a"}
