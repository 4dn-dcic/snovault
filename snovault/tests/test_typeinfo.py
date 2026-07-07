"""
Unit tests for snovault.typeinfo.extract_schema_links -- a pure recursive
generator that walks a JSON schema and yields the dotted paths to linkTo fields.
It is used to build type link metadata and had no direct test.
"""
import pytest

from ..typeinfo import extract_schema_links


pytestmark = [pytest.mark.unit]


def test_falsy_schema_yields_nothing():
    assert list(extract_schema_links(None)) == []
    assert list(extract_schema_links({})) == []


def test_top_level_link():
    schema = {'properties': {'lab': {'linkTo': 'Lab'}}}
    assert list(extract_schema_links(schema)) == [('lab',)]


def test_non_link_properties_ignored():
    schema = {'properties': {'name': {'type': 'string'}, 'lab': {'linkTo': 'Lab'}}}
    assert list(extract_schema_links(schema)) == [('lab',)]


def test_array_of_links_unwraps_items():
    schema = {'properties': {'files': {'items': {'linkTo': 'File'}}}}
    assert list(extract_schema_links(schema)) == [('files',)]


def test_nested_object_prefixes_path():
    schema = {
        'properties': {
            'sub': {'properties': {'award': {'linkTo': 'Award'}}}
        }
    }
    assert list(extract_schema_links(schema)) == [('sub', 'award')]


def test_mixed_schema_collects_all_paths():
    schema = {
        'properties': {
            'lab': {'linkTo': 'Lab'},
            'plain': {'type': 'string'},
            'sub': {'properties': {'award': {'linkTo': 'Award'}}},
            'arr': {'items': {'linkTo': 'Y'}},
        }
    }
    assert sorted(extract_schema_links(schema)) == [('arr',), ('lab',), ('sub', 'award')]


def test_deeply_nested_paths():
    schema = {
        'properties': {
            'a': {'properties': {'b': {'properties': {'c': {'linkTo': 'Z'}}}}}
        }
    }
    assert list(extract_schema_links(schema)) == [('a', 'b', 'c')]


def test_truthy_schema_missing_properties_key_raises():
    # Documents that a non-empty schema without a 'properties' key is a hard error
    # (KeyError) rather than yielding nothing -- callers must pass real schemas.
    with pytest.raises(KeyError):
        list(extract_schema_links({'type': 'object'}))
