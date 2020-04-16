import os
import pytest
import mock

from .. import TYPES
from ..elasticsearch.create_mapping import merge_schemas, type_mapping, update_mapping_by_embed
from ..elasticsearch.interfaces import ELASTIC_SEARCH
from .pyramidfixtures import dummy_request
from .test_views import PARAMETERIZED_NAMES
from .toolfixtures import registry
from ..settings import Settings
from contextlib import contextmanager


unit_test_type = 'EmbeddingTest'


@contextmanager
def mappings_use_nested(value=True):
    """ Context manager that sets the MAPPINGS_USE_NESTED setting with the given value, default True """
    old_setting = Settings.MAPPINGS_USE_NESTED
    try:
        Settings.MAPPINGS_USE_NESTED = value
        yield
    finally:
        Settings.MAPPINGS_USE_NESTED = old_setting


@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_type_mapping(registry, item_type):
    """
    Test basic mapping properties for each item type
    """
    with mappings_use_nested(False):
        mapping = type_mapping(registry[TYPES], item_type)
        assert mapping
        assert 'properties' in mapping
        assert 'include_in_all' in mapping
        if item_type == 'TestingLinkTargetElasticSearch':
            assert mapping['properties']['reverse_es'].get('type', 'object') != 'nested'  # should not occur here


def test_type_mapping_nested(registry):
    """
    Tests that mapping a field with a list of dicts in it maps with type=nested if told to do so
    """
    with mappings_use_nested(True):
        mapping = type_mapping(registry[TYPES], 'TestingLinkTargetElasticSearch')
        assert mapping
        assert 'properties' in mapping
        assert 'include_in_all' in mapping
        assert mapping['properties']['reverse_es']['type'] == 'nested'  # should occur here


def test_merge_schemas(registry):
    """ Tests merging schemas with EmbeddingTest """
    test_schema = registry[TYPES][unit_test_type].schema
    test_subschema = test_schema['properties']['attachment']
    res = merge_schemas(test_subschema, registry[TYPES])
    assert res
    assert res != test_subschema
    assert res['properties']['attachment']['attachment'] is True


def test_update_mapping_by_embed(registry):
    # first, test with dummy data
    curr_s = {'title': 'Test', 'type': 'string'}
    curr_e = 'test'
    curr_m = {'properties': {}}
    new_m = update_mapping_by_embed(curr_m, curr_e, curr_s)
    assert 'test' in curr_m['properties']
    assert new_m['type'] == 'text'
    assert 'raw' in new_m['fields']
    assert 'lower_case_sort' in new_m['fields']

    # then test with real data and wildcard (*)
    test_schema = registry[TYPES][unit_test_type].schema
    test_subschema = test_schema['properties']['attachment']
    curr_s = merge_schemas(test_subschema, registry[TYPES])
    # * means embed all top level fields
    curr_e = '*'
    curr_m = {'properties': {}}
    new_m = update_mapping_by_embed(curr_m, curr_e, curr_s)
    for s_key in curr_s['properties']:
        check = curr_s['properties'][s_key]
        # this syntax needed for linkTos which could be arrays
        if 'linkTo' not in check.get('items', check):
            assert s_key in new_m['properties']
