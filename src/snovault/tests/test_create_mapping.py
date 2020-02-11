import pytest

from .. import TYPES
from ..elasticsearch.create_mapping import merge_schemas, type_mapping, update_mapping_by_embed
from ..elasticsearch.interfaces import ELASTIC_SEARCH
from .pyramidfixtures import dummy_request
from .test_views import PARAMETERIZED_NAMES
from .toolfixtures import registry


unit_test_type = 'EmbeddingTest'


@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_type_mapping(registry, item_type):
    """
    Test basic mapping properties for each item type
    """
    mapping = type_mapping(registry[TYPES], item_type)
    assert mapping
    assert 'properties' in mapping
    assert 'include_in_all' in mapping


def test_merge_schemas(registry):
    """ Tests merging schemas with EmbeddingTest """
    test_schema = registry[TYPES][unit_test_type].schema
    test_subschema = test_schema['properties']['attachment']
    res = merge_schemas(test_subschema, registry[TYPES])
    assert res
    assert res != test_subschema
    assert res['properties']['attachment']['attachment'] == True


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
