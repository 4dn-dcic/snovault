import pytest
from unittest import mock

from contextlib import contextmanager
from dcicutils.misc_utils import ignored

from ..elasticsearch.create_mapping import merge_schemas, type_mapping, update_mapping_by_embed, get_items_to_upgrade
from ..interfaces import TYPES
from ..settings import Settings
from ..util import add_default_embeds

from .test_views import PARAMETERIZED_NAMES


unit_test_type = 'EmbeddingTest'


@contextmanager
def mappings_use_nested(value=True):
    """
    Context manager that sets the MAPPINGS_USE_NESTED setting with the given value, default True
    """
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
        if item_type == 'TestingLinkTargetElasticSearch':
            assert mapping['properties']['reverse_es'].get('type', 'object') != 'nested'  # should not occur here

        # check calculated properties on objects/arrays of objects are mapped correctly
        if item_type == 'TestingCalculatedProperties':
            assert mapping['properties']['nested']['properties']['key']['type'] == 'text'
            assert mapping['properties']['nested']['properties']['value']['type'] == 'text'
            assert mapping['properties']['nested']['properties']['keyvalue']['type'] == 'text'
            assert mapping['properties']['nested2']['properties']['key']['type'] == 'text'
            assert mapping['properties']['nested2']['properties']['value']['type'] == 'text'
            assert mapping['properties']['nested2']['properties']['keyvalue']['type'] == 'text'


def test_type_mapping_nested(registry):
    """
    Tests that mapping a field with a list of dicts in it maps with type=nested only if told to do so on
    the schema. For this case it is not specified, so if object is expected.
    """
    with mappings_use_nested(True):
        mapping = type_mapping(registry[TYPES], 'TestingLinkTargetElasticSearch')
        assert mapping
        assert 'properties' in mapping
        # if type is defined on this field, it should beg object, NOT nested since it is not enabled on this field
        assert mapping['properties']['reverse_es'].get('type', 'object') == 'object'


def test_type_mapping_nested_with_disabled_parameter(registry):
    """
    Tests that mapping a type with an object field with nested enabled correctly maps with nested.
    """
    with mappings_use_nested(True):
        mapping = type_mapping(registry[TYPES], 'TestingNestedEnabled')
        assert mapping
        assert 'properties' in mapping
        assert mapping['properties']['object_options'].get('type', 'object') != 'nested'  # neither enabled
        assert mapping['properties']['disabled_array_of_objects_in_calc_prop'].get('type', 'object') != 'nested'
        assert mapping['properties']['enabled_array_of_objects_in_calc_prop']['type'] == 'nested'  # enabled
        assert mapping['properties']['nested_options']['type'] == 'nested'  # enabled


def test_merge_schemas(registry):
    """
    Tests merging schemas with EmbeddingTest
    """
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


# types to test
TEST_TYPES = ['testing_mixins', 'embedding_test', 'nested_embedding_container', 'nested_object_link_target',
              'testing_download', 'testing_link_source_sno', 'testing_link_aggregate_sno', 'testing_link_target_sno',
              'testing_post_put_patch_sno', 'testing_dependencies', 'testing_link_target_elastic_search']


@pytest.mark.parametrize('item_type', TEST_TYPES)
def test_create_mapping_correctly_maps_embeds(registry, item_type):
    """
    This test does not actually use elasticsearch
    Only tests the mappings generated from schemas
    This test existed in FF/CGAP and has been ported here so we can detect issues earlier
    """
    mapping = type_mapping(registry[TYPES], item_type)
    assert mapping
    type_info = registry[TYPES].by_item_type[item_type]
    schema = type_info.schema
    embeds = add_default_embeds(item_type, registry[TYPES], type_info.embedded_list, schema)
    # assert that all embeds exist in mapping for the given type
    for embed in embeds:
        mapping_pointer = mapping
        split_embed = embed.split('.')
        for idx, split_ in enumerate(split_embed):
            # see if this is last level of embedding- may be a field or object
            if idx == len(split_embed) - 1:
                if 'properties' in mapping_pointer and split_ in mapping_pointer['properties']:
                    final_mapping = mapping_pointer['properties']
                else:
                    final_mapping = mapping_pointer
                if split_ != '*':
                    assert split_ in final_mapping
                else:
                    assert 'properties' in final_mapping or final_mapping.get('type') == 'object'
            else:
                assert split_ in mapping_pointer['properties']
                mapping_pointer = mapping_pointer['properties'][split_]


def test_create_mapping_drops_unmappable_properties(registry):
    """
    Use EmbeddingTest schema to ensure unmappable properties under
    patternProperties and additionalProperties are not mapped.
    """
    test_item_type = "embedding_test"
    expected_embeds = ["pattern_property_embed", "additional_property_embed"]
    expected_mapped_property = "should_be_mapped"
    mapping = type_mapping(registry[TYPES], test_item_type)
    mapped_properties = mapping.get("properties")
    assert mapped_properties
    for expected_embed in expected_embeds:
        mapped_embed = mapped_properties.get(expected_embed)
        assert mapped_embed
        mapped_embed_properties = mapped_embed.get("properties", {})
        assert len(mapped_embed_properties.keys()) == 1
        assert mapped_embed_properties.get(expected_mapped_property)


@pytest.fixture
def biosample(testapp):
    url = "/testing-biosample-sno"
    item = {
        "identifier": "test_biosample",
        "ranking": 5,
    }
    return testapp.post_json(url, item, status=201).json["@graph"][0]


def mock_scan(uuids=None):
    """
    Mock for ES scan which returns list of dicts.
    """
    embedded_item = []
    if uuids:
        for uuid in uuids:
            embedded_item.append({"_id": uuid})
    mocked_scan = mock.Mock()
    mocked_scan.return_value = embedded_item
    return mocked_scan


@mock.patch("snovault.elasticsearch.create_mapping.check_if_index_exists", return_value=True)
def test_get_items_to_upgrade(mock_check_index, testapp, biosample):
    """
    Test Elasticsearch items requiring an upgrade are identified for
    indexing.
    """
    ignored(mock_check_index)  # mocked for side effect
    app = testapp.app
    es = None  # ES component mocked
    item_type = "testing_biosample_sno"
    biosample_uuid = biosample["uuid"]
    with mock.patch("snovault.elasticsearch.create_mapping.scan", new=mock_scan()):
        # Mocked scan found no uuids to upgrade
        to_upgrade = get_items_to_upgrade(app, es, item_type)
        assert not to_upgrade
    with mock.patch(
        "snovault.elasticsearch.create_mapping.scan",
        new=mock_scan(uuids=[biosample_uuid])
    ):
        # Mocked scan found the biosample uuid to upgrade
        to_upgrade = get_items_to_upgrade(app, es, item_type)
        assert to_upgrade == {biosample_uuid}
