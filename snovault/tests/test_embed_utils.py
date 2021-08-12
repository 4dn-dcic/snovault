import copy
import pytest

# from dcicutils.qa_utils import notice_pytest_fixtures
from ..interfaces import TYPES
from ..util import (
    build_embedded_model,
    build_default_embeds,
    crawl_schema,
    expand_embedded_list,
    find_collection_subtypes,
    find_default_embeds_for_schema,
)

# This is handled in pytest.ini and should not be redundantly here. -kmp 4-Jul-2020
# from .toolfixtures import registry
# notice_pytest_fixtures(registry)


def test_find_collection_subtypes(app):
    test_item_type = 'AbstractItemTest'
    expected_types = ['abstract_item_test_second_sub_item', 'abstract_item_test_sub_item']
    res = find_collection_subtypes(app.registry, test_item_type)
    assert sorted(res) == expected_types


def test_build_default_embeds():
    embeds_to_add = ['obj1.obj2', 'obj1', 'obj1.obj3.*']
    processed_embeds = {'obj1.*'}
    final_embeds = build_default_embeds(embeds_to_add, processed_embeds)
    expected_embeds = [
        'obj1.obj2.display_title',
        'obj1.obj2.@id',
        'obj1.obj2.@type',
        'obj1.obj2.uuid',
        'obj1.obj2.principals_allowed.*',
        'obj1.obj2.status',
        'obj1.*',
        'obj1.obj3.*'
    ]
    assert(set(final_embeds) == set(expected_embeds))


def test_find_default_embeds_and_expand_emb_list(registry):
    # use EmbeddingTest as test case
    # 'attachment' -> linkTo TestingDownload
    type_info = registry[TYPES].by_item_type['embedding_test']
    schema_props = type_info.schema.get('properties')
    default_embeds = find_default_embeds_for_schema('', schema_props)
    expected_embeds = ['attachment', 'principals_allowed.*']
    assert(set(default_embeds) == set(expected_embeds))

    # get expansions from 'attachment'
    dummy_emb_list = [
        emb + '.*'
        if not emb.endswith('*') else emb
        for emb in expected_embeds
    ]
    embs_to_add, _ = expand_embedded_list('EmbeddingTest', registry[TYPES], dummy_emb_list, schema_props, set())
    expected_to_add = [
        'attachment',
        'attachment.attachment.*',
        'attachment.attachment2.*',
        'attachment.principals_allowed.*'
    ]
    assert(set(embs_to_add) == set(expected_to_add))

    # add default embeds for all items 'attachment'
    test_embed = ['attachment.attachment.*']
    embs_to_add2, _ = expand_embedded_list('EmbeddingTest', registry[TYPES], test_embed, schema_props, set())
    expected_to_add2 = ['attachment']
    assert(set(embs_to_add2) == set(expected_to_add2))
    # lastly check the built embeds
    expected_built = [
        'attachment.display_title',
        'attachment.@type',
        'attachment.uuid',
        'attachment.@id',
        'attachment.principals_allowed.*',
        'attachment.status'
    ]
    assert set(expected_built) == set(build_default_embeds(embs_to_add2, set()))


def test_crawl_schema(registry):
    field_path = 'attachment.@type'
    embedding_schema = registry[TYPES].by_item_type['embedding_test'].schema
    res = crawl_schema(registry[TYPES], field_path, embedding_schema)
    assert isinstance(res, dict)
    assert res['type'] == 'array'

    # test some bad cases.
    with pytest.raises(Exception) as exec_info:
        crawl_schema(registry[TYPES], field_path, 'not_a_schema')
    # different error, since it attempts to find the file locally
    assert 'Invalid starting schema' in str(exec_info)

    field_path2 = 'attachment.@id.title'
    with pytest.raises(Exception) as exec_info2:
        crawl_schema(registry[TYPES], field_path2, embedding_schema)
    # different error, since it attempts to find the file locally
    assert 'Non-dictionary schema' in str(exec_info2)

    field_path3 = 'attachment.@types.blah'
    with pytest.raises(Exception) as exec_info3:
        crawl_schema(registry[TYPES], field_path3, embedding_schema)
    # different error, since it attempts to find the file locally
    assert 'Field not found' in str(exec_info3)

    # screw with the schema to create an invalid linkTo
    embedding_schema = registry[TYPES].by_item_type['embedding_test'].schema
    schema_copy = copy.deepcopy(embedding_schema)
    schema_copy['properties']['attachment']['linkTo'] = 'NotAnItem'
    with pytest.raises(Exception) as exec_info4:
        crawl_schema(registry[TYPES], field_path, schema_copy)
    # different error, since it attempts to find the file locally
    assert 'Invalid linkTo' in str(exec_info4)


def test_build_embedded_model():
    res1 = build_embedded_model([
        "link_to_nested_objects.display_title",
        "link_to_nested_object.uuid",
        "link_to_nested_object.@id",
        "link_to_nested_object.display_title",
        "link_to_nested_objects.principals_allowed.*",
        "link_to_nested_objects.associates.y",
        "principals_allowed.*",
        "link_to_nested_objects.uuid",
        "link_to_nested_object.principals_allowed.*",
        "link_to_nested_objects.@type",
        "link_to_nested_objects.@id",
        "link_to_nested_object.associates.x",
        "link_to_nested_object.@type"
    ])
    assert res1 == {
        "fields_to_use": ["*"],
        "link_to_nested_objects": {
            "fields_to_use": ["display_title", "uuid", "@type", "@id"],
            "principals_allowed": {
                "fields_to_use": ["*"]},
            "associates": {
                "fields_to_use": ["y"]
            }
        },
        "link_to_nested_object": {
            "fields_to_use": ["uuid", "@id", "display_title", "@type"],
            "principals_allowed": {
                "fields_to_use": ["*"]
            },
            "associates": {
                "fields_to_use": ["x"]
            }
        },
        "principals_allowed": {
            "fields_to_use": ["*"]
        }
    }
    res2 = build_embedded_model([
        "link_to_nested_objects.display_title",
        "link_to_nested_object.uuid",
        "link_to_nested_object.@id",
        "link_to_nested_object.display_title",
        "link_to_nested_objects.principals_allowed.*",
        "link_to_nested_objects.associates.x",
        "link_to_nested_objects.associates.y",
        "principals_allowed.*",
        "link_to_nested_objects.uuid",
        "link_to_nested_object.principals_allowed.*",
        "link_to_nested_objects.@type",
        "link_to_nested_objects.@id",
        "link_to_nested_object.associates.y",
        "link_to_nested_object.associates.x",
        "link_to_nested_object.@type"
    ])
    assert res2 == {
        "fields_to_use": ["*"],
        "link_to_nested_objects": {
            "fields_to_use": ["display_title", "uuid", "@type", "@id"],
            "principals_allowed": {
                "fields_to_use": ["*"]},
            "associates": {
                "fields_to_use": ["x", "y"]
            }
        },
        "link_to_nested_object": {
            "fields_to_use": ["uuid", "@id", "display_title", "@type"],
            "principals_allowed": {
                "fields_to_use": ["*"]
            },
            "associates": {
                "fields_to_use": ["y", "x"]
            }
        },
        "principals_allowed": {
            "fields_to_use": ["*"]
        }
    }
