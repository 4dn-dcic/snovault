import pytest

from copy import deepcopy
from .. import TYPES
from ..util import (
    build_default_embeds,
    find_default_embeds_for_schema,
    find_collection_subtypes,
    expand_embedded_list,
    crawl_schema
)
from .toolfixtures import registry


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
    dummy_emb_list = [emb + '.*' if not emb.endswith('*') else emb for emb in expected_embeds ]
    embs_to_add, _ = expand_embedded_list('EmbeddingTest', registry[TYPES], dummy_emb_list, schema_props, set())
    expected_to_add = ['attachment', 'attachment.attachment.*', 'attachment.attachment2.*', 'attachment.principals_allowed.*']
    assert(set(embs_to_add) == set(expected_to_add))

    # add default embeds for all items 'attachment'
    test_embed = ['attachment.attachment.*']
    embs_to_add2, _ = expand_embedded_list('EmbeddingTest', registry[TYPES], test_embed, schema_props, set())
    expected_to_add2 = ['attachment']
    assert(set(embs_to_add2) == set(expected_to_add2))
    # lastly check the built embeds
    expected_built = ['attachment.display_title',
                      'attachment.@type',
                      'attachment.uuid',
                      'attachment.@id',
                      'attachment.principals_allowed.*']
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
    schema_copy = deepcopy(embedding_schema)
    schema_copy['properties']['attachment']['linkTo'] = 'NotAnItem'
    with pytest.raises(Exception) as exec_info4:
        crawl_schema(registry[TYPES], field_path, schema_copy)
    # different error, since it attempts to find the file locally
    assert 'Invalid linkTo' in str(exec_info4)
