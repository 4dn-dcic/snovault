import pytest
from snovault.util import (
    build_default_embeds,
    find_default_embeds_for_schema,
    expand_embedded_list,
    crawl_schema
)

def test_find_collection_subtypes(app):
    from snovault.util import find_collection_subtypes
    test_item_type = 'Snowset'
    expected_types = ['snowball', 'snowfort']
    res = find_collection_subtypes(app.registry, test_item_type)
    assert sorted(res) == expected_types


def test_build_default_embeds():
    """ simple unit test """
    embeds_to_add = ['obj1.obj2', 'obj1', 'obj1.obj3.*']
    processed_embeds = {'obj1.*'}
    final_embeds = build_default_embeds(embeds_to_add, processed_embeds)
    expected_embeds = [
        'obj1.obj2.display_title',
        'obj1.obj2.@id',
        'obj1.obj2.uuid',
        'obj1.obj2.principals_allowed.*',
        'obj1.*',
        'obj1.obj3.*'
    ]
    assert(set(final_embeds) == set(expected_embeds))


def test_find_default_embeds_and_expand_emb_list(registry):
    from snovault import TYPES
    # use snowflake as test case
    type_info = registry[TYPES].by_item_type['snowflake']
    schema_props = type_info.schema.get('properties')
    default_embeds = find_default_embeds_for_schema('', schema_props)
    expected_embeds = ['lab', 'award', 'submitted_by', 'snowset', 'principals_allowed.*']
    assert(set(default_embeds) == set(expected_embeds))
    # lets use the default embeds as an "embedded_list" for snowflake
    dummy_emb_list = [emb + '.*' if not emb.endswith('*') else emb for emb in expected_embeds ]
    embs_to_add, proc_embs = expand_embedded_list('snowflake', registry[TYPES], dummy_emb_list, schema_props, set())
    expected_to_add = ['lab', 'lab.pi', 'lab.awards', 'lab.principals_allowed.*',
                       'award', 'award.pi', 'award.principals_allowed.*', 'submitted_by',
                       'submitted_by.lab', 'submitted_by.submits_for', 'submitted_by.principals_allowed.*',
                       'snowset', 'snowset.lab', 'snowset.award', 'snowset.principals_allowed.*', 'snowset.submitted_by']
    assert(set(embs_to_add) == set(expected_to_add))

    # add default embeds for all items in a path
    test_embed = ['snowset.award.uuid']
    embs_to_add2, proc_embs2 = expand_embedded_list('snowflake', registry[TYPES], test_embed, schema_props, set())
    expected_to_add2 = ['snowset', 'snowset.award']
    assert(set(embs_to_add2) == set(expected_to_add2))
    # lastly check the built embeds
    expected_built = ['snowset.display_title', 'snowset.@id', 'snowset.principals_allowed.*',
                      'snowset.uuid', 'snowset.award.display_title', 'snowset.award.@id',
                      'snowset.award.principals_allowed.*', 'snowset.award.uuid']
    assert set(expected_built) == set(build_default_embeds(embs_to_add2, set()))


def test_crawl_schema(registry):
    from snovault import TYPES
    from copy import deepcopy
    field_path = 'lab.awards.title'
    snowflake_schema = registry[TYPES].by_item_type['snowflake'].schema
    res = crawl_schema(registry[TYPES], field_path, snowflake_schema)
    assert isinstance(res, dict)
    assert res['type'] == 'string'

    # test some bad cases.
    with pytest.raises(Exception) as exec_info:
        crawl_schema(registry[TYPES], field_path, 'not_a_schema')
    # different error, since it attempts to find the file locally
    assert 'Invalid starting schema' in str(exec_info)

    field_path2 = 'lab.awards.title.title'
    with pytest.raises(Exception) as exec_info2:
        crawl_schema(registry[TYPES], field_path2, snowflake_schema)
    # different error, since it attempts to find the file locally
    assert 'Non-dictionary schema' in str(exec_info2)

    field_path3 = 'lab.awards.not_a_field'
    with pytest.raises(Exception) as exec_info3:
        crawl_schema(registry[TYPES], field_path3, snowflake_schema)
    # different error, since it attempts to find the file locally
    assert 'Field not found' in str(exec_info3)

    # screw with the schema to create an invalid linkTo
    snowflake_schema = registry[TYPES].by_item_type['snowflake'].schema
    schema_copy = deepcopy(snowflake_schema)
    schema_copy['properties']['lab']['linkTo'] = 'NotAnItem'
    with pytest.raises(Exception) as exec_info4:
        crawl_schema(registry[TYPES], field_path, schema_copy)
    # different error, since it attempts to find the file locally
    assert 'Invalid linkTo' in str(exec_info4)
