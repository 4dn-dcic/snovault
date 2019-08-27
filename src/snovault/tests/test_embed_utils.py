import pytest
from snovault.util import (
    build_default_embeds,
    find_default_embeds_for_schema,
    find_collection_subtypes,
    expand_embedded_list,
    crawl_schema
)

def test_find_collection_subtypes(app):
    test_item_type = 'AbstractItemTest' 
    expected_types = ['AbstractItemTestSecondSubItem', 'AbstractItemTestSubItem']
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
    from snovault import TYPES
    # use EmbeddingTest as test case, linkTo TestingDownload
    type_info = registry[TYPES].by_item_type['EmbeddingTest']
    schema_props = type_info.schema.get('properties')
    default_embeds = find_default_embeds_for_schema('', schema_props)
    expected_embeds = ['attachment', 'principals_allowed.*']
    assert(set(default_embeds) == set(expected_embeds))

    # get expansions from attachment    
    dummy_emb_list = [emb + '.*' if not emb.endswith('*') else emb for emb in expected_embeds ]
    embs_to_add, _ = expand_embedded_list('EmbeddingTest', registry[TYPES], dummy_emb_list, schema_props, set())
    expected_to_add = ['attachment', 'attachment.attachment.*', 'attachment.attachment2.*', 'attachment.principals_allowed.*']
    assert(set(embs_to_add) == set(expected_to_add))

    # add default embeds for all items in a path
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

