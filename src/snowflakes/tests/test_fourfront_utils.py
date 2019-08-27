import pytest
from snovault.util import (
    build_default_embeds,
    find_default_embeds_for_schema,
    expand_embedded_list,
    crawl_schema
)


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
