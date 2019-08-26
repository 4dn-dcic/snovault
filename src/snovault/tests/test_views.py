import pytest
from snovault import TYPES
TYPE_NAMES = ['TestingPostPutPatchSno', 'TestingDownload']


@pytest.mark.parametrize('item_type', TYPE_NAMES)
def test_profiles(testapp, item_type):
    from jsonschema_serialize_fork import Draft4Validator
    res = testapp.get('/profiles/%s.json' % item_type).maybe_follow(status=200)
    errors = Draft4Validator.check_schema(res.json)
    assert not errors
    # added from snovault.schema_views._annotated_schema
    assert 'rdfs:seeAlso' in res.json
    assert 'rdfs:subClassOf' in res.json
    assert 'children' in res.json
    assert res.json['isAbstract'] is False


# @pytest.mark.parametrize('item_type', ['Item', 'item', 'Snowset', 'snowset'])
# def test_profiles_abstract(testapp, item_type):
#     from jsonschema_serialize_fork import Draft4Validator
#     res = testapp.get('/profiles/%s.json' % item_type).maybe_follow(status=200)
#     errors = Draft4Validator.check_schema(res.json)
#     assert not errors
#     # added from snovault.schema_views._annotated_schema
#     assert 'rdfs:seeAlso' in res.json
#     # Item/item does not have subClass
#     if item_type.lower() == 'item':
#         assert 'rdfs:subClassOf' not in res.json
#     else:
#         assert 'rdfs:subClassOf' in res.json
#     # abstract types wil have children
#     assert len(res.json['children']) >= 1
#     assert res.json['isAbstract'] is True
#
#
# def test_profiles_all(testapp, registry):
#     from jsonschema_serialize_fork import Draft4Validator
#     res = testapp.get('/profiles/').maybe_follow(status=200)
#     # make sure all types are present, including abstract types
#     for ti in registry[TYPES].by_item_type.values():
#         assert ti.name in res.json
#     for ti in registry[TYPES].by_abstract_type.values():
#         assert ti.name in res.json
#
# def test_bad_frame(testapp, award):
#     res = testapp.get(award['@id'] + '?frame=bad', status=404)
#     assert res.json['detail'] == '?frame=bad'
