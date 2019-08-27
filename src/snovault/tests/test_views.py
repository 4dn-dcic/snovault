import pytest
from snovault import TYPES
TYPE_NAMES = ['TestingPostPutPatchSno', 'TestingDownload']

# this (sortof) does what type_length
# does in the snowflakes test_views.py
def get_parameterized_names():
    import os
    return [name.split('.')[0] for name in os.listdir(os.getcwd() + '/src/snovault/test_schemas')]

PARAMETERIZED_NAMES = get_parameterized_names()

@pytest.mark.parametrize('item_type', [k for k in PARAMETERIZED_NAMES])
def test_collections(testapp, item_type):
    res = testapp.get('/' + item_type).follow(status=200)
    assert '@graph' in res.json

@pytest.mark.parametrize('item_type', [k for k in PARAMETERIZED_NAMES])
def test_html_collections(testapp, item_type):
    res = testapp.get('/' + item_type).follow(status=200)
    assert res.body.startswith(b'<!DOCTYPE html>')

# works as is
def test_home_json(testapp):
    res = testapp.get('/', status=200)
    assert res.json['@type']

# works as is
def test_vary_json(anontestapp):
    res = anontestapp.get('/', status=200)
    assert res.vary is not None
    assert 'Accept' in res.vary

# XXX tests parameterized on TYPE_LENGTH are still in snowflakes

# works as it (if x failed is expected, which i think it is)
@pytest.mark.xfail
def test_abstract_collection(testapp, experiment):
    testapp.get('/Dataset/{accession}'.format(**experiment))
    testapp.get('/datasets/{accession}'.format(**experiment))

def test_home(testapp):
    res = testapp.get('/', status=200)
    assert res.body.startswith(b'<!DOCTYPE html>')

# works as is
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


# needs modification?
# passes 'AbstractItemTest'
# there are no other abstract collections here to use?
@pytest.mark.parametrize('item_type', ['AbstractItemTest'])
def test_profiles_abstract(testapp, item_type):
    from jsonschema_serialize_fork import Draft4Validator
    res = testapp.get('/profiles/%s.json' % item_type).maybe_follow(status=200)
    errors = Draft4Validator.check_schema(res.json)
    assert not errors
    # added from snovault.schema_views._annotated_schema
    assert 'rdfs:seeAlso' in res.json
    # Item/item does not have subClass
    if item_type.lower() == 'item':
        assert 'rdfs:subClassOf' not in res.json
    else:
        assert 'rdfs:subClassOf' in res.json
    # abstract types wil have children
    assert len(res.json['children']) >= 1
    assert res.json['isAbstract'] is True


# works as is
def test_profiles_all(testapp, registry):
    from jsonschema_serialize_fork import Draft4Validator
    res = testapp.get('/profiles/').maybe_follow(status=200)
    # make sure all types are present, including abstract types
    for ti in registry[TYPES].by_item_type.values():
        assert ti.name in res.json
    for ti in registry[TYPES].by_abstract_type.values():
        assert ti.name in res.json

# needs modification 
# gives 404, i think because /award
# doesn't exist in snovault
def test_bad_frame(testapp, award):
    res = testapp.get(award['@id'] + '?frame=bad', status=404)
    assert res.json['detail'] == '?frame=bad'
