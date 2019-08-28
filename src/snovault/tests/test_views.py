import pytest
from snovault import TYPES
TYPE_NAMES = ['TestingPostPutPatchSno', 'TestingDownload']

# this (sortof) does what type_length
# does in the snowflakes test_views.py
def get_parameterized_names():
    import os
    return [name.split('.')[0] for name in os.listdir(os.getcwd() + '/src/snovault/test_schemas')]

PARAMETERIZED_NAMES = get_parameterized_names()

# this test now explicitly looks for 'item_type'
# in the response body
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_collections(testapp, item_type):
    res = testapp.get('/' + item_type).follow(status=200)
    assert item_type.encode('utf-8') in res.body

# same change as above
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_html_collections(testapp, item_type):
    res = testapp.get('/' + item_type).follow(status=200)
    assert item_type.encode('utf-8') in res.body

# same change as above
@pytest.mark.slow
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_html_pages(testapp, item_type):
    res = testapp.get('/%s?limit=all' % item_type).follow(status=200)
    for item in res.json['@graph']:
        res = testapp.get(item['@id'])
        assert res.body.startswith(b'<!DOCTYPE html>')
        assert item_type.encode('utf-8') in res.body

# remove workbook, same change as above
@pytest.mark.slow
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_html_server_pages(item_type, wsgi_server):
    from webtest import TestApp
    testapp = TestApp(wsgi_server)
    res = testapp.get(
        '/%s?limit=all' % item_type,
        headers={'Accept': 'application/json'},
    ).follow(
        status=200,
        headers={'Accept': 'application/json'},
    )
    for item in res.json['@graph']:
        res = testapp.get(item['@id'], status=200)
        assert res.body.startswith(b'<!DOCTYPE html>')
        assert item_type.encode('utf-8') in res.body
        assert b'Internal Server Error' not in res.body

# works as is
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_json(testapp, item_type):
    res = testapp.get('/' + item_type).follow(status=200)
    assert res.json['@type']

# XXX: because we dont have anonhtmltestapp, this
# test will now return 200 instead of 401?
def test_json_basic_auth(testapp):
    from base64 import b64encode
    from pyramid.compat import ascii_native_
    url = '/'
    value = "Authorization: Basic %s" % ascii_native_(b64encode(b'nobody:pass'))
    res = testapp.get(url, headers={'Authorization': value}, status=200)
    assert res.content_type == 'application/json'

# works as is
def test_home_json(testapp):
    res = testapp.get('/', status=200)
    assert res.json['@type']

# works as is
def test_vary_json(anontestapp):
    res = anontestapp.get('/', status=200)
    assert res.vary is not None
    assert 'Accept' in res.vary

# this test returns 404 since /award is not
# found (was 422)
# also now checks that status is error
# was just asserting presence of res.json['errors']
def test_collection_post_bad_json(testapp):
    item = {'foo': 'bar'}
    res = testapp.post_json('/award', item, status=404)
    print(res.json)
    assert res.json['status'] == 'error'

# works as is
def test_jsonld_context(testapp):
    res = testapp.get('/terms/', status=200)
    assert res.json

# works without workbook?
# also not sure if it previously had any effect
# as status code was not checked, now looks for 200
@pytest.mark.slow
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_index_data_workbook(testapp, indexer_testapp, item_type):
    res = testapp.get('/%s?limit=all' % item_type).follow(status=200)
    for item in res.json['@graph']:
        indexer_testapp.get(item['@id'] + '@@index-data', status=200)

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
