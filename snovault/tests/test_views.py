import os
import pytest

from base64 import b64encode
from jsonschema_serialize_fork import Draft4Validator
from pyramid.compat import ascii_native_
from .. import TYPES
from .toolfixtures import registry, root


TYPE_NAMES = ['TestingPostPutPatchSno', 'TestingDownload']


def get_parameterized_names():
    """ Get all item types from schema names """
    return [name.split('.')[0] for name in os.listdir(os.getcwd() + '/snovault/test_schemas')]

PARAMETERIZED_NAMES = get_parameterized_names()


@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_collections(testapp, item_type):
    """ Get all item types, check they are in the response """
    res = testapp.get('/' + item_type).follow(status=200)
    assert item_type.encode('utf-8') in res.body


@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_json(testapp, item_type):
    """ Check that when we get proper item types """
    res = testapp.get('/' + item_type).follow(status=200)
    assert (item_type + 'Collection') in res.json['@type']


def test_json_basic_auth(anontestapp):
    url = '/'
    value = "Authorization: Basic %s" % ascii_native_(b64encode(b'nobody:pass'))
    res = anontestapp.get(url, headers={'Authorization': value}, status=401)
    assert res.content_type == 'application/json'


def test_home_json(testapp):
    res = testapp.get('/', status=200)
    assert res.json['@type']


def test_collection_post_bad_json(testapp):
    item = {'foo': 'bar'}
    res = testapp.post_json('/embedding-tests', item, status=422)
    assert res.json['status'] == 'error'


def test_collection_post_malformed_json(testapp):
    item = '{'
    headers = {'Content-Type': 'application/json'}
    res = testapp.post('/embedding-tests', item, status=400, headers=headers)
    assert res.json['detail'].startswith('Expecting')


def test_collection_post_missing_content_type(testapp):
    item = '{}'
    testapp.post('/embedding-tests', item, status=415)


def test_collection_post_bad(anontestapp):
    value = "Authorization: Basic %s" % ascii_native_(b64encode(b'nobody:pass'))
    anontestapp.post_json('/embedding-tests', {}, headers={'Authorization': value}, status=401)


@pytest.mark.slow
def test_collection_limit(testapp):
    """ Post 3 EmbeddingTests, check that limit=all, limit=2 works """
    obj1 = {
        'title': "Testing1",
        'description': "This is testig object 1",
    }
    obj2 = {
        'title': "Testing2",
        'description': "This is testig object 2",
    }
    obj3 = {
        'title': "Testing3",
        'description': "This is testig object 3",
    }
    testapp.post_json('/embedding-tests', obj1, status=201)
    testapp.post_json('/embedding-tests', obj2, status=201)
    testapp.post_json('/embedding-tests', obj3, status=201)
    res_all = testapp.get('/embedding-tests/?limit=all', status=200)
    res_2 = testapp.get('/embedding-tests/?limit=2', status=200)
    assert len(res_all.json['@graph']) == 3
    assert len(res_2.json['@graph']) == 2

def test_collection_put(testapp, execute_counter):
    """ Insert and udpate an item into a collection, verify it worked """
    initial = {
        'title': "Testing",
        'type': "object", # include a non-required field
        'description': "This is the initial insert",
    }
    item_url = testapp.post_json('/embedding-tests', initial).location

    with execute_counter.expect(1):
        item = testapp.get(item_url).json

    for key in initial:
        assert item[key] == initial[key]

    update = {
        'title': "New Testing",
        'type': "object",
        'description': "This is the updated insert",
    }
    testapp.put_json(item_url, update, status=200)

    res = testapp.get('/' + item['uuid']).follow().json

    for key in update:
        assert res[key] == update[key]


def test_invalid_collection_put(testapp):
    """ Tests that inserting various invalid items will appropriately fail """
    missing_required = {
        'title': "Testing",
        'type': "object"
    }
    testapp.post_json('/embedding-tests', missing_required, status=422)

    nonexistent_field = {
        'title': "Testing",
        'type': "string",
        'descriptionn': "This is a descriptionn", # typo
    }
    testapp.post_json('/embedding-tests', nonexistent_field, status=422)

    valid = {
        'title': "Testing",
        'type': "object",
        'description': "This is a valid object",
    }
    invalid_update = {
        'descriptionn': "This is an invalid update",
    }
    item_url = testapp.post_json('/embedding-tests', valid, status=201).location
    testapp.put_json(item_url, invalid_update, status=422)


def test_page_toplevel(anontestapp):
    res = anontestapp.get('/embedding-tests/', status=200)
    assert res.json['@id'] == '/embedding-tests/'


def test_jsonld_context(testapp):
    res = testapp.get('/terms/', status=200)
    assert res.json


@pytest.mark.slow
@pytest.mark.parametrize('item_type', PARAMETERIZED_NAMES)
def test_index_data_workbook(testapp, indexer_testapp, item_type):
    res = testapp.get('/%s?limit=all' % item_type).follow(status=200)
    for item in res.json['@graph']:
        indexer_testapp.get(item['@id'] + '@@index-data', status=200)


@pytest.mark.xfail
def test_abstract_collection(testapp, experiment):
    testapp.get('/Dataset/{accession}'.format(**experiment))
    testapp.get('/datasets/{accession}'.format(**experiment))


def test_home(testapp):
    testapp.get('/', status=200)


@pytest.mark.parametrize('item_type', TYPE_NAMES)
def test_profiles(testapp, item_type):
    res = testapp.get('/profiles/%s.json' % item_type).maybe_follow(status=200)
    errors = Draft4Validator.check_schema(res.json)
    assert not errors
    # added from ..schema_views._annotated_schema
    assert 'rdfs:seeAlso' in res.json
    assert 'rdfs:subClassOf' in res.json
    assert 'children' in res.json
    assert res.json['isAbstract'] is False


@pytest.mark.parametrize('item_type', ['AbstractItemTest'])
def test_profiles_abstract(testapp, item_type):
    res = testapp.get('/profiles/%s.json' % item_type).maybe_follow(status=200)
    errors = Draft4Validator.check_schema(res.json)
    assert not errors
    # added from ..schema_views._annotated_schema
    assert 'rdfs:seeAlso' in res.json
    # Item/item does not have subClass
    if item_type.lower() == 'item':
        assert 'rdfs:subClassOf' not in res.json
    else:
        assert 'rdfs:subClassOf' in res.json
    # abstract types wil have children
    assert len(res.json['children']) >= 1
    assert res.json['isAbstract'] is True


def test_profiles_all(testapp, registry):
    res = testapp.get('/profiles/').maybe_follow(status=200)
    # make sure all types are present, including abstract types
    for ti in registry[TYPES].by_item_type.values():
        assert ti.name in res.json
    for ti in registry[TYPES].by_abstract_type.values():
        assert ti.name in res.json
