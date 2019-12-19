import pytest


@pytest.fixture
def TestKey(testapp):
    """ Posts an item under testing_keys_schema """
    url = '/testing-keys'
    item = {
        'name': 'Orange',
        'grouping': 'fruit',
        'obj_id': '123'
    }
    testapp.post_json(url, item, status=201)


@pytest.fixture
def TestKeyDefinition(testapp):
    """ Posts an item under testing_keys_def """
    url = '/testing-keys-def'
    item = {
        'name': 'Orange',
        'grouping': 'fruit',
        'obj_id': '123',
        'system_id': 'abc'
    }
    testapp.post_json(url, item, status=201)


@pytest.fixture
def TestKeyName(testapp):
    """ Posts an item under testing_keys_name """
    url = '/testing-keys-name'
    item = {
        'name': 'Orange',
        'grouping': 'fruit',
        'obj_id': '123',
        'system_id': 'abc'
    }
    testapp.post_json(url, item, status=201)


def test_schema_unique_key(TestKey, testapp):
    """
    Tests that when we define a uniqueKey on the schema that we cannot post a
    second item with a repeat key value in any fields
    """
    url = '/testing-keys'
    duplicate_name = {
        'name': 'Orange',
        'grouping': 'fruit',
        'obj_id': '456',
        'system_id': 'def'
    }
    testapp.post_json(url, duplicate_name, status=409)
    duplicate_id = {
        'name': 'Banana',
        'grouping': 'fruit',
        'obj_id': '123',
        'system_id': 'def'
    }
    testapp.post_json(url, duplicate_id, status=409)
    duplicate_system_id = {
        'name': 'Banana',
        'grouping': 'fruit',
        'obj_id': '456',
        'system_id': 'abc'
    }  # should be allowed since not marked as unique wrt the DB
    testapp.post_json(url, duplicate_system_id, status=201)
    correct = {
        'name': 'Apple',
        'grouping': 'fruit',
        'obj_id': '789',
        'system_id': 'hij'
    }  # should also work since all fields are unique
    testapp.post_json(url, correct, status=201)
    # both gets should not work since obj_id and name not are name_keys
    testapp.get(url + '/' + correct['obj_id'], status=404)
    testapp.get(url + '/' + correct['name'], status=404)


def test_definition_unique_key(TestKeyDefinition, testapp):
    """
    Tests that using 'obj_id' as the name key allows lookup via resource path
    using 'obj_id' and not 'name'
    """
    url = '/testing-keys-def'
    duplicate_id = {
        'name': 'Banana',
        'grouping': 'fruit',
        'obj_id': '123',
        'system_id': 'def'
    }  # sanity: posting should fail as above since obj_id is a uniqueKey
    testapp.post_json(url, duplicate_id, status=409)
    duplicate_system_id = {
        'name': 'Banana',
        'grouping': 'fruit',
        'obj_id': '456',
        'system_id': 'abc'
    }  # this will work despite system_id being marked as unique_key in definition
    testapp.post_json(url, duplicate_system_id, status=201)
    # obj_id should succeed since it is a unique_key
    resp = testapp.get(url + '/' + duplicate_system_id['obj_id']).follow(status=200).json
    # since obj_id is a identification_key but not a name_key we still expect uuid
    assert resp['uuid'] in resp['@id']
    # should fail since name is not a name_key
    testapp.get(url + '/' + duplicate_system_id['name'], status=404)


def test_name_key(TestKeyName, testapp):
    """
    Tests that using 'name' as the name key allows lookup via resource path
    using 'name' and not 'obj_id'
    """
    url = '/testing-keys-name'
    duplicate_id = {
        'name': 'Banana',
        'grouping': 'fruit',
        'obj_id': '123',
        'system_id': 'def'
    }  # sanity: posting should fail as above since obj_id is a uniqueKey
    testapp.post_json(url, duplicate_id, status=409)
    correct = {
        'name': 'Apple',
        'grouping': 'fruit',
        'obj_id': '789',
        'system_id': 'hij'
    }
    testapp.post_json(url, correct, status=201)
    # obj_id should succeed since it is a name_key
    resp = testapp.get(url + '/' + correct['name']).follow(status=200).json
    assert resp['name'] in resp['@id']  # @id is name since its the name_key
    testapp.get(url + '/' + correct['obj_id'], status=404)


def test_name_key_identification_key_mismatch(testapp):
    """
    Tries to post an item to a type with mismatched traversal and name key
    which will fail
    """
    url = '/testing-keys-mismatch'
    item = {
        'name': 'Orange',
        'grouping': 'fruit',
        'obj_id': '123',
        'system_id': 'abc'
    }
    with pytest.raises(KeyError):  # error in embed.py:181
        testapp.post_json(url, item)
