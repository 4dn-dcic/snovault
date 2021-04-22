import pytest


targets = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
    {'name': 'two', 'uuid': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377'},
]

item = {
    'required': 'required value',
}

simple1 = {
    'required': 'required value',
    'simple1': 'supplied simple1',
}

simple2 = {
    'required': 'required value',
    'simple2': 'supplied simple2',
}

item_with_uuid = [
    {
        'uuid': '0f13ff76-c559-4e70-9497-a6130841df9f',
        'required': 'required value 1',
        'field_no_default': 'test'

    },
    {
        'uuid': '6c3e444b-f290-43c4-bfb9-d20135377770',
        'required': 'required value 2',
    },
]

item_with_link = [
    {
        'required': 'required value 1',
        'protected_link': '775795d3-4410-4114-836b-8eeecf1d0c2f',
    },
    {
        'required': 'required value 2',
        'protected_link': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377',
    },
]


COLLECTION_URL = '/testing-post-put-patch-sno/'


@pytest.fixture
def link_targets(testapp):
    url = '/testing-link-targets-sno/'
    for item in targets:
        testapp.post_json(url, item, status=201)


@pytest.fixture
def content(testapp, external_tx):
    res = testapp.post_json(COLLECTION_URL, item_with_uuid[0], status=201)
    return {'@id': res.location}


@pytest.fixture
def content_with_child(testapp):
    parent_res = testapp.post_json('/testing-link-targets-sno/', {}, status=201)
    parent_id = parent_res.json['@graph'][0]['@id']
    child_res = testapp.post_json('/testing-link-sources-sno/', {'target': parent_id})
    child_id = child_res.json['@graph'][0]['@id']
    return {'@id': parent_id, 'child': child_id}


def test_admin_post(testapp, external_tx):
    testapp.post_json(COLLECTION_URL, item, status=201)
    testapp.post_json(COLLECTION_URL, item_with_uuid[0], status=201)


def test_admin_put_uuid(content, testapp):
    url = content['@id']
    # so long as the same uuid is supplied, PUTing the uuid is fine
    testapp.put_json(url, item_with_uuid[0], status=200)
    # but the uuid may not be changed on PUT;
    testapp.put_json(url, item_with_uuid[1], status=422)


def test_defaults_on_put(content, testapp):
    url = content['@id']
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'
    assert res.json['simple2'] == 'simple2 default'

    res = testapp.put_json(url, simple1, status=200)
    assert res.json['@graph'][0]['simple1'] == 'supplied simple1'
    assert res.json['@graph'][0]['simple2'] == 'simple2 default'

    res = testapp.put_json(url, simple2, status=200)
    assert res.json['@graph'][0]['simple1'] == 'simple1 default'
    assert res.json['@graph'][0]['simple2'] == 'supplied simple2'


def test_patch(content, testapp):
    """ Augmented to also test revision history """
    url = content['@id']
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'
    assert res.json['simple2'] == 'simple2 default'

    res = testapp.patch_json(url, {}, status=200)
    assert res.json['@graph'][0]['simple1'] == 'simple1 default'
    assert res.json['@graph'][0]['simple2'] == 'simple2 default'

    res = testapp.patch_json(url, {'simple1': 'supplied simple1'}, status=200)
    assert res.json['@graph'][0]['simple1'] == 'supplied simple1'
    assert res.json['@graph'][0]['simple2'] == 'simple2 default'

    res = testapp.patch_json(url, {'simple2': 'supplied simple2'}, status=200)
    assert res.json['@graph'][0]['simple1'] == 'supplied simple1'
    assert res.json['@graph'][0]['simple2'] == 'supplied simple2'

    revisions = testapp.get(url + '/@@revision-history').json['revisions']
    assert len(revisions) == 4


def test_patch_delete_fields(content, testapp):
    url = content['@id']
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'
    assert res.json['simple2'] == 'simple2 default'
    assert res.json['field_no_default'] == 'test'

    res = testapp.patch_json(url, {'simple1': 'this is a test'}, status=200)
    assert res.json['@graph'][0]['simple1'] == 'this is a test'

    # delete fields with defaults resets to default, while deleting non default field
    # completely removes them
    res = testapp.patch_json(url + "?delete_fields=simple1,field_no_default", {}, status=200)
    assert 'field_no_default' not in res.json['@graph'][0].keys()
    assert res.json['@graph'][0]['simple1'] == 'simple1 default'


def test_patch_delete_fields_enum(content, testapp):
    """
    enum with no default set, should delete completely
    """
    url = content['@id']
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'
    assert res.json['simple2'] == 'simple2 default'

    res = testapp.patch_json(url, {'enum_no_default': '2', 'simple1': 'this is a test'}, status=200)
    assert res.json['@graph'][0]['enum_no_default'] == '2'
    assert res.json['@graph'][0]['simple1'] == 'this is a test'

    # delete enums without a default removes it completely
    res = testapp.patch_json(url + "?delete_fields=simple1,enum_no_default", {}, status=200)
    assert 'enum_no_default' not in res.json['@graph'][0].keys()
    assert res.json['@graph'][0]['simple1'] == 'simple1 default'


def test_patch_delete_fields_non_string(content, testapp):
    url = content['@id']
    res = testapp.get(url)

    # delete fields with defaults resets to default, while deleting non default field
    # completely removes them
    res = testapp.patch_json(url + "?delete_fields=schema_version", {}, status=200)
    assert res.json['@graph'][0]['schema_version'] == '1'


def test_patch_delete_fields_fails_with_no_validation(content, testapp):
    url = content['@id']
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'
    assert res.json['simple2'] == 'simple2 default'
    assert res.json['field_no_default'] == 'test'

    # using delete_fields with validate=False will now raise a validation err
    res = testapp.patch_json(url + "?delete_fields=simple1,field_no_default&validate=false", {}, status=422)
    assert res.json['description'] == "Failed validation"
    assert 'Cannot delete fields' in res.json['errors'][0]['description']


def test_patch_delete_fields_bad_param(content, testapp):
    """
    delete_fields should not fail with a bad fieldname, but simply ignore
    """
    url = content['@id']
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'
    assert res.json['simple2'] == 'simple2 default'
    assert res.json['field_no_default'] == 'test'
    res = testapp.patch_json(url + "?delete_fields=simple1,bad_fieldname", {}, status=200)
    # default value
    assert res.json['@graph'][0]['simple1'] == 'simple1 default'
    assert 'bad_fieldname' not in res.json['@graph'][0]


def test_patch_delete_fields_import_items_admin(link_targets, testapp):
    res = testapp.post_json(COLLECTION_URL, item_with_link[0], status=201)
    url = res.location
    assert res.json['@graph'][0]['protected_link']
    res = testapp.patch_json(url + "?delete_fields=protected_link", {}, status=200)


def test_patch_delete_fields_required(content, testapp):
    url = content['@id']
    res = testapp.get(url)

    # with validate=false, then defaults are not populated so default fields are also deleted
    res = testapp.patch_json(url + "?delete_fields=required", {}, status=422)
    assert res.json['description'] == "Failed validation"
    assert res.json['errors'][0]['name'] == "Schema: "
    assert res.json['errors'][0]['description'] == "'required' is a required property"


def test_patch_new_schema_version(content, root, testapp, monkeypatch):
    collection = root['testing_post_put_patch_sno']
    properties = collection.type_info.schema['properties']

    url = content['@id']
    res = testapp.get(url)
    assert res.json['schema_version'] == '1'

    monkeypatch.setitem(properties['schema_version'], 'default', '2')
    monkeypatch.setattr(collection.type_info, 'schema_version', '2')
    monkeypatch.setitem(properties, 'new_property', {'default': 'new'})
    res = testapp.patch_json(url, {}, status=200)
    assert res.json['@graph'][0]['schema_version'] == '2'
    assert res.json['@graph'][0]['new_property'] == 'new'


def test_admin_put_protected_link_revision_history(link_targets, testapp):
    """ Admin can update protected fields that are links.
        Also tests that links show up in revision history.
    """
    res = testapp.post_json(COLLECTION_URL, item_with_link[0], status=201)
    url = res.location

    testapp.put_json(url, item_with_link[0], status=200)
    testapp.put_json(url, item_with_link[1], status=200)
    revisions = testapp.get(url + '/@@revision-history').json['revisions']
    for target_uuid, revision in zip([
        '775795d3-4410-4114-836b-8eeecf1d0c2f',  # initial post
        '775795d3-4410-4114-836b-8eeecf1d0c2f',  # first PUT
        'd6784f5e-48a1-4b40-9b11-c8aefb6e1377'  # second PUT (changes link target)
    ], revisions):
        assert revision['protected_link'] == target_uuid


def test_put_object_not_touching_children(content_with_child, testapp):
    url = content_with_child['@id']
    res = testapp.put_json(url, {}, status=200)
    assert content_with_child['child'] in res.json['@graph'][0]['reverse']


def test_put_object_editing_child(content_with_child, testapp):
    edit = {
        'reverse': [{
            '@id': content_with_child['child'],
            'status': 'released',
        }]
    }
    # this is no longer allowed
    res = testapp.put_json(content_with_child['@id'], edit, status=422)
    assert res.json['description'] == 'Failed validation'
    assert len(res.json['errors']) == 1
    res_error = res.json['errors'][0]
    assert res_error['name'] == 'Schema: reverse'
    assert res_error['description'] == 'submission of calculatedProperty disallowed'


def test_name_key_validation(link_targets, testapp):
    # name_key
    target_data = {'name': 'one#name'}
    res = testapp.post_json('/testing-link-targets-sno/', target_data, status=422)
    assert res.json['description'] == 'Failed validation'
    res_error = res.json['errors'][0]
    assert res_error['name'] == "Item: path characters"
    assert "Forbidden character(s) {'#'}" in res_error['description']

    # unique_key
    source_data = {'name': 'two@*name', 'target': targets[0]['uuid']}
    res = testapp.post_json('/testing-link-sources-sno/', source_data, status=422)
    assert res.json['description'] == 'Failed validation'
    res_error = res.json['errors'][0]
    assert res_error['name'] == 'Item: path characters'
    assert "Forbidden character(s) {'*'}" in res_error['description']


def test_retry(testapp):
    res = testapp.post_json(COLLECTION_URL, {'required': ''})
    url = res.location
    res = testapp.get(url + '/@@testing-retry?datastore=database')
    assert res.json['attempt'] == 2
    assert not res.json['detached']


def test_check_only(content, testapp):
    """ Tests check_only doesn't actually touch DB. """
    url = content['@id']
    # PATCH
    res = testapp.patch_json(url + '?check_only=True',
                             {'simple1': 'supplied simple1'}, status=200)
    assert res.json['status'] == 'success'
    assert res.json['@type'] == ['result']
    # item did not change
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'

    # PUT
    altered_item = item_with_uuid[0].copy()
    altered_item['simple1'] = 'supplied simple1'
    res = testapp.put_json(url+ '?check_only=True', item_with_uuid[0], status=200)
    assert res.json['status'] == 'success'
    assert res.json['@type'] == ['result']
    # item did not change
    res = testapp.get(url)
    assert res.json['simple1'] == 'simple1 default'

    # POST
    res = testapp.post_json(COLLECTION_URL + '?check_only=True', {'required': ''})
    assert res.json['status'] == 'success'
    assert res.json['@type'] == ['result']

    # check_only raises validation errors
    res = testapp.patch_json(url + "?check_only=True&delete_fields=required", {}, status=422)
    assert len(res.json['errors']) == 2
    val_error = res.json['errors'][0]
    assert val_error['name'] == 'Schema: '
    assert val_error['description'] == "'required' is a required property"
    del_error = res.json['errors'][1]
    assert del_error['name'] == 'delete_fields'
    assert del_error['description'] == 'Error deleting fields'

    # no additional revisions should show up in db
    revisions = testapp.get(url + '/@@revision-history').json['revisions']
    assert len(revisions) == 1


def test_max_sid_view(content, testapp):
    res = testapp.get('/max-sid', status=200)
    assert res.json['status'] == 'success'
    assert 'max_sid' in res.json
    starting_sid = res.json['max_sid']
    # increment sid and make sure it is updated
    url = content['@id']
    testapp.patch_json(url, {}, status=200)

    res = testapp.get('/max-sid', status=200)
    assert res.json['status'] == 'success'
    assert res.json['max_sid'] > starting_sid


def test_create_es_item_without_es(content, testapp):
    """
    Items with `properties_datastore='elasticsearch'` should fail without ES set up
    """
    target_data = {'name': 'es_target_test'}
    res = testapp.post_json('/testing-link-targets-elastic-search/', target_data, status=500)
    assert res.json['detail'] == 'Forced datastore elasticsearch is not configured'
