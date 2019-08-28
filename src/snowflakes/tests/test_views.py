import pytest
from snovault import TYPES


def _type_length():
    # Not a fixture as we need to parameterize tests on this
    from ..loadxl import ORDER
    from pkg_resources import resource_stream
    import codecs
    import json
    utf8 = codecs.getreader("utf-8")
    return {
        name: len(json.load(utf8(resource_stream('snowflakes', 'tests/data/inserts/%s.json' % name))))
        for name in ORDER
    }


TYPE_LENGTH = _type_length()

PUBLIC_COLLECTIONS = [
    'lab',
    'award',
]


# relevant?
def test_load_sample_data(
        award,
        lab,
        submitter,
        ):
    assert True, 'Fixtures have loaded sample data'



# XXX: can probably be repurposed
def test_post_duplicate_uuid(testapp, award):
    item = {
        'uuid': award['uuid'],
        'name': 'NIS39393',
        'title': 'Grant to make snow',
        'project': 'ENCODE',
        'rfa': 'ENCODE3',
    }
    testapp.post_json('/award', item, status=409)

# XXX: how to set up?
def test_user_effective_principals(submitter, lab, anontestapp, execute_counter):
    email = submitter['email']
    with execute_counter.expect(1):
        res = anontestapp.get('/@@testing-user',
                              extra_environ={'REMOTE_USER': str(email)})
    assert sorted(res.json['effective_principals']) == [
        'group.submitter',
        'lab.%s' % lab['uuid'],
        'remoteuser.%s' % email,
        'submits_for.%s' % lab['uuid'],
        'system.Authenticated',
        'system.Everyone',
        'userid.%s' % submitter['uuid'],
        'viewing_group.ENCODE',
    ]


# N/A?
def test_page_nested(workbook, anontestapp):
    res = anontestapp.get('/test-section/subpage/', status=200)
    assert res.json['@id'] == '/test-section/subpage/'

# N/A?
def test_page_nested_in_progress(workbook, anontestapp):
    return anontestapp.get('/test-section/subpage-in-progress/', status=403)

# N/A?
def test_page_homepage(workbook, anontestapp):
    res = anontestapp.get('/pages/homepage/', status=200)
    assert res.json['canonical_uri'] == '/'

    res = anontestapp.get('/', status=200)
    assert 'default_page' in res.json
    assert res.json['default_page']['@id'] == '/pages/homepage/'

# N/A?
def test_page_collection_default(workbook, anontestapp):
    res = anontestapp.get('/pages/images/', status=200)
    assert res.json['canonical_uri'] == '/images/'

    res = anontestapp.get('/images/', status=200)
    assert 'default_page' in res.json
    assert res.json['default_page']['@id'] == '/pages/images/'
