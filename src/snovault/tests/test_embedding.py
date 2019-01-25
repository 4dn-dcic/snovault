import pytest

targets = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
    {'name': 'two', 'uuid': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377'},
]

sources = [
    {
        'name': 'A',
        'target': '775795d3-4410-4114-836b-8eeecf1d0c2f',
        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd',
        'status': 'current',
    },
    {
        'name': 'B',
        'target': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377',
        'uuid': '1e152917-c5fd-4aec-b74f-b0533d0cc55c',
        'status': 'deleted',
    },
]


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture
def content(testapp):
    url = '/testing-link-targets-sno/'
    for item in targets:
        testapp.post_json(url, item, status=201)

    url = '/testing-link-sources-sno/'
    for item in sources:
        testapp.post_json(url, item, status=201)

def test_linked_uuids_unset(content, dummy_request, threadlocals):
    # without setting _indexing_view = True on the request,
    # _linked_uuids not tracked and _sid_cache not populated in resource.py
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@object')
    assert dummy_request._linked_uuids == set()
    assert dummy_request._sid_cache == {}


def test_linked_uuids_object(content, dummy_request, threadlocals):
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@object')
    assert dummy_request._linked_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd'}
    assert dummy_request._rev_linked_uuids_by_item == {}
    assert set(dummy_request._sid_cache) == dummy_request._linked_uuids


def test_linked_uuids_embedded(content, dummy_request, threadlocals):
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@embedded')
    assert dummy_request._linked_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}
    # _rev_linked_uuids_by_item is in form {target uuid: set(source uuid)}
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'16157204-8c8f-4672-a1a4-14f4b8021fcd'}
    }
    assert set(dummy_request._sid_cache) == dummy_request._linked_uuids


def test_linked_uuids_page(content, dummy_request, threadlocals):
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@page')
    assert dummy_request._linked_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'16157204-8c8f-4672-a1a4-14f4b8021fcd'}
    }
    assert set(dummy_request._sid_cache) == dummy_request._linked_uuids


def test_linked_uuids_expand_target(content, dummy_request, threadlocals):
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@expand?expand=target')
    # expanding does not add to the embedded_list
    assert dummy_request._linked_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'16157204-8c8f-4672-a1a4-14f4b8021fcd'}
    }
    assert set(dummy_request._sid_cache) == dummy_request._linked_uuids


def test_linked_uuids_index_data(content, dummy_request, threadlocals):
    # this is the main view use to create data model for indexing
    # automatically sets request._indexing_view and will populate
    # _linked_uuids and _rev_linked_uuids_by_item
    # the behavior for this is diffent for object, embedded, and audit views
    res = dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@index-data', as_user='INDEXER')
    # Since the embedded view is run last, these values correspond to that view
    assert dummy_request._linked_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'16157204-8c8f-4672-a1a4-14f4b8021fcd'}
    }
    assert set(dummy_request._sid_cache) == dummy_request._linked_uuids
    # Confirm all items in the _sid_cache are up-to-date
    for rid in dummy_request._linked_uuids:
        found_sid = dummy_request.registry['storage'].write.get_by_uuid(rid).sid
        assert dummy_request._sid_cache.get(rid) == found_sid

    # embedded view linked uuids are unchanged; none are added from the audits
    assert set([l['uuid'] for l in res['linked_uuids_embedded']]) == dummy_request._linked_uuids
    assert set(res['rev_linked_to_me']) == {'775795d3-4410-4114-836b-8eeecf1d0c2f'}

    # object view linked uuids are contained within the embedded linked uuids
    assert set([l['uuid'] for l in res['linked_uuids_object']]) <= dummy_request._linked_uuids
    # no rev links are created for the object version
    assert res['rev_linked_to_me'] == []
