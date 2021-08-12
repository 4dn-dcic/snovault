import pytest

from dcicutils.qa_utils import ignored, notice_pytest_fixtures
from re import findall
from ..interfaces import TYPES
from ..util import add_default_embeds, crawl_schemas_by_embeds
from .test_views import PARAMETERIZED_NAMES


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


# Convert names to snake is needed so that
# we can parameterize on the embeds
def convert_names_to_snake():
    res = []
    for name in PARAMETERIZED_NAMES:
        caps = findall('[A-Z][^A-Z]*', name)
        snake = '_'.join(map(str.lower, caps))
        res.append(snake)
    return res


SNAKE_NAMES = convert_names_to_snake()


@pytest.fixture
def content(testapp):
    url = '/testing-link-targets-sno/'
    for item in targets:
        testapp.post_json(url, item, status=201)

    url = '/testing-link-sources-sno/'
    for item in sources:
        testapp.post_json(url, item, status=201)


@pytest.mark.parametrize('item_type', [name for name in SNAKE_NAMES if name != 'testing_server_default'])
def test_add_default_embeds(registry, item_type):
    """
    Ensure default embedding matches the schema for each object
    """
    notice_pytest_fixtures(registry)
    type_info = registry[TYPES].by_item_type[item_type]
    schema = type_info.schema
    embeds = add_default_embeds(item_type, registry[TYPES], type_info.embedded_list, schema)
    principals_allowed_included_in_default_embeds = False
    for embed in embeds:
        split_embed = embed.strip().split('.')
        if 'principals_allowed' in split_embed:
            principals_allowed_included_in_default_embeds = True
        error, _ = crawl_schemas_by_embeds(item_type, registry[TYPES], split_embed, schema['properties'])
        assert error is None

    assert principals_allowed_included_in_default_embeds


@pytest.mark.parametrize('item_type', SNAKE_NAMES)
def test_manual_embeds(registry, item_type):
    """
    Ensure manual embedding in the types files are valid
    """
    notice_pytest_fixtures(registry)
    type_info = registry[TYPES].by_item_type[item_type]
    schema = type_info.schema
    embeds = type_info.embedded_list
    for embed in embeds:
        split_embed = embed.strip().split('.')
        error, _ = crawl_schemas_by_embeds(item_type, registry[TYPES], split_embed, schema['properties'])
        assert error is None


def test_linked_uuids_unset(content, dummy_request, threadlocals):
    notice_pytest_fixtures(content, dummy_request, threadlocals)
    # without setting _indexing_view = True on the request,
    # _linked_uuids not tracked and _sid_cache not populated in resource.py
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@object')
    assert dummy_request._linked_uuids == set()
    assert dummy_request._sid_cache == {}


def test_linked_uuids_object(content, dummy_request, threadlocals):
    notice_pytest_fixtures(content, dummy_request, threadlocals)
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@object')
    assert dummy_request._linked_uuids == {('16157204-8c8f-4672-a1a4-14f4b8021fcd', 'TestingLinkSourceSno')}
    assert dummy_request._rev_linked_uuids_by_item == {}


def test_linked_uuids_embedded(content, dummy_request, threadlocals):
    notice_pytest_fixtures(content, dummy_request, threadlocals)
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@embedded')
    assert dummy_request._linked_uuids == {
        ('16157204-8c8f-4672-a1a4-14f4b8021fcd', 'TestingLinkSourceSno'),
        ('775795d3-4410-4114-836b-8eeecf1d0c2f', 'TestingLinkTargetSno')
    }
    # _rev_linked_uuids_by_item is in form {target uuid: set(source uuid)}
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'reverse': ['16157204-8c8f-4672-a1a4-14f4b8021fcd']}
    }


def test_linked_uuids_page(content, dummy_request, threadlocals):
    notice_pytest_fixtures(content, dummy_request, threadlocals)
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@page')
    assert dummy_request._linked_uuids == {
         ('16157204-8c8f-4672-a1a4-14f4b8021fcd', 'TestingLinkSourceSno'),
         ('775795d3-4410-4114-836b-8eeecf1d0c2f', 'TestingLinkTargetSno')
    }
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'reverse': ['16157204-8c8f-4672-a1a4-14f4b8021fcd']}
    }


def test_linked_uuids_expand_target(content, dummy_request, threadlocals):
    notice_pytest_fixtures(content, dummy_request, threadlocals)
    # needed to track _linked_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@expand?expand=target')
    # expanding does not add to the embedded_list
    assert dummy_request._linked_uuids == {
         ('16157204-8c8f-4672-a1a4-14f4b8021fcd', 'TestingLinkSourceSno'),
         ('775795d3-4410-4114-836b-8eeecf1d0c2f', 'TestingLinkTargetSno')
    }
    assert dummy_request._rev_linked_uuids_by_item == {
        '775795d3-4410-4114-836b-8eeecf1d0c2f': {'reverse': ['16157204-8c8f-4672-a1a4-14f4b8021fcd']}
    }


def test_linked_uuids_index_data(content, dummy_request, threadlocals):
    notice_pytest_fixtures(content, dummy_request, threadlocals)
    # this is the main view use to create data model for indexing
    # automatically sets request._indexing_view and will populate
    # a number of different attributes on the request
    res = dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@index-data', as_user='INDEXER')
    # Since the embedded view is run last, these values correspond to that view
    assert dummy_request._linked_uuids == {(sources[0]['uuid'], 'TestingLinkSourceSno'),
                                           (targets[0]['uuid'], 'TestingLinkTargetSno')}
    assert dummy_request._rev_linked_uuids_by_item == {targets[0]['uuid']: {'reverse': [sources[0]['uuid']]}}
    # Confirm all items in the _sid_cache are up-to-date
    for rid, _ in dummy_request._linked_uuids:
        found_sid = dummy_request.registry['storage'].write.get_by_uuid(rid).sid
        assert dummy_request._sid_cache.get(rid) == found_sid

    # embedded view linked uuids are unchanged
    assert res['rev_link_names'] == {}
    assert res['rev_linked_to_me'] == [targets[0]['uuid']]
    # object view linked uuids are contained within the embedded linked uuids
    assert set((linked['uuid'], linked['item_type']) for linked in res['linked_uuids_object']) <= dummy_request._linked_uuids

    # now test the target. this will reset all attributes on dummy_request
    res2 = dummy_request.embed('/testing-link-targets-sno/', targets[0]['uuid'], '@@index-data', as_user='INDEXER')
    assert dummy_request._linked_uuids == {(sources[0]['uuid'], 'TestingLinkSourceSno'),
                                           (targets[0]['uuid'], 'TestingLinkTargetSno')}
    assert dummy_request._rev_linked_uuids_by_item == {targets[0]['uuid']: {'reverse': [sources[0]['uuid']]}}
    assert res2['rev_link_names'] == {'reverse': [sources[0]['uuid']]}
    assert res2['rev_linked_to_me'] == []

    # test the next target to ensure that the _sid_cache persists between requests
    # sources[1]['uuid'] does not show up because it has status=deleted (no rev_link)
    res3 = dummy_request.embed('/testing-link-targets-sno/', targets[1]['uuid'], '@@index-data', as_user='INDEXER')
    assert {sources[0]['uuid'], targets[0]['uuid'], targets[1]['uuid']} <= set(dummy_request._sid_cache)
    ignored(res3)
