"""Pure-logic unit tests for two esstorage.py efficiency fixes:

    1. get_rev_links restricting the ES query to no _source (uuids come from
       the hit's own _id) instead of fetching whole documents.
    2. __iter__'s scan query restricting _source to False, since the top-level
       hit envelope never carries a 'uuid' key and the entire document was
       previously fetched and discarded.

These construct an ElasticSearchStorage instance directly (bypassing __init__,
which requires a full pyramid registry/ES client) and mock only the ES
transport, matching the pattern used elsewhere in this repo for testing
query-construction logic without a live cluster.
"""
from unittest import mock

from ..elasticsearch.esstorage import ElasticSearchStorage


def make_storage():
    storage = object.__new__(ElasticSearchStorage)
    storage.es = mock.MagicMock()
    storage.index = 'test-namespace*'
    return storage


class DummyModel:
    def __init__(self, uuid):
        self.uuid = uuid


def test_get_rev_links_restricts_source_and_returns_ids():
    storage = make_storage()
    storage.es.search.return_value = {
        'took': 1, 'timed_out': False,
        '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0},
        'hits': {
            'total': {'value': 2, 'relation': 'eq'},
            'max_score': None,
            'hits': [
                {'_index': 'idx', '_type': '_doc', '_id': 'uuid-1', '_score': None},
                {'_index': 'idx', '_type': '_doc', '_id': 'uuid-2', '_score': None},
            ],
        },
    }

    result = storage.get_rev_links(DummyModel('parent-uuid'), 'some.rel')

    assert result == ['uuid-1', 'uuid-2']
    _, kwargs = storage.es.search.call_args
    assert kwargs['body']['_source'] is False


def test_iter_restricts_source_and_yields_ids():
    storage = make_storage()
    raw_hits = [
        {'_index': 'idx', '_type': '_doc', '_id': 'uuid-1'},
        {'_index': 'idx', '_type': '_doc', '_id': 'uuid-2'},
    ]

    with mock.patch('snovault.elasticsearch.esstorage.scan', return_value=iter(raw_hits)) as mock_scan:
        result = list(storage.__iter__())

    assert result == ['uuid-1', 'uuid-2']
    _, kwargs = mock_scan.call_args
    assert kwargs['query']['_source'] is False
