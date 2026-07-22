"""
Deterministic performance guardrails for the @@index-data render path.

These tests protect two invariants of the drain-level ``MAX(sid)`` hoist
(``request._batch_max_sid``; see ``snovault.indexing_views.item_index_data``,
``snovault.embed._embed`` and ``snovault.elasticsearch.indexer.update_objects_queue``):

1. When a drain supplies ``_batch_max_sid`` the render issues **no** per-document
   ``SELECT max(current_propsheets.sid)`` query; without it (a direct
   ``GET /<uuid>/@@index-data`` or the sync path) the render still computes
   ``context.max_sid`` exactly as before.
2. The indexed document is byte-for-byte identical with and without the hoist
   (the hoist is output-neutral under the drain's READ ONLY REPEATABLE READ
   snapshot; only the timing block ``indexing_stats`` differs between renders).

They also pin the query count of representative canonical ``@@index-data``
renders so that a newly-introduced N+1 in the render path fails loudly.

These need only PostgreSQL (no Elasticsearch), so they are intentionally left
unmarked and run in the fast UNIT (``not indexing``) partition, matching
``test_embedding.py``.

Query counting uses an engine-level ``after_cursor_execute`` recorder throughout
(the ``sql_recorder`` fixture below) -- the same mechanism the repository's
``execute_counter`` fixture and the scout profiling harness use, and thus "the
closest authoritative test helper" the ship scope allows. It is used in place of
``execute_counter`` itself because both the batch and fallback renders here are
driven via the internal ``dummy_request.embed('@@index-data', as_user='INDEXER')``
call, and ``execute_counter`` only counts inside an active zope-transaction
savepoint (``zsa_savepoints.state == 'begun'``), which that internal embed path
does not enter -- so ``execute_counter`` would silently count zero here. The
recorder counts unconditionally on the engine, so it observes both paths.
"""
import contextlib
import json
import pytest

from copy import deepcopy
from unittest.mock import Mock
from dcicutils.qa_utils import notice_pytest_fixtures
from pyramid.threadlocal import manager
from sqlalchemy import event

from ..elasticsearch.indexer import Indexer
from ..embed import _embed
from ..interfaces import DBSESSION, STORAGE


# ---------------------------------------------------------------------------
# Dedicated fixtures with a known link/rev-link shape (not shared rows), so the
# pinned query counts depend only on this graph, never on unrelated test data.
# ---------------------------------------------------------------------------
GUARDRAIL_TARGET = {
    'name': 'guardrail-rev-target',
    'uuid': 'c6f9a1e2-0000-4a00-8000-00000000ab01',
}
# All sources are status=current so every one is a live rev-link that the
# target's `reverse` calc property traverses (the rev-link N+1 shape).
GUARDRAIL_SOURCES = [
    {
        'name': f'guardrail-src-{i}',
        'target': GUARDRAIL_TARGET['uuid'],
        'uuid': f'c6f9a1e2-0000-4a00-8000-0000000000{i:02d}',
        'status': 'current',
    }
    for i in range(4)
]


@pytest.fixture
def guardrail_content(testapp):
    """POST one rev-link target with 4 current sources through the real cycle."""
    testapp.post_json('/testing-link-targets-sno/', GUARDRAIL_TARGET, status=201)
    for source in GUARDRAIL_SOURCES:
        testapp.post_json('/testing-link-sources-sno/', source, status=201)


# ---------------------------------------------------------------------------
# Engine-level SQL statement recorder (mirrors execute_counter's mechanism).
# ---------------------------------------------------------------------------
class SqlRecorder:
    """Records SQL statements executed on the engine while active."""

    def __init__(self):
        self.active = False
        self.statements = []

    @contextlib.contextmanager
    def recording(self):
        self.statements = []
        self.active = True
        try:
            yield self
        finally:
            self.active = False

    @property
    def count(self):
        return len(self.statements)

    @property
    def max_sid_count(self):
        """Number of `SELECT max(current_propsheets.sid)` aggregates recorded.

        This aggregate is emitted by `context.max_sid` (storage.RDBStorage
        .get_max_sid) and, within a single @@index-data render, by nothing else,
        so it uniquely identifies the per-document MAX(sid) query the hoist
        removes -- independent of cache warmth or unrelated fixture rows.
        """
        needle = 'max(current_propsheets.sid)'
        return sum(1 for s in self.statements if needle in ' '.join(s.split()).lower())


@pytest.fixture
def sql_recorder(app):
    engine = app.registry[DBSESSION].bind
    recorder = SqlRecorder()

    @event.listens_for(engine, 'after_cursor_execute')
    def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if recorder.active:
            recorder.statements.append(statement)

    yield recorder

    event.remove(engine, 'after_cursor_execute', _after_cursor_execute)


def _render_index_data(dummy_request, coll, uuid, batch_max_sid=None):
    """Render one item's @@index-data exactly as Indexer.update_object does.

    Setting `dummy_request._batch_max_sid` before the embed mimics the drain
    having hoisted MAX(sid) (it propagates to the @@index-data subrequest via
    embed._embed); leaving it None mimics a direct/sync render (fallback).
    """
    dummy_request._batch_max_sid = batch_max_sid
    return dummy_request.embed(coll, uuid, '@@index-data', as_user='INDEXER')


def _doc_without_timings(document):
    """Copy of the indexed document minus the non-deterministic timing block."""
    comparable = deepcopy(document)
    comparable.pop('indexing_stats', None)
    return comparable


def _clear_app_caches():
    """Drop snovault's per-transaction item/key/embed caches (threadlocal)."""
    if manager.stack:
        threadlocals = manager.stack[0]
        for name in ('snovault.connection.item_cache',
                     'snovault.connection.key_cache',
                     'snovault.connection.embed_cache'):
            threadlocals.pop(name, None)


def _cold_reset(registry):
    """Simulate a cache-cold first touch (clear app caches + SA identity map)."""
    _clear_app_caches()
    registry[DBSESSION]().expunge_all()


def test_batch_max_sid_only_propagates_to_index_data(dummy_request, monkeypatch):
    """A drain value must not leak into unrelated nested embed subrequests."""
    propagated = {}

    def invoke_subrequest(subrequest):
        propagated[subrequest.path] = subrequest.__dict__.get('_batch_max_sid')
        # Supply the bookkeeping normally initialized while Pyramid invokes the
        # subrequest; the result itself is immaterial to this propagation test.
        subrequest._linked_uuids = set()
        subrequest._rev_linked_uuids_by_item = {}
        subrequest._aggregated_items = {}
        subrequest._sid_cache = {}
        return {}

    monkeypatch.setattr(dummy_request, 'invoke_subrequest', invoke_subrequest)
    dummy_request._batch_max_sid = 42

    _embed(dummy_request, '/unrelated/@@object', as_user='INDEXER')
    _embed(dummy_request, '/target/@@index-data', as_user='INDEXER')

    assert propagated == {
        '/unrelated/@@object': None,
        '/target/@@index-data': 42,
    }


# ---------------------------------------------------------------------------
# 1. MAX(sid) hoist: batch path removes the per-document MAX(sid) query, and the
#    indexed document is unchanged. Covers both the batch and fallback paths.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize('coll,uuid_key,uuid_val', [
    ('/testing-link-sources-sno/', 'source', GUARDRAIL_SOURCES[0]['uuid']),  # 1 linkTo
    ('/testing-link-targets-sno/', 'target', GUARDRAIL_TARGET['uuid']),      # N rev-links
])
def test_index_data_batch_max_sid_hoist(guardrail_content, dummy_request, threadlocals,
                                        sql_recorder, coll, uuid_key, uuid_val):
    notice_pytest_fixtures(guardrail_content, dummy_request, threadlocals, sql_recorder)
    batch_max_sid = dummy_request.registry[STORAGE].write.get_max_sid()

    # Warm once so both measured renders share an identical cache state; the
    # MAX(sid) query is issued regardless of warmth, so this does not mask it.
    _render_index_data(dummy_request, coll, uuid_val)

    # Fallback path: no _batch_max_sid -> exactly one MAX(sid) aggregate.
    with sql_recorder.recording():
        fallback_doc = _render_index_data(dummy_request, coll, uuid_val)
    assert sql_recorder.max_sid_count == 1, uuid_key

    # Batch path: _batch_max_sid supplied -> the MAX(sid) aggregate is gone.
    with sql_recorder.recording():
        batch_doc = _render_index_data(dummy_request, coll, uuid_val,
                                       batch_max_sid=batch_max_sid)
    assert sql_recorder.max_sid_count == 0, uuid_key
    assert sql_recorder.count == 0, (
        'batch render of a warm item should issue no SQL at all; got: %s'
        % sql_recorder.statements)

    # The hoist is output-neutral: max_sid value and every other indexed byte
    # match (only the timing block differs between renders).
    assert batch_doc['max_sid'] == fallback_doc['max_sid'] == batch_max_sid
    assert _doc_without_timings(batch_doc) == _doc_without_timings(fallback_doc)

    # Zero is a valid supplied max_sid, not the fallback sentinel.
    with sql_recorder.recording():
        zero_doc = _render_index_data(dummy_request, coll, uuid_val,
                                      batch_max_sid=0)
    assert sql_recorder.max_sid_count == 0, uuid_key
    assert zero_doc['max_sid'] == 0


def test_queue_drain_reuses_one_max_sid_for_all_documents(
        guardrail_content, dummy_request, threadlocals, sql_recorder):
    """Exercise update_objects_queue -> update_object -> @@index-data itself.

    This guards the actual batch wiring, rather than merely simulating it by
    setting ``_batch_max_sid`` immediately before a synthetic render.
    """
    notice_pytest_fixtures(guardrail_content, dummy_request, threadlocals,
                           sql_recorder)
    registry = dummy_request.registry
    uuids = [GUARDRAIL_SOURCES[0]['uuid'], GUARDRAIL_TARGET['uuid']]
    messages = []
    for uuid in uuids:
        model = registry[STORAGE].write.get_by_uuid(uuid)
        messages.append({
            'Body': json.dumps({
                'uuid': uuid,
                'sid': model.sid,
                'timestamp': '2026-01-01T00:00:00',
                'strict': True,
            }),
        })
    expected_max_sid = registry[STORAGE].write.get_max_sid()

    indexer = Indexer.__new__(Indexer)
    indexer.registry = registry
    indexer.es = Mock()
    indexer.queue = Mock(delete_batch_size=10)
    indexer.get_messages_from_queue = Mock(side_effect=[
        (messages, 'primary'),
        ([], None),
    ])
    counter = [0]

    with sql_recorder.recording():
        errors, deferred = indexer.update_objects_queue(dummy_request, counter)

    assert errors == []
    assert deferred is False
    assert counter == [len(uuids)]
    assert sql_recorder.max_sid_count == 1, sql_recorder.statements
    assert indexer.es.index.call_count == len(uuids)
    indexed_documents = [call.kwargs['body']
                         for call in indexer.es.index.call_args_list]
    assert [document['uuid'] for document in indexed_documents] == uuids
    assert all(document['max_sid'] == expected_max_sid
               for document in indexed_documents)


# ---------------------------------------------------------------------------
# 2. N+1 guardrail: pin the cold query count of a canonical rev-linked render so
#    a new per-item query in the render path is caught. Uses the fallback path
#    (context.max_sid) and dedicated fixtures with a fixed rev-link fan-in.
# ---------------------------------------------------------------------------
def test_index_data_cold_query_count_is_pinned(guardrail_content, dummy_request,
                                               threadlocals, sql_recorder):
    notice_pytest_fixtures(guardrail_content, dummy_request, threadlocals, sql_recorder)
    registry = dummy_request.registry

    # Rev-linked target with 4 current sources, rendered cold (fallback path).
    _cold_reset(registry)
    with sql_recorder.recording():
        _render_index_data(dummy_request, '/testing-link-targets-sno/',
                           GUARDRAIL_TARGET['uuid'])
    target_statements = list(sql_recorder.statements)
    target_cold = len(target_statements)

    # Simple source with a single linkTo, rendered cold (fallback path).
    _cold_reset(registry)
    with sql_recorder.recording():
        _render_index_data(dummy_request, '/testing-link-sources-sno/',
                           GUARDRAIL_SOURCES[0]['uuid'])
    source_statements = list(sql_recorder.statements)
    source_cold = len(source_statements)

    # Pinned expectations (see module docstring). Both counts include exactly one
    # MAX(sid) aggregate (fallback path). A new N+1 in the render path makes these
    # jump; the recorded statements are surfaced on failure for diagnosis. If the
    # render path legitimately changes, update these deliberately.
    #
    # Target (rev-linked, 4 current sources) = 8 cold queries:
    #   1 get_by_uuid(target 3-table join) + 1 get_by_unique_key (path traversal)
    #   + 1 get_rev_links (links-by-target) + 4 per-source status loads (the
    #   rev-link N+1, one per current source) + 1 MAX(sid). Grows by one query per
    #   additional current source -- that linear growth is exactly the N+1 shape.
    # Source (single linkTo that transitively embeds its target's rev-links)
    #   = 8 cold queries (also 1 MAX(sid)); depends on the same 4-source fan-in.
    assert target_cold == EXPECTED_TARGET_COLD_QUERIES, target_statements
    assert source_cold == EXPECTED_SOURCE_COLD_QUERIES, source_statements


# Baselined empirically (see module docstring and the breakdown above). The
# 4-source fan-in in GUARDRAIL_SOURCES is load-bearing for these counts.
EXPECTED_TARGET_COLD_QUERIES = 8
EXPECTED_SOURCE_COLD_QUERIES = 8
