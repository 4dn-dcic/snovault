"""Local-only tests for secondary PostgreSQL/SQS coalescing.

These use deterministic in-memory state and mocked queue clients.  They make no
connections to PostgreSQL, SQS, Elasticsearch, Redis, or other live services.
"""

import datetime
import json
import threading
import time
import uuid
from types import SimpleNamespace
from unittest import mock

from sqlalchemy import create_mock_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from ..elasticsearch.es_index_listener import coalescing_sweep_interval
from ..elasticsearch.indexer import Indexer
from ..elasticsearch.indexer_queue import QueueManager, dlq_to_primary, queue_indexing
from ..elasticsearch.interfaces import (
    ELASTIC_SEARCH,
    INDEXER_QUEUE,
    INDEXER_QUEUE_MIRROR,
    INVALIDATION_SCOPE_ENABLED,
    SECONDARY_INDEXING_COALESCER,
)
from ..elasticsearch.secondary_indexing import (
    PostgresSecondaryIndexingStore,
    SecondaryIndexingCoalescer,
    coalescing_mode,
    coalescing_repair_settings,
    reset_secondary_coalescing,
    secondary_coalescing_status,
)
from ..interfaces import STORAGE
from ..invalidation import add_to_indexing_queue
from ..storage import SecondaryIndexingPending


class MemoryStore:
    """Locking stand-in for the PostgreSQL protocol, shared by fake connections."""

    def __init__(self):
        self.rows = {}
        self.locks = {}
        self.meta_lock = threading.Lock()
        self.events = []
        self.pause_first_prepare = False
        self.first_prepare_locked = threading.Event()
        self.release_first_prepare = threading.Event()
        self.prepare_calls = 0

    def _lock(self, key):
        with self.meta_lock:
            return self.locks.setdefault(key, threading.Lock())

    def prepare_targets(self, target_uuids, namespace, queued_sid):
        targets = sorted({str(uuid.UUID(str(target))) for target in target_uuids})
        keys = [(target, namespace) for target in targets]
        locks = [self._lock(key) for key in keys]
        for lock in locks:
            lock.acquire()
        try:
            with self.meta_lock:
                self.prepare_calls += 1
                call_number = self.prepare_calls
            if self.pause_first_prepare and call_number == 1:
                self.first_prepare_locked.set()
                assert self.release_first_prepare.wait(timeout=2)
            suppressed = {
                target for target, key in zip(targets, keys)
                if self.rows.get(key, {}).get('pending')
            }
            now = datetime.datetime.now(datetime.timezone.utc)
            for target, key in zip(targets, keys):
                row = self.rows.get(key)
                if row is None or not row['pending']:
                    self.rows[key] = {
                        'pending': True,
                        'queued_sid': int(queued_sid or 0),
                        'queued_at': now,
                    }
                else:
                    row['queued_sid'] = max(row['queued_sid'], int(queued_sid or 0))
            self.events.append(('commit', namespace, tuple(targets)))
            return {
                'targets': targets,
                'suppressed': suppressed,
                'send': set(targets) - suppressed,
            }
        finally:
            for lock in reversed(locks):
                lock.release()

    def claim(self, rid, namespace, message_sid, max_sid):
        rid = str(uuid.UUID(str(rid)))
        key = (rid, namespace)
        with self._lock(key):
            row = self.rows.get(key)
            message_sid = int(message_sid or 0)
            if row is None:
                return {'outcome': 'noop_row_absent', 'effective_sid': message_sid}
            if not row['pending']:
                return {'outcome': 'noop_not_pending', 'effective_sid': message_sid}
            effective_sid = max(message_sid, row['queued_sid'])
            if effective_sid > int(max_sid):
                return {'outcome': 'deferred_stale', 'effective_sid': effective_sid}
            row['pending'] = False
            return {'outcome': 'claimed', 'effective_sid': effective_sid}

    def rearm_stale(self, namespace, stale_seconds, row_limit):
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=stale_seconds)
        candidates = []
        for (rid, row_namespace), row in sorted(self.rows.items()):
            if (row_namespace == namespace and row['pending']
                    and row['queued_at'] < cutoff):
                row['queued_at'] = datetime.datetime.now(datetime.timezone.utc)
                candidates.append({
                    'rid': rid,
                    'queued_sid': row['queued_sid'],
                    'queued_at': row['queued_at'],
                })
                if len(candidates) == row_limit:
                    break
        self.events.append(('sweep_commit', namespace, len(candidates)))
        return candidates

    def status(self, namespace):
        matching = [row for (rid, ns), row in self.rows.items() if ns == namespace]
        pending = [row for row in matching if row['pending']]
        return {
            'namespace': namespace,
            'table_rows': len(matching),
            'pending_count': len(pending),
            'oldest_pending_age_seconds': 0 if pending else None,
        }

    def inspect(self, rid, namespace=None):
        rid = str(uuid.UUID(str(rid)))
        return [
            {'namespace': ns, **row}
            for (row_rid, ns), row in sorted(self.rows.items())
            if row_rid == rid and (namespace is None or namespace == ns)
        ]

    def reset(self, namespace, target_uuids=None, all_targets=False, dry_run=True,
              requeue=False, row_limit=1000):
        selected = []
        allowed = set(target_uuids or [])
        for (rid, ns), row in sorted(self.rows.items()):
            if ns != namespace or not row['pending']:
                continue
            if not all_targets and rid not in allowed:
                continue
            selected.append({'rid': rid, 'queued_sid': row['queued_sid'], 'queued_at': row['queued_at']})
            if not dry_run:
                if requeue:
                    row['queued_at'] = datetime.datetime.now(datetime.timezone.utc)
                else:
                    row['pending'] = False
            if len(selected) == row_limit:
                break
        if not dry_run:
            self.events.append(('reset_commit', namespace, requeue, len(selected)))
        return selected

    def release_all(self, namespace):
        released = 0
        for (rid, row_namespace), row in self.rows.items():
            if row_namespace == namespace and row['pending']:
                row['pending'] = False
                released += 1
        self.events.append(('release_all', namespace, released))
        return released


class FailingPrepareStore(MemoryStore):
    def prepare_targets(self, target_uuids, namespace, queued_sid):
        raise RuntimeError('injected PostgreSQL state failure')


class FailingReleaseStore(MemoryStore):
    def release_all(self, namespace):
        raise AssertionError('off-mode cleanup must not access PostgreSQL state')


class FakeQueue:
    def __init__(self, namespace='env-a'):
        self.env_name = namespace
        self.add_calls = []
        self.send_calls = []
        self.fail_next_add = False
        self.fail_next_send = False

    def add_uuids(self, registry, uuids, **kwargs):
        uuids = list(uuids)
        self.add_calls.append((uuids, kwargs))
        registry[SECONDARY_INDEXING_COALESCER].store.events.append(
            ('send', self.env_name, tuple(uuids)))
        if self.fail_next_add:
            self.fail_next_add = False
            raise RuntimeError('injected SQS add failure')
        return uuids, []

    def send_messages(self, messages, target_queue='primary'):
        messages = list(messages)
        self.send_calls.append((messages, target_queue))
        if self.fail_next_send:
            self.fail_next_send = False
            raise RuntimeError('injected SQS send failure')
        return []


class FakeRegistry(dict):
    def __init__(self, settings=None, **values):
        super().__init__(values)
        self.settings = settings or {}


def make_coalescer(mode='on', namespace='env-a', store=None):
    store = store or MemoryStore()
    queue = FakeQueue(namespace)
    registry = FakeRegistry(
        settings={'indexer.coalesce_secondary': mode},
        **{INDEXER_QUEUE: queue},
    )
    coalescer = SecondaryIndexingCoalescer(registry, store=store)
    registry[SECONDARY_INDEXING_COALESCER] = coalescer
    return coalescer, queue, store, registry


def test_table_is_narrow_namespace_keyed_and_sweeper_index_allows_hot_suppression():
    table = SecondaryIndexingPending.__table__
    assert list(table.columns.keys()) == ['rid', 'namespace', 'pending', 'queued_sid', 'queued_at']
    assert [column.name for column in table.primary_key.columns] == ['rid', 'namespace']
    assert table.columns.queued_at.nullable is False
    assert next(iter(table.foreign_keys)).ondelete == 'CASCADE'
    index = next(iter(table.indexes))
    ddl = str(CreateIndex(index).compile(dialect=postgresql.dialect()))
    assert '(namespace, queued_at)' in ddl
    assert 'WHERE pending' in ddl
    # queued_sid must stay out of every index (keys and INCLUDE lists alike) so
    # that merging a newer sid into an already-pending row stays a HOT update.
    assert 'INCLUDE' not in ddl
    assert 'queued_sid' not in ddl
    statements = []

    def capture(statement, *args, **kwargs):
        statements.append(str(statement.compile(dialect=engine.dialect)))

    engine = create_mock_engine('postgresql://', capture)
    table.create(engine)
    emitted_ddl = '\n'.join(statements)
    assert 'fillfactor = 70' in emitted_ddl
    assert 'autovacuum_vacuum_scale_factor = 0.02' in emitted_ddl


def test_postgres_protocol_uses_ordered_unconditional_locks_and_skip_locked_sweep():
    assert 'ON CONFLICT (rid, namespace) DO NOTHING' in str(PostgresSecondaryIndexingStore.INSERT_MISSING)
    assert 'ORDER BY target.rid' in str(PostgresSecondaryIndexingStore.INSERT_MISSING)
    assert 'FOR UPDATE' in str(PostgresSecondaryIndexingStore.LOCK_TARGETS)
    assert 'ORDER BY rid' in str(PostgresSecondaryIndexingStore.LOCK_TARGETS)
    assert 'FOR UPDATE SKIP LOCKED' in str(PostgresSecondaryIndexingStore.REARM_STALE)


def test_rollout_mode_defaults_and_invalid_values_fail_open():
    assert coalescing_mode({}) == 'off'
    assert coalescing_mode({'indexer.coalesce_secondary': 'shadow'}) == 'shadow'
    assert coalescing_mode({'indexer.coalesce_secondary': 'on'}) == 'on'
    assert coalescing_mode({'indexer.coalesce_secondary': 'typo'}) == 'off'


def test_on_mode_suppresses_only_after_state_commit_and_merges_sid():
    rid = str(uuid.uuid4())
    coalescer, queue, store, _ = make_coalescer()

    assert coalescer.enqueue([rid], sid=10) == ([rid], [])
    assert coalescer.enqueue([rid], sid=12) == ([], [])

    assert len(queue.add_calls) == 2
    assert queue.add_calls[0][1]['coalesced'] is True
    assert queue.add_calls[0][1]['origin'] == 'fanout'
    assert queue.add_calls[1][0] == []
    assert store.rows[(rid, 'env-a')]['queued_sid'] == 12
    assert store.events[0][0] == 'commit'
    assert store.events[1][0] == 'send'


def test_release_all_clears_pending_state_for_queue_purges():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=10)

    assert coalescer.release_all() == 1
    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_off_mode_release_all_does_not_access_optional_state():
    coalescer, _, _, _ = make_coalescer(mode='off', store=FailingReleaseStore())

    assert coalescer.release_all() == 0


def test_purge_queue_releases_matching_coalescing_namespace():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=10)
    manager = object.__new__(QueueManager)
    manager.registry = {SECONDARY_INDEXING_COALESCER: coalescer}
    manager.env_name = 'env-a'
    manager.queue_url = 'primary'
    manager.second_queue_url = 'secondary'
    manager.dlq_url = 'dlq'
    manager._wait_until_purge_queue_allowed = mock.Mock()

    class PurgeExceptions:
        PurgeQueueInProgress = type('PurgeQueueInProgress', (Exception,), {})

    manager.client = SimpleNamespace(
        purge_queue=mock.Mock(), exceptions=PurgeExceptions)

    manager.purge_queue()

    assert manager.client.purge_queue.call_count == 3
    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_clear_queue_releases_matching_coalescing_namespace():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=10)
    message = {'MessageId': 'one', 'ReceiptHandle': 'receipt'}
    manager = object.__new__(QueueManager)
    manager.registry = {SECONDARY_INDEXING_COALESCER: coalescer}
    manager.env_name = 'env-a'
    manager.queue_targets = ['primary', 'secondary', 'dlq']
    manager.receive_messages = mock.Mock(side_effect=[[message], [], [], []])
    manager.delete_messages = mock.Mock()

    manager.clear_queue()

    manager.delete_messages.assert_called_once_with([message], 'primary')
    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_delete_secondary_queue_releases_matching_coalescing_namespace():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=10)
    manager = object.__new__(QueueManager)
    manager.registry = {SECONDARY_INDEXING_COALESCER: coalescer}
    manager.env_name = 'env-a'
    manager.queue_url = 'primary'
    manager.second_queue_url = 'secondary'
    manager.dlq_url = 'dlq'
    manager.client = SimpleNamespace(delete_queue=mock.Mock())

    manager.delete_queue('secondary')

    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_invalid_sweep_interval_uses_safe_default_and_zero_disables_sweeping():
    assert coalescing_sweep_interval({
        'indexer.coalesce_secondary.sweep_interval': 'invalid'
    }) == 300
    assert coalescing_sweep_interval({
        'indexer.coalesce_secondary.sweep_interval': '-1'
    }) == 0


def test_invalid_repair_settings_use_safe_bounded_defaults():
    assert coalescing_repair_settings({
        'indexer.coalesce_secondary.stale_seconds': 'invalid',
        'indexer.coalesce_secondary.sweep_limit': 'invalid',
    }) == (1800, 500)
    assert coalescing_repair_settings({
        'indexer.coalesce_secondary.stale_seconds': '-1',
        'indexer.coalesce_secondary.sweep_limit': '0',
    }) == (0, 1)
    coalescer, _, store, registry = make_coalescer()
    registry.settings.update({
        'indexer.coalesce_secondary.stale_seconds': 'invalid',
        'indexer.coalesce_secondary.sweep_limit': 'invalid',
    })
    store.rearm_stale = mock.Mock(return_value=[])

    coalescer.sweep()

    store.rearm_stale.assert_called_once_with('env-a', 1800, 500)


def test_shadow_runs_state_machine_but_sends_would_be_suppressed_targets():
    rid = str(uuid.uuid4())
    coalescer, queue, store, _ = make_coalescer(mode='shadow')
    coalescer.enqueue([rid], sid=10)
    coalescer.enqueue([rid], sid=11)
    assert [call[0] for call in queue.add_calls] == [[rid], [rid]]
    assert store.rows[(rid, 'env-a')]['queued_sid'] == 11


def test_two_connection_race_serializes_and_sends_once():
    rid = str(uuid.uuid4())
    store = MemoryStore()
    store.pause_first_prepare = True
    coalescer, queue, _, _ = make_coalescer(store=store)
    results = []

    first = threading.Thread(target=lambda: results.append(coalescer.enqueue([rid], sid=20)))
    second = threading.Thread(target=lambda: results.append(coalescer.enqueue([rid], sid=21)))
    first.start()
    assert store.first_prepare_locked.wait(timeout=1)
    second.start()
    time.sleep(0.02)
    assert second.is_alive()  # second fake connection is waiting for the target lock
    store.release_first_prepare.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive() and not second.is_alive()
    assert sum(len(call[0]) for call in queue.add_calls) == 1
    assert store.rows[(rid, 'env-a')]['queued_sid'] == 21


def test_namespace_isolation_prevents_blue_green_starvation():
    rid = str(uuid.uuid4())
    store = MemoryStore()
    env_a, queue_a, _, _ = make_coalescer(namespace='env-a', store=store)
    env_b, queue_b, _, _ = make_coalescer(namespace='env-b', store=store)
    env_a.enqueue([rid], sid=30)
    env_b.enqueue([rid], sid=30)
    assert queue_a.add_calls[0][0] == [rid]
    assert queue_b.add_calls[0][0] == [rid]
    assert store.rows[(rid, 'env-a')]['pending'] is True
    assert store.rows[(rid, 'env-b')]['pending'] is True


def test_send_failure_remains_pending_and_sweeper_recovers_after_commit():
    rid = str(uuid.uuid4())
    coalescer, queue, store, _ = make_coalescer()
    queue.fail_next_add = True
    queued, failed = coalescer.enqueue([rid], sid=40)
    assert queued == [] and len(failed) == 1
    row = store.rows[(rid, 'env-a')]
    assert row['pending'] is True
    row['queued_at'] -= datetime.timedelta(hours=1)

    result = coalescer.sweep()
    assert result == {'rearmed': 1, 'sent': 1, 'failed': 0}
    message = queue.send_calls[-1][0][0]
    assert message['uuid'] == rid
    assert message['sid'] == 40
    assert message['coalesced'] is True
    assert message['origin'] == 'sweeper'
    assert store.events[-1][0] == 'sweep_commit'


def test_state_failure_fails_open_with_legacy_unmarked_secondary_message():
    rid = str(uuid.uuid4())
    coalescer, queue, _, _ = make_coalescer(store=FailingPrepareStore())

    queued, failed = coalescer.enqueue([rid], sid=41)

    assert queued == [rid] and failed == []
    assert queue.add_calls == [([rid], {
        'strict': True,
        'target_queue': 'secondary',
        'sid': 41,
        'telemetry_id': None,
        'coalesced': False,
        'origin': None,
    })]


def test_state_and_send_failure_propagates_so_cause_message_is_not_deleted():
    rid = str(uuid.uuid4())
    coalescer, queue, _, _ = make_coalescer(store=FailingPrepareStore())
    queue.fail_next_add = True

    try:
        coalescer.enqueue([rid], sid=42)
    except RuntimeError as error:
        assert 'injected SQS add failure' in str(error)
    else:
        raise AssertionError('combined PostgreSQL/SQS failure must propagate')


def test_stale_snapshot_defers_without_releasing_pending():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=50)
    result = coalescer.claim(rid, message_sid=45, max_sid=49)
    assert result == {'outcome': 'deferred_stale', 'effective_sid': 50}
    assert store.rows[(rid, 'env-a')]['pending'] is True


def test_claim_then_redelivery_is_a_safe_noop_claim():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=60)
    assert coalescer.claim(rid, 60, 60)['outcome'] == 'claimed'
    assert coalescer.claim(rid, 60, 60)['outcome'] == 'noop_not_pending'
    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_reset_dry_run_release_and_safe_requeue():
    rid = str(uuid.uuid4())
    coalescer, queue, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=70)
    assert coalescer.status()['pending_count'] == 1
    assert coalescer.inspect(rid)[0]['namespace'] == 'env-a'

    dry_run = coalescer.reset(target_uuids=[rid], dry_run=True)
    assert dry_run['matched'] == 1
    assert store.rows[(rid, 'env-a')]['pending'] is True

    requeued = coalescer.reset(target_uuids=[rid], dry_run=False, requeue=True)
    assert requeued['requeued'] == 1
    assert store.rows[(rid, 'env-a')]['pending'] is True
    assert queue.send_calls[-1][0][0]['origin'] == 'admin_requeue'

    released = coalescer.reset(target_uuids=[rid], dry_run=False, requeue=False)
    assert released['released'] == 1
    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_reset_view_is_bounded_authorized_surface_with_audited_principal():
    rid = str(uuid.uuid4())
    service = mock.Mock()
    service.reset.return_value = {
        'matched': 1, 'released': 1, 'requeued': 0, 'send_failures': 0,
        'dry_run': False, 'namespace': 'env-a', 'row_limit': 1,
    }
    request = SimpleNamespace(
        json={'uuids': [rid], 'dry_run': False, 'requeue': False, 'limit': 1},
        registry={SECONDARY_INDEXING_COALESCER: service},
        authenticated_userid='mailto.operator@example.org',
    )
    with mock.patch('snovault.elasticsearch.secondary_indexing.log.warning') as audit:
        response = reset_secondary_coalescing(None, request)
    assert response['released'] == 1
    service.reset.assert_called_once_with(
        target_uuids=[rid], all_targets=False, dry_run=False,
        requeue=False, row_limit=1,
    )
    assert audit.call_args.kwargs['authenticated_userid'] == 'mailto.operator@example.org'


def test_status_view_supports_per_target_and_aggregate_queries_even_when_off():
    rid = str(uuid.uuid4())
    service = mock.Mock(mode='off')
    service.inspect.return_value = [{'namespace': 'env-a', 'pending': True}]
    service.status.return_value = {'mode': 'off', 'namespace': 'env-a', 'pending_count': 1}
    registry = {SECONDARY_INDEXING_COALESCER: service}

    target_response = secondary_coalescing_status(
        None, SimpleNamespace(registry=registry, params={'uuid': rid}))
    assert target_response['states'] == [{'namespace': 'env-a', 'pending': True}]
    service.inspect.assert_called_once_with(rid)

    aggregate_response = secondary_coalescing_status(
        None, SimpleNamespace(registry=registry, params={}))
    assert aggregate_response['pending_count'] == 1


def test_queue_indexing_remains_an_explicit_flag_blind_force_bypass():
    rid = str(uuid.uuid4())
    queue = mock.Mock()
    queue.add_uuids.return_value = ([rid], [])
    request = SimpleNamespace(
        json={'uuids': [rid], 'target_queue': 'secondary', 'strict': True},
        registry={INDEXER_QUEUE: queue},
        params={},
    )
    response = queue_indexing(None, request)
    assert response['number_queued'] == 1
    queue.add_uuids.assert_called_once_with(
        request.registry, [rid], strict=True, target_queue='secondary', telemetry_id=None)


def test_dlq_replay_preserves_coalescing_marker_for_a_safe_claim():
    rid = str(uuid.uuid4())
    body = json.dumps({
        'uuid': rid, 'sid': 75, 'strict': True,
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'coalesced': True, 'origin': 'fanout',
    })
    queue = mock.Mock()
    queue.receive_messages.return_value = [{'Body': body}]
    queue.send_messages.return_value = []
    request = SimpleNamespace(registry={INDEXER_QUEUE: queue})

    response = dlq_to_primary(None, request)

    assert response == {'number_failed': 0, 'number_migrated': 1}
    queue.send_messages.assert_called_once_with([body])
    assert json.loads(queue.send_messages.call_args.args[0][0])['coalesced'] is True


def test_final_target_selection_consumes_diff_before_secondary_coalescing():
    source = str(uuid.uuid4())
    removed_by_scope = str(uuid.uuid4())
    retained = str(uuid.uuid4())
    new_rev = str(uuid.uuid4())
    coalescer = mock.Mock(enabled=True)
    coalescer.enqueue.return_value = ([retained, new_rev], [])
    indexer = object.__new__(Indexer)
    indexer.registry = SimpleNamespace(settings={INVALIDATION_SCOPE_ENABLED: True})
    indexer.secondary_coalescer = coalescer
    indexer.queue = mock.Mock()
    invalidated_with_type = {(removed_by_scope, 'TypeA'), (retained, 'TypeB')}

    def filter_scope(registry, diff, typed, secondary):
        assert diff == ['Source.field']
        secondary.discard(removed_by_scope)

    with mock.patch('snovault.elasticsearch.indexer.find_uuids_for_indexing', return_value=(
            {source, removed_by_scope, retained}, invalidated_with_type)), \
            mock.patch('snovault.elasticsearch.indexer.filter_invalidation_scope', side_effect=filter_scope):
        indexer.find_and_queue_secondary_items(
            {source}, {new_rev}, sid=80, telemetry_id='telemetry', diff=['Source.field'])

    coalescer.enqueue.assert_called_once_with(
        {retained, new_rev}, sid=80, telemetry_id='telemetry')
    assert 'diff' not in coalescer.enqueue.call_args.kwargs


def test_off_mode_uses_original_secondary_queue_call_exactly():
    source = str(uuid.uuid4())
    target = str(uuid.uuid4())
    indexer = object.__new__(Indexer)
    indexer.registry = SimpleNamespace(settings={})
    indexer.secondary_coalescer = mock.Mock(enabled=False)
    indexer.queue = mock.Mock()
    indexer.queue.add_uuids.return_value = ([target], [])
    with mock.patch('snovault.elasticsearch.indexer.find_uuids_for_indexing', return_value=(
            {source, target}, {(target, 'Type')})):
        result = indexer.find_and_queue_secondary_items({source}, set(), sid=90, telemetry_id='t')
    assert result == ([target], [])
    indexer.queue.add_uuids.assert_called_once_with(
        indexer.registry, [target], strict=True,
        target_queue='secondary', sid=90, telemetry_id='t')


def test_primary_edit_queue_payload_and_mirror_behavior_are_unchanged():
    rid = str(uuid.uuid4())
    primary = mock.Mock()
    mirror = mock.Mock()
    request = SimpleNamespace(
        params={},
        registry={INDEXER_QUEUE: primary, INDEXER_QUEUE_MIRROR: mirror, ELASTIC_SEARCH: None},
    )
    item = {'uuid': rid, 'sid': 100, 'diff': ['Type.field'], 'telemetry_id': 'trace'}
    add_to_indexing_queue(True, request, item, 'edit')

    for queue in (primary, mirror):
        queue.send_messages.assert_called_once()
        payload = queue.send_messages.call_args.args[0][0]
        assert payload == item
        assert set(payload) == {
            'uuid', 'sid', 'diff', 'telemetry_id', 'strict', 'method', 'timestamp'}
        assert payload['strict'] is False
        assert payload['method'] == 'PATCH'
        assert queue.send_messages.call_args.kwargs == {'target_queue': 'primary'}


def test_primary_render_does_not_claim_interleaved_secondary_state():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=100)
    primary_body = {
        'uuid': rid, 'sid': 101, 'strict': False,
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'method': 'PATCH', 'diff': ['Type.field'],
    }
    message = {
        'MessageId': 'primary', 'ReceiptHandle': 'receipt',
        'Body': json.dumps(primary_body), 'Attributes': {},
    }
    indexer = object.__new__(Indexer)
    indexer.secondary_coalescer = coalescer
    indexer.queue = mock.Mock(delete_batch_size=10)
    indexer.get_messages_from_queue = mock.Mock(side_effect=[([message], 'primary'), ([], None)])
    indexer.update_object = mock.Mock(return_value=None)
    indexer.find_and_queue_secondary_items = mock.Mock(return_value=([], []))
    request = SimpleNamespace(registry={
        STORAGE: SimpleNamespace(write=SimpleNamespace(get_max_sid=lambda: 101)),
    })

    errors, deferred = indexer.update_objects_queue(request, [0])

    assert errors == [] and deferred is False
    assert store.rows[(rid, 'env-a')]['pending'] is True
    indexer.find_and_queue_secondary_items.assert_called_once()


def test_queue_manager_default_payload_has_no_coalescing_fields_and_receives_attributes():
    manager = object.__new__(QueueManager)
    manager.send_messages = mock.Mock(return_value=[])
    manager.client = mock.Mock()
    manager.client.receive_message.return_value = {'Messages': []}
    manager.queue_url = 'primary-url'
    manager.receive_batch_size = 10
    manager.queue_targets = {'primary': 'primary-url'}
    rid = str(uuid.uuid4())

    manager.add_uuids({}, [rid], strict=False, target_queue='primary', sid=101)
    body = manager.send_messages.call_args.args[0][0]
    assert set(body) == {'uuid', 'sid', 'strict', 'timestamp'}
    manager.receive_messages()
    manager.client.receive_message.assert_called_once_with(
        QueueUrl='primary-url',
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
        AttributeNames=['ApproximateReceiveCount', 'SentTimestamp'],
    )


def test_redelivered_marker_is_rendered_again_but_deleted_only_after_success():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=110)
    message_body = {
        'uuid': rid, 'sid': 110, 'strict': True,
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'coalesced': True, 'origin': 'fanout',
    }
    messages = [
        {
            'MessageId': str(number), 'ReceiptHandle': 'receipt-%s' % number,
            'Body': json.dumps(message_body),
            'Attributes': {'ApproximateReceiveCount': str(number)},
        }
        for number in (1, 2)
    ]
    indexer = object.__new__(Indexer)
    indexer.secondary_coalescer = coalescer
    indexer.queue = mock.Mock(delete_batch_size=10)
    indexer.get_messages_from_queue = mock.Mock(side_effect=[(messages, 'secondary'), ([], None)])

    def render(*args, **kwargs):
        assert store.rows[(rid, 'env-a')]['pending'] is False

    indexer.update_object = mock.Mock(side_effect=render)
    request = SimpleNamespace(registry={
        STORAGE: SimpleNamespace(write=SimpleNamespace(get_max_sid=lambda: 110)),
    })
    counter = [0]

    errors, deferred = indexer.update_objects_queue(request, counter)

    assert errors == [] and deferred is False
    assert indexer.update_object.call_count == 2
    assert counter == [2]
    indexer.queue.delete_messages.assert_called_once_with(messages, target_queue='secondary')
    assert store.rows[(rid, 'env-a')]['pending'] is False


def test_failed_render_releases_no_sqs_message_and_redelivery_can_retry():
    rid = str(uuid.uuid4())
    coalescer, _, store, _ = make_coalescer()
    coalescer.enqueue([rid], sid=120)
    message = {
        'MessageId': 'one', 'ReceiptHandle': 'receipt',
        'Body': json.dumps({
            'uuid': rid, 'sid': 120, 'strict': True,
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'coalesced': True, 'origin': 'fanout',
        }),
        'Attributes': {'ApproximateReceiveCount': '1'},
    }
    indexer = object.__new__(Indexer)
    indexer.secondary_coalescer = coalescer
    indexer.queue = mock.Mock(delete_batch_size=10)
    indexer.get_messages_from_queue = mock.Mock(side_effect=[([message], 'secondary'), ([], None)])
    indexer.update_object = mock.Mock(return_value={'error_message': 'injected render failure'})
    request = SimpleNamespace(registry={
        STORAGE: SimpleNamespace(write=SimpleNamespace(get_max_sid=lambda: 120)),
    })

    errors, deferred = indexer.update_objects_queue(request, [0])

    assert errors == [{'error_message': 'injected render failure'}]
    assert deferred is False
    indexer.queue.delete_messages.assert_not_called()
    indexer.queue.replace_messages.assert_called_once_with(
        [message], target_queue='secondary', vis_timeout=180)
    # Claim-before-render released the row; the still-undelivered SQS message can
    # redeliver and safely take the no-op claim path.
    assert store.rows[(rid, 'env-a')]['pending'] is False
    assert coalescer.claim(rid, 120, 120)['outcome'] == 'noop_not_pending'


def test_secondary_enqueue_failure_retains_cause_message_at_delete_batch_boundary():
    rid = str(uuid.uuid4())
    coalescer, secondary_queue, _, _ = make_coalescer(store=FailingPrepareStore())
    secondary_queue.fail_next_add = True
    message = {
        'MessageId': 'one', 'ReceiptHandle': 'receipt',
        'Body': json.dumps({
            'uuid': rid, 'sid': 130, 'strict': False,
            'timestamp': datetime.datetime.utcnow().isoformat(),
        }),
        'Attributes': {},
    }
    indexer = object.__new__(Indexer)
    indexer.secondary_coalescer = coalescer
    indexer.queue = mock.Mock(delete_batch_size=1)
    indexer.get_messages_from_queue = mock.Mock(side_effect=[([message], 'primary')])
    indexer.update_object = mock.Mock(return_value=None)
    indexer.find_and_queue_secondary_items = mock.Mock(
        side_effect=lambda source, reverse, sid, telemetry_id, diff=None:
        coalescer.enqueue(source | reverse, sid=sid, telemetry_id=telemetry_id))
    request = SimpleNamespace(registry={
        STORAGE: SimpleNamespace(write=SimpleNamespace(get_max_sid=lambda: 130)),
    })

    try:
        indexer.update_objects_queue(request, [0])
    except RuntimeError as error:
        assert 'injected SQS add failure' in str(error)
    else:
        raise AssertionError('secondary enqueue failure must retain the cause message')

    indexer.queue.delete_messages.assert_not_called()


def test_claim_failure_falls_back_to_strict_rendering():
    rid = str(uuid.uuid4())
    message = {
        'MessageId': 'one', 'ReceiptHandle': 'receipt',
        'Body': json.dumps({
            'uuid': rid, 'sid': 140, 'strict': True,
            'timestamp': datetime.datetime.utcnow().isoformat(),
            'coalesced': True, 'origin': 'fanout',
        }),
        'Attributes': {},
    }
    coalescer = mock.Mock(enabled=True, namespace='env-a')
    coalescer.claim.side_effect = RuntimeError('state table unavailable')
    indexer = object.__new__(Indexer)
    indexer.secondary_coalescer = coalescer
    indexer.queue = mock.Mock(delete_batch_size=10)
    indexer.get_messages_from_queue = mock.Mock(side_effect=[([message], 'secondary'), ([], None)])
    indexer.update_object = mock.Mock(return_value=None)
    request = SimpleNamespace(registry={
        STORAGE: SimpleNamespace(write=SimpleNamespace(get_max_sid=lambda: 140)),
    })

    errors, deferred = indexer.update_objects_queue(request, [0])

    assert errors == [] and deferred is False
    indexer.update_object.assert_called_once_with(
        request, rid, add_to_secondary=None, sid=140, max_sid=140,
        curr_time=mock.ANY, telemetry_id=None)
    indexer.queue.delete_messages.assert_called_once_with(
        [message], target_queue='secondary')
