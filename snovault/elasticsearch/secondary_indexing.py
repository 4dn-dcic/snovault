"""PostgreSQL coalescing state for secondary invalidation fan-out.

SQS remains the work transport.  PostgreSQL arbitrates only whether a strict,
full-render secondary job for a target is already outstanding in this queue
namespace.  Primary edit events never enter this module.
"""

import datetime
import time
import uuid
from contextlib import contextmanager

import structlog
from dcicutils.misc_utils import ignored
from pyramid.httpexceptions import HTTPBadRequest
from pyramid.settings import asbool
from pyramid.view import view_config
from sqlalchemy import text as psql_text

from ..interfaces import DBSESSION
from .interfaces import INDEXER_QUEUE, SECONDARY_INDEXING_COALESCER


log = structlog.getLogger(__name__)

COALESCING_MODE_SETTING = 'indexer.coalesce_secondary'
COALESCING_MODES = frozenset({'off', 'shadow', 'on'})
COALESCING_STALE_SECONDS_SETTING = 'indexer.coalesce_secondary.stale_seconds'
COALESCING_SWEEP_INTERVAL_SETTING = 'indexer.coalesce_secondary.sweep_interval'
COALESCING_SWEEP_LIMIT_SETTING = 'indexer.coalesce_secondary.sweep_limit'

DEFAULT_STALE_SECONDS = 1800
DEFAULT_SWEEP_INTERVAL = 300
DEFAULT_SWEEP_LIMIT = 500
MAX_OPERATION_ROWS = 1000
TARGET_BATCH_SIZE = 500


def includeme(config):
    config.add_route('reset_secondary_coalescing', '/reset_secondary_coalescing')
    config.add_route('secondary_coalescing_status', '/secondary_coalescing_status')
    config.registry[SECONDARY_INDEXING_COALESCER] = SecondaryIndexingCoalescer(config.registry)
    config.scan(__name__)


def coalescing_mode(settings):
    """Return a safe rollout mode; an invalid value fails open to normal SQS sends."""
    mode = str(settings.get(COALESCING_MODE_SETTING, 'off')).strip().lower()
    if mode not in COALESCING_MODES:
        log.error(
            'Invalid secondary coalescing mode; treating it as off',
            configured_mode=mode,
            allowed_modes=sorted(COALESCING_MODES),
        )
        return 'off'
    return mode


class PostgresSecondaryIndexingStore:
    """Short READ COMMITTED transactions on connections separate from index snapshots."""

    INSERT_MISSING = psql_text("""
        INSERT INTO secondary_indexing_pending
            (rid, namespace, pending, queued_sid, queued_at)
        SELECT target.rid, :namespace, TRUE, :queued_sid, CURRENT_TIMESTAMP
          FROM unnest(CAST(:rids AS uuid[])) AS target(rid)
          JOIN resources ON resources.rid = target.rid
         ORDER BY target.rid
        ON CONFLICT (rid, namespace) DO NOTHING
        RETURNING rid
    """)
    LOCK_TARGETS = psql_text("""
        SELECT rid, pending
          FROM secondary_indexing_pending
         WHERE namespace = :namespace
           AND rid = ANY(CAST(:rids AS uuid[]))
         ORDER BY rid
         FOR UPDATE
    """)
    ARM_TARGETS = psql_text("""
        UPDATE secondary_indexing_pending AS work
           SET pending = TRUE,
               queued_sid = CASE
                   WHEN work.pending THEN GREATEST(work.queued_sid, :queued_sid)
                   ELSE :queued_sid
               END,
               queued_at = CASE
                   WHEN work.pending THEN work.queued_at
                   ELSE CURRENT_TIMESTAMP
               END
         WHERE work.namespace = :namespace
           AND work.rid = ANY(CAST(:rids AS uuid[]))
           AND (NOT work.pending OR work.queued_sid < :queued_sid)
    """)
    CLAIM_TARGET = psql_text("""
        SELECT pending, queued_sid
          FROM secondary_indexing_pending
         WHERE rid = CAST(:rid AS uuid) AND namespace = :namespace
         FOR UPDATE
    """)
    RELEASE_TARGET = psql_text("""
        UPDATE secondary_indexing_pending
           SET pending = FALSE
         WHERE rid = CAST(:rid AS uuid) AND namespace = :namespace
    """)
    REARM_STALE = psql_text("""
        WITH candidates AS (
            SELECT rid, namespace
              FROM secondary_indexing_pending
             WHERE namespace = :namespace
               AND pending
               AND queued_at < CURRENT_TIMESTAMP - make_interval(secs => :stale_seconds)
             ORDER BY queued_at, rid
             LIMIT :row_limit
             FOR UPDATE SKIP LOCKED
        )
        UPDATE secondary_indexing_pending AS work
           SET queued_at = CURRENT_TIMESTAMP
          FROM candidates
         WHERE work.rid = candidates.rid
           AND work.namespace = candidates.namespace
        RETURNING work.rid, work.queued_sid, work.queued_at
    """)
    STATUS = psql_text("""
        SELECT COUNT(*) AS table_rows,
               COUNT(*) FILTER (WHERE pending) AS pending_count,
               EXTRACT(EPOCH FROM (
                   CURRENT_TIMESTAMP - MIN(queued_at) FILTER (WHERE pending)
               )) AS oldest_pending_age_seconds
          FROM secondary_indexing_pending
         WHERE namespace = :namespace
    """)
    INSPECT_ALL_NAMESPACES = psql_text("""
        SELECT namespace, pending, queued_sid, queued_at
          FROM secondary_indexing_pending
         WHERE rid = CAST(:rid AS uuid)
         ORDER BY namespace
    """)
    INSPECT_NAMESPACE = psql_text("""
        SELECT namespace, pending, queued_sid, queued_at
          FROM secondary_indexing_pending
         WHERE rid = CAST(:rid AS uuid) AND namespace = :namespace
    """)

    def __init__(self, registry, batch_size=TARGET_BATCH_SIZE):
        self.registry = registry
        self.batch_size = batch_size

    def _engine(self):
        # MPIndexer replaces registry[DBSESSION] inside each worker, so resolve it
        # lazily rather than retaining the parent process's session factory.
        bind = self.registry[DBSESSION]().get_bind()
        return getattr(bind, 'engine', bind)

    @contextmanager
    def _connection(self):
        connection = self._engine().connect()
        try:
            # Index renders intentionally use REPEATABLE READ READ ONLY.  State
            # transitions need a separate, short write transaction whose locking
            # decisions can observe a waiter that committed while it was blocked.
            yield connection.execution_options(isolation_level='READ COMMITTED')
        finally:
            connection.close()

    @contextmanager
    def _transaction(self):
        with self._connection() as connection:
            transaction = connection.begin()
            try:
                yield connection
            except Exception:
                transaction.rollback()
                raise
            else:
                transaction.commit()

    def _chunks(self, values):
        for start in range(0, len(values), self.batch_size):
            yield values[start:start + self.batch_size]

    def prepare_targets(self, target_uuids, namespace, queued_sid):
        """Arm targets under unconditional locks and return already-covered UUIDs.

        Missing rows are inserted pending before all rows are selected FOR UPDATE.
        Consequently two producers racing on a previously absent or released target
        serialize before either decides whether to suppress its SQS send.  Each batch
        commits before this method returns and therefore before the caller contacts SQS.
        """
        targets = sorted({str(uuid.UUID(str(target))) for target in target_uuids})
        queued_sid = int(queued_sid or 0)
        suppressed = set()
        for batch in self._chunks(targets):
            params = {'rids': batch, 'namespace': namespace, 'queued_sid': queued_sid}
            with self._transaction() as connection:
                inserted = {
                    str(row.rid)
                    for row in connection.execute(self.INSERT_MISSING, params)
                }
                locked = connection.execute(self.LOCK_TARGETS, params).mappings().all()
                suppressed.update(
                    str(row['rid']) for row in locked
                    if row['pending'] and str(row['rid']) not in inserted
                )
                connection.execute(self.ARM_TARGETS, params)
        # A target deleted before the state transaction cannot be tracked, but sending
        # it preserves the pre-feature behavior and lets normal missing-item handling run.
        return {
            'targets': targets,
            'suppressed': suppressed,
            'send': set(targets) - suppressed,
        }

    def claim(self, rid, namespace, message_sid, max_sid):
        rid = str(uuid.UUID(str(rid)))
        message_sid = int(message_sid or 0)
        max_sid = int(max_sid)
        with self._connection() as connection:
            transaction = connection.begin()
            try:
                row = connection.execute(
                    self.CLAIM_TARGET,
                    {'rid': rid, 'namespace': namespace},
                ).mappings().first()
                if row is None:
                    transaction.commit()
                    return {'outcome': 'noop_row_absent', 'effective_sid': message_sid}
                if not row['pending']:
                    transaction.commit()
                    return {'outcome': 'noop_not_pending', 'effective_sid': message_sid}
                effective_sid = max(message_sid, int(row['queued_sid']))
                if effective_sid > max_sid:
                    transaction.rollback()
                    return {'outcome': 'deferred_stale', 'effective_sid': effective_sid}
                connection.execute(
                    self.RELEASE_TARGET,
                    {'rid': rid, 'namespace': namespace},
                )
                transaction.commit()
                return {'outcome': 'claimed', 'effective_sid': effective_sid}
            except Exception:
                transaction.rollback()
                raise

    def rearm_stale(self, namespace, stale_seconds, row_limit):
        with self._transaction() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    self.REARM_STALE,
                    {
                        'namespace': namespace,
                        'stale_seconds': max(int(stale_seconds), 0),
                        'row_limit': min(max(int(row_limit), 1), MAX_OPERATION_ROWS),
                    },
                ).mappings()
            ]

    def status(self, namespace):
        with self._connection() as connection:
            row = connection.execute(self.STATUS, {'namespace': namespace}).mappings().one()
        return {
            'namespace': namespace,
            'table_rows': int(row['table_rows']),
            'pending_count': int(row['pending_count']),
            'oldest_pending_age_seconds': (
                float(row['oldest_pending_age_seconds'])
                if row['oldest_pending_age_seconds'] is not None else None
            ),
        }

    def inspect(self, rid, namespace=None):
        params = {'rid': str(uuid.UUID(str(rid)))}
        statement = self.INSPECT_ALL_NAMESPACES
        if namespace is not None:
            params['namespace'] = namespace
            statement = self.INSPECT_NAMESPACE
        with self._connection() as connection:
            rows = connection.execute(statement, params).mappings().all()
        return [dict(row) for row in rows]

    def reset(self, namespace, target_uuids=None, all_targets=False, dry_run=True,
              requeue=False, row_limit=MAX_OPERATION_ROWS):
        row_limit = min(max(int(row_limit), 1), MAX_OPERATION_ROWS)
        params = {'namespace': namespace, 'row_limit': row_limit}
        target_filter = ''
        if not all_targets:
            params['rids'] = sorted({str(uuid.UUID(str(rid))) for rid in target_uuids})
            target_filter = 'AND rid = ANY(CAST(:rids AS uuid[]))'
        select_sql = """
            SELECT rid, queued_sid, queued_at
              FROM secondary_indexing_pending
             WHERE namespace = :namespace AND pending {target_filter}
             ORDER BY queued_at, rid
             LIMIT :row_limit
        """.format(target_filter=target_filter)
        with self._transaction() as connection:
            rows = connection.execute(psql_text(select_sql + ' FOR UPDATE SKIP LOCKED'), params).mappings().all()
            result = [dict(row) for row in rows]
            if dry_run or not result:
                return result
            rids = [str(row['rid']) for row in rows]
            if requeue:
                connection.execute(psql_text("""
                    UPDATE secondary_indexing_pending
                       SET queued_at = CURRENT_TIMESTAMP
                     WHERE namespace = :namespace
                       AND rid = ANY(CAST(:rids AS uuid[]))
                       AND pending
                """), {'namespace': namespace, 'rids': rids})
            else:
                connection.execute(psql_text("""
                    UPDATE secondary_indexing_pending
                       SET pending = FALSE
                     WHERE namespace = :namespace
                       AND rid = ANY(CAST(:rids AS uuid[]))
                       AND pending
                """), {'namespace': namespace, 'rids': rids})
            return result


class SecondaryIndexingCoalescer:
    """Rollout gating, SQS transport, metrics, and operational orchestration."""

    def __init__(self, registry, store=None):
        self.registry = registry
        self.queue = registry[INDEXER_QUEUE]
        self.store = store or PostgresSecondaryIndexingStore(registry)

    @property
    def mode(self):
        return coalescing_mode(self.registry.settings)

    @property
    def namespace(self):
        return self.queue.env_name

    @property
    def enabled(self):
        return self.mode in {'shadow', 'on'}

    def enqueue(self, target_uuids, sid=None, telemetry_id=None):
        started = time.monotonic()
        targets = list(target_uuids)
        if not self.enabled:
            # The caller normally takes the original path directly in off mode.  Keep
            # this fallback behavior-compatible for direct users of the service too.
            return self.queue.add_uuids(
                self.registry, targets, strict=True,
                target_queue='secondary', sid=sid, telemetry_id=telemetry_id,
            )
        state_ready = True
        try:
            prepared = self.store.prepare_targets(targets, self.namespace, sid)
        except Exception as error:
            # Failure-open preserves current indexing behavior.  Earlier committed
            # batches, if any, remain pending and are repaired by the sweeper.
            log.exception(
                'Secondary coalescing state enqueue failed; sending all targets',
                coalescing_event='enqueue_db_failure',
                namespace=self.namespace,
                target_count=len(targets),
                error=repr(error),
            )
            state_ready = False
            prepared = {'targets': targets, 'suppressed': set(), 'send': set(targets)}
        send_targets = prepared['targets'] if self.mode == 'shadow' else sorted(prepared['send'])
        try:
            queued, failed = self.queue.add_uuids(
                self.registry,
                send_targets,
                strict=True,
                target_queue='secondary',
                sid=sid,
                telemetry_id=telemetry_id,
                coalesced=state_ready,
                origin='fanout' if state_ready else None,
            )
        except Exception as error:
            if not state_ready:
                # With neither durable state nor a successful SQS send there is no
                # recovery record. Propagate so the causing primary message remains
                # undeleted and can retry after its visibility timeout.
                log.exception(
                    'Secondary state and SQS send both unavailable; retaining cause message',
                    coalescing_event='unrecoverable_enqueue_failure',
                    namespace=self.namespace,
                    target_count=len(send_targets),
                    error=repr(error),
                )
                raise
            queued = []
            failed = [{'uuid': target, 'error': repr(error)} for target in send_targets]
            log.exception(
                'Secondary coalescing SQS send failed; sweeper will repair',
                coalescing_event='send_failure',
                namespace=self.namespace,
                target_count=len(send_targets),
                error=repr(error),
            )
        if failed and not state_ready:
            log.error(
                'Secondary state unavailable and SQS reported failed sends; retaining cause message',
                coalescing_event='unrecoverable_enqueue_failure',
                namespace=self.namespace,
                target_count=len(send_targets),
                send_failures=len(failed),
            )
            raise RuntimeError(
                'Secondary state was unavailable and %s SQS sends failed; '
                'the causing message must retry.' % len(failed))
        log.info(
            'Secondary fan-out coalescing decision',
            coalescing_event='fanout',
            mode=self.mode,
            namespace=self.namespace,
            targets=len(prepared['targets']),
            suppressed=len(prepared['suppressed']),
            send_attempted=len(send_targets),
            sent=max(0, len(send_targets) - len(failed)),
            send_failures=len(failed),
            state_ready=state_ready,
            telemetry_id=telemetry_id,
            duration_ms=round((time.monotonic() - started) * 1000, 3),
        )
        return queued, failed

    def claim(self, rid, message_sid, max_sid, origin=None, receive_count=None):
        started = time.monotonic()
        result = self.store.claim(rid, self.namespace, message_sid, max_sid)
        log.info(
            'Secondary coalescing consumer claim',
            coalescing_event='claim',
            claim=result['outcome'],
            item_uuid=str(rid),
            namespace=self.namespace,
            origin=origin,
            approximate_receive_count=receive_count,
            effective_sid=result['effective_sid'],
            duration_ms=round((time.monotonic() - started) * 1000, 3),
        )
        return result

    @staticmethod
    def _messages(rows, origin):
        timestamp = datetime.datetime.utcnow().isoformat()
        return [
            {
                'uuid': str(row['rid']),
                'sid': int(row['queued_sid']),
                'strict': True,
                'timestamp': timestamp,
                'coalesced': True,
                'origin': origin,
            }
            for row in rows
        ]

    def sweep(self):
        if not self.enabled:
            return {'rearmed': 0, 'sent': 0, 'failed': 0}
        started = time.monotonic()
        stale_seconds = max(0, int(self.registry.settings.get(
            COALESCING_STALE_SECONDS_SETTING, DEFAULT_STALE_SECONDS)))
        row_limit = min(MAX_OPERATION_ROWS, max(1, int(self.registry.settings.get(
            COALESCING_SWEEP_LIMIT_SETTING, DEFAULT_SWEEP_LIMIT))))
        rows = self.store.rearm_stale(self.namespace, stale_seconds, row_limit)
        messages = self._messages(rows, 'sweeper')
        try:
            failed = self.queue.send_messages(messages, target_queue='secondary') if messages else []
        except Exception as error:
            failed = [{'error': repr(error)} for _ in messages]
            log.exception(
                'Secondary coalescing sweeper SQS send failed',
                coalescing_event='sweeper_send_failure',
                namespace=self.namespace,
                rearmed=len(rows),
                error=repr(error),
            )
        result = {'rearmed': len(rows), 'sent': len(messages) - len(failed), 'failed': len(failed)}
        log.info(
            'Secondary coalescing sweep complete',
            coalescing_event='sweeper',
            namespace=self.namespace,
            stale_seconds=stale_seconds,
            duration_ms=round((time.monotonic() - started) * 1000, 3),
            **result,
        )
        return result

    def status(self):
        status = self.store.status(self.namespace)
        status['mode'] = self.mode
        return status

    def inspect(self, rid, all_namespaces=True):
        return self.store.inspect(rid, None if all_namespaces else self.namespace)

    def reset(self, target_uuids=None, all_targets=False, dry_run=True,
              requeue=False, row_limit=MAX_OPERATION_ROWS):
        rows = self.store.reset(
            self.namespace,
            target_uuids=target_uuids,
            all_targets=all_targets,
            dry_run=dry_run,
            requeue=requeue,
            row_limit=row_limit,
        )
        failed = []
        if requeue and not dry_run and rows:
            # reset(requeue=True) commits queued_at before this send, so a failed
            # operator resend remains pending and is eligible for a later sweep.
            try:
                failed = self.queue.send_messages(
                    self._messages(rows, 'admin_requeue'), target_queue='secondary')
            except Exception as error:
                failed = [{'error': repr(error)} for _ in rows]
                log.exception(
                    'Secondary coalescing admin requeue failed; sweeper will repair',
                    coalescing_event='admin_send_failure',
                    namespace=self.namespace,
                    error=repr(error),
                )
        return {
            'matched': len(rows),
            'released': len(rows) if not dry_run and not requeue else 0,
            'requeued': len(rows) - len(failed) if not dry_run and requeue else 0,
            'send_failures': len(failed),
            'dry_run': dry_run,
            'namespace': self.namespace,
            'row_limit': min(int(row_limit), MAX_OPERATION_ROWS),
        }


@view_config(
    route_name='reset_secondary_coalescing',
    request_method='POST',
    permission='index',
)
def reset_secondary_coalescing(context, request):
    """Release or safely requeue a bounded set of pending rows for this environment."""
    ignored(context)
    body = request.json
    all_targets = body.get('all') is True
    target_uuids = body.get('uuids')
    if all_targets == bool(target_uuids):
        raise HTTPBadRequest('Provide exactly one of a non-empty uuids list or all=true.')
    if target_uuids is not None and not isinstance(target_uuids, list):
        raise HTTPBadRequest('uuids must be a list.')
    try:
        if target_uuids:
            target_uuids = [str(uuid.UUID(str(rid))) for rid in target_uuids]
        row_limit = int(body.get('limit', MAX_OPERATION_ROWS))
    except (TypeError, ValueError, AttributeError):
        raise HTTPBadRequest('Every uuid and limit must be valid.')
    if row_limit < 1 or row_limit > MAX_OPERATION_ROWS:
        raise HTTPBadRequest('limit must be between 1 and %s.' % MAX_OPERATION_ROWS)
    try:
        dry_run = asbool(body.get('dry_run', True))
        requeue = asbool(body.get('requeue', False))
    except ValueError:
        raise HTTPBadRequest('dry_run and requeue must be booleans.')
    coalescer = request.registry[SECONDARY_INDEXING_COALESCER]
    result = coalescer.reset(
        target_uuids=target_uuids,
        all_targets=all_targets,
        dry_run=dry_run,
        requeue=requeue,
        row_limit=row_limit,
    )
    log.warning(
        'Secondary coalescing administrative action',
        coalescing_event='admin_reset',
        authenticated_userid=request.authenticated_userid,
        requested_all=all_targets,
        requested_uuids=len(target_uuids or []),
        requeue=requeue,
        **result,
    )
    return result


@view_config(
    route_name='secondary_coalescing_status',
    request_method='GET',
    permission='index',
)
def secondary_coalescing_status(context, request):
    """Inspect aggregate or per-target state without changing rollout mode."""
    ignored(context)
    coalescer = request.registry[SECONDARY_INDEXING_COALESCER]
    target_uuid = request.params.get('uuid')
    if target_uuid:
        try:
            target_uuid = str(uuid.UUID(str(target_uuid)))
        except (TypeError, ValueError, AttributeError):
            raise HTTPBadRequest('uuid must be valid.')
        try:
            states = coalescer.inspect(target_uuid)
        except Exception as error:
            log.exception('Unable to inspect secondary coalescing target state')
            return {
                'mode': coalescer.mode,
                'target_uuid': target_uuid,
                'status': 'Failure',
                'detail': repr(error),
            }
        return {
            'mode': coalescer.mode,
            'target_uuid': target_uuid,
            'states': states,
            'status': 'Success',
        }
    try:
        response = coalescer.status()
    except Exception as error:
        log.exception('Unable to inspect secondary coalescing aggregate state')
        return {
            'mode': coalescer.mode,
            'namespace': coalescer.namespace,
            'status': 'Failure',
            'detail': repr(error),
        }
    response['status'] = 'Success'
    return response
