# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.

## Revision-history tracking (`track_revisions` per-type flag)

By default, every item type tracks full Postgres revision history: each update inserts a
new row into the `propsheets` table keyed by `(rid, name, sid)`, so prior versions are
preserved and the `@@revision-history` view can walk them.

A type can opt out of history tracking by setting a class attribute on its `Item` subclass:

```python
class MyType(Item):
    item_type = 'my_type'
    track_revisions = False   # default is True
```

Behavior when `track_revisions = False`:
- **Write path** (`snovault/storage.py`): `PickStorage.update` resolves the flag from the
  type registry via `model.item_type` (`_track_revisions_for`) and passes it to
  `RDBStorage.update`. After the normal write, `RDBStorage._prune_revisions` deletes the
  prior `propsheets` rows for each written `(rid, name)`, leaving **at most one row** per
  resource + sheet name. A fresh propsheet row is still created per write (so the `sid`
  advances and sid-based indexing/invalidation still detects the change) — only older rows
  are pruned. The first write (create) has no prior rows and is unaffected.
- **Revision-history view** (`snovault/crud_views.py::item_view_revision_history`): returns
  **HTTP 404** with a message like "Revision history is not tracked for item type ..."
  rather than silently returning only the current version (which would falsely imply a full
  history).

The flag is declared/documented on the base `Item` class in `snovault/resources.py`.
Tests: `snovault/tests/test_storage.py` (`test_track_revisions_*`) exercise both the
disabled and enabled paths, asserting the propsheet row count **directly against the DB**.
Test types `TestingRevisionHistoryDisabled` / `TestingRevisionHistoryEnabled` live in
`snovault/tests/testing_views.py`.

Test gotcha: `testapp` and the `session` fixture share one `DBSession`. Do all `testapp`
requests first, then raw `session` queries at the end — a `session.query(...)` interleaved
before a subsequent `testapp` request corrupts the shared transaction and makes traversal
404 (this also leaks into later tests).

## SQS test-queue namespacing and INDEXING CI flakiness

`QueueManager` (`snovault/elasticsearch/indexer_queue.py`) names its 3 SQS queues from
`registry.settings['env.name']`, falling back (only when `env.name` is falsy) to
`registry.settings['indexer.namespace']`, and only after that to a sanitized
`socket.gethostname()`. `snovault/tests/test_indexing.py`'s `app_settings` fixture already
sets `indexer.namespace = INDEXER_NAMESPACE_FOR_TESTING` (the same per-CI-run/per-python-version
identifier used for the ES index namespace), so SQS queues get namespaced per run for free
instead of colliding on the CI runner's hostname across runs/repos. Because that identifier
can contain a python version like `3.11` (periods aren't valid in SQS queue names),
`QueueManager.clean_env_namespace` sanitizes whichever of the two settings actually gets
used, not just the hostname fallback — don't bypass it when constructing queue names.

**Do not set `settings['env.name']` directly to a test-run identifier** to achieve this -
`snovault.elasticsearch`'s `includeme()` separately reads `settings['env.name']` to decide
whether to look up a blue/green mirror env
(`mirror_env = blue_green_mirror_env(env_name) if env_name else None`). In an
orchestrated-but-unconfigured environment (e.g. CI, no `IDENTITY` available),
`blue_green_mirror_env(...)` raises `ValueError: There is no default identity name available
for IDENTITY` rather than returning `None` — it's only skipped because a falsy `env_name`
short-circuits the call. Making `env.name` truthy in test settings (an earlier version of
this fix did exactly that) crashes app construction before any test runs, taking down the
whole INDEXING CI job immediately. `indexer.namespace` doesn't have this problem: nothing
else reads it to gate blue/green lookups, so it's safe to always set in test settings.

Only `test_indexing.py` sets `elasticsearch.server` in its settings, and that whole file is
tagged `pytest.mark.indexing` + `pytest.mark.es`, which only runs in the CI "INDEXING" job
(`make remote-test-indexing`) — so this queue-naming fix and its CI cleanup step only need
to touch that job, not "UNIT".

CI cleanup: `poetry run wipe-test-indexer-queues $TEST_JOB_ID` (mirrors `wipe-test-indices`,
in `.github/workflows/main.yml`'s "Cleanup (INDEXING)" step) lists/deletes SQS queues by the
same sanitized-namespace prefix, so namespaced queues don't accumulate under the 14-day
message-retention period. As of this writing the CI IAM role
(`arn:aws:iam::643366669028:role/4dn-dcic-github-actions-deployment-role`) is **not**
authorized for `sqs:ListQueues`/`sqs:DeleteQueue`, so this step currently no-ops in practice
(queues still accumulate until that policy gap is closed). The command deliberately treats
any AWS error here (including `AccessDenied`) as a soft failure — logs a warning and returns
normally — specifically so a missing IAM permission doesn't fail the whole CI job the way it
did the first time this was wired up.

`QueueManager.purge_queue()` now sleeps `PURGE_QUEUE_LOCKOUT_SECONDS +
PURGE_QUEUE_SAFETY_SECONDS` (~61s) after issuing the purge, in addition to the pre-purge
rate-limit wait, because AWS's `PurgeQueue` doesn't guarantee full propagation for up to 60s
— without this, `snovault/tests/test_indexing.py`'s autouse `setup_and_teardown` fixture
(which purges the shared queue when non-empty, e.g. after a prior test failed to clean up)
could let a still-purging queue leak stale messages into several subsequent tests. This
purge path is *not* made redundant by the per-run queue namespacing above: `QueueManager` is
created once per (session-scoped, parametrized-on-mpindexer) `app` fixture, so the same
queue is shared across all ~100+ tests in one INDEXING run — namespacing only prevents
*cross-run*/*cross-repo* collisions, not intra-run leakage between sequential tests. The
post-purge wait is a raw `time.sleep(...)`, not another `collision_manager.wait_if_needed()`
call — the latter resets the lockout timestamp to "now" (post-sleep), which would double the
wait on the very next `purge_queue()` call instead of just enforcing one ~61s window per
call (verified by direct measurement, not by running `test_queue_manager_purge_queue_wait`
locally — see below).

`receive_messages()` now passes an explicit `WaitTimeSeconds=10` (was implicitly using the
queue's 2-second `ReceiveMessageWaitTimeSeconds` default) so receive calls long-poll for
messages that are genuinely in flight but not yet visible.

Local-verification gotcha: every test in `test_indexing.py`, including "pure logic" ones
like `test_queue_manager_creation`/`test_queue_manager_purge_queue_wait` that mock out boto3
entirely, still depends on the file's autouse `setup_and_teardown(app)` fixture and so
requires a live ES/Postgres/AWS-SSO session to even collect-and-run locally (pytest autouse
fixtures apply per-file, and this one requests `app`, which pulls in `aws_auth`). New
boto3-mocked unit coverage for `QueueManager`/`receive_n_messages` logic therefore lives in
standalone files without that autouse dependency: `snovault/tests/test_indexer_queue.py`
(imports `receive_n_messages` from `test_indexing` — safe, since importing a function
doesn't trigger the other file's autouse fixture) and
`snovault/tests/test_wipe_test_indexer_queues.py`. Anything that actually needs live SQS
timing (e.g. confirming `test_queue_manager_purge_queue_wait`'s exact wait math) has to be
verified either via a standalone throwaway script using `ControlledTime` mocks (no live AWS
needed, just no pytest/autouse fixture involved) or via real CI.

## SQS/ES-polling tests in `test_indexing.py` need the flaky rerun decorator

Tests in `snovault/tests/test_indexing.py` that poll SQS and/or ES for eventual
consistency intermittently fail on CI purely from timing (this also hits `master`; the
INDEXING CI job is known-flaky). The established mitigation is
`@pytest.mark.flaky(max_runs=N, rerun_filter=delay_rerun)` (`delay_rerun` from
`snovault/tools.py` sleeps 10s between reruns). When adding a test to this file that
follows the queue-then-poll pattern, include that decorator — several timing failures
traced back to tests that used the pattern but were missing it
(`test_aggregated_items`, `test_indexer_namespacing`, `test_indexer_queue_adds_telemetry_id`).

## `ItemNamespace.__getattr__` sharp edge (calculated.py)

`ItemNamespace.__getattr__` resolves unknown names via `self.registry` / `self._properties`
(both pyramid `reify` properties). If the namespace was built with `request=None` (or a
request lacking `.registry`), the reify body raises `AttributeError`, which re-enters
`__getattr__` and recurses to `RecursionError` instead of a clean `AttributeError`. Not
reachable in production (`calculate_properties` always passes a real request), but when
unit-testing the namespace directly, inject `registry`/`_properties` via the `ns` dict —
see `snovault/tests/test_calculated_registry.py`.
