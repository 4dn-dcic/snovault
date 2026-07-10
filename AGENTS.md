# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.

## Automatic tag-and-publish-on-master release workflow

`.github/workflows/main.yml`'s `publish` job (`needs: build`, runs only on
`push` to `master`) reads the version from `pyproject.toml` (`poetry version -s`) and checks
both for its git tag and for an existing PyPI release. It creates the tag if missing and
publishes only if PyPI does not already have the version, all **in the same job run**. It deliberately does not rely on
`.github/workflows/main-publish.yml`'s tag-triggered `on: push: tags` event, because GitHub
Actions does not start a new workflow run from a tag pushed using the default
`GITHUB_TOKEN` (anti-recursion rule) — `main-publish.yml` remains only for manual/
`workflow_dispatch` publishing.

The "Create and push tag" step is gated on the tag-existence check (`exists == 'false'`) —
tag once per version. The "Publish to PyPI" step instead uses the independently checked
`pypi_exists == 'false'` condition. Its curl request treats only HTTP 200 (present) and 404
(absent) as valid; any other status fails the job closed. This split preserves recovery from
a tag-exists-but-never-published state, while avoiding a failing duplicate-upload attempt on
later merges. `publish-to-pypi` itself treats an existing release as an error, rather than
an idempotent success.

`make build-for-ga` must use `POETRY_VIRTUALENVS_CREATE=true poetry install`, never
`poetry config --local virtualenvs.create true`: the latter changes tracked `poetry.toml` and
makes the release checkout dirty, causing `publish-to-pypi` to abort. The workflow also sets
the variable job-wide and asserts `git diff --exit-code` directly after dependency install so
any future mutation names the affected file at the source of the failure.

The workflow-level `permissions:` block in `main.yml` stays `id-token: write` / `contents:
read` (needed by the `build` job's AWS OIDC auth); `contents: write` (needed to push the
tag) is scoped to the `publish` job only, since a job-level `permissions:` block replaces
rather than merges with the workflow-level one.

## Self-registration field whitelist (`create_unauthorized_user`)

`snovault/authentication.py::create_unauthorized_user` (`POST /create-unauthorized-user`)
lets an Auth0-authenticated-but-not-yet-registered caller self-register a new `User`. It
runs with `request.remote_user = 'EMBED'` (grants the `restricted_fields` write
permission) while validating/creating the User, so the submitted body must be filtered
before validation or a caller could self-assign privileged fields (e.g.
`"groups": ["admin"]`, `"status": "deleted"`) on their own new account.

The fix is a **whitelist** (`SELF_REGISTRATION_ALLOWED_FIELDS`), not a blocklist:
snovault is consumed by apps with different `User` schemas (e.g. fourfront's
`lab`/`submits_for`/`groups`/`viewing_groups`/`pending_lab` vs. other consumers'
equivalents), so enumerating "dangerous" field names only protects against the specific
names an author thought of and silently misses a differently-named privileged field on
another consumer's schema. A whitelist of known-safe fields fails safe everywhere
instead.

Current whitelist: `email`, `first_name`, `last_name`, `preferred_email`, `job_title`,
`institution`, `pending_lab` (plus `was_unauthorized`, which the endpoint force-sets
itself). `pending_lab` is a deliberate documented exception: although its schema
`permission` is restricted like the truly privileged fields, it is only a self-declared
request that an admin must separately review and promote to `lab` before it grants any
real access — unlike `lab`/`groups`/`submits_for`/`viewing_groups`, which grant real
access immediately if set. If a future consumer's registration form needs to submit an
additional legitimate field, add it to `SELF_REGISTRATION_ALLOWED_FIELDS` (in the shared
snovault base, not a per-app override) after confirming it doesn't itself grant
privilege.

Note: `smaht-portal` has its own independent, redundant-but-harmless override
(`smaht_create_unauthorized_user`) that already blocklist-strips
`groups`/`submission_centers`/`consortia`/`submits_for`/`status`/`uuid` for that app
specifically. It doesn't need to be touched — it's just now also protected at the
framework level, as is every other consumer (e.g. fourfront, which has no override at
all).

## Testing views that call `create_unauthorized_user` (or similar) directly

To unit-test a view function that hits the DB by calling it directly against
`dummy_request` (bypassing the normal WSGI/`testapp` request cycle):
- The test's root's ACL must actually grant whatever permission the view relies on.
  `snovault/tests/root.py::TestRoot.__acl__` had drifted from the production
  `snovault/root.py::SnovaultRoot.__acl__` and was missing
  `(Allow, 'remoteuser.EMBED', 'restricted_fields')` — added back since
  `create_unauthorized_user` depends on it.
- Set `request.context = request.root` before calling schema validation directly (schema
  `permission` checks read `request.context`, which real route dispatch would set but a
  bare `dummy_request` does not).
- Request the `transaction` fixture (from `snovault/tests/serverfixtures.py`) in the test
  signature before calling `transaction.commit()` on a direct (non-WSGI) call. Skipping
  this and calling `import transaction; transaction.commit()` ad hoc bypasses the
  `zsa_savepoints`/`external_tx` per-test savepoint machinery that every other test
  relies on for isolation, and leaves real committed rows that leak into and break later
  tests (e.g. `test_storage.py`'s row-count assertions) even though `external_tx` is
  autoused.
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

**`QueueManager.purge_queue()` deliberately does NOT wait out the post-purge propagation
window** (a version of this PR briefly did, via an unconditional `time.sleep(...)` after
issuing the purge — see git history). That was reverted after live CI evidence: it turned a
~12.5 minute INDEXING run into a ~57.5 minute one (same 79 tests, measured via
`gh-axi run view --job ... --log`'s pytest duration summary). Root cause: `queue_is_empty()`
(the caller's gate for whether to purge at all, in `create_mapping.py`) reads AWS's own
documented *approximate*/eventually-consistent `ApproximateNumberOfMessages(NotVisible)`
queue attributes, which produced enough false "not empty" positives across ~79 rapid-fire
sequential indexing tests sharing one queue that `purge_queue()` was actually getting called
on a large fraction of tests — not just the rare post-failure-recovery case the propagation
wait was meant to protect. Making every one of those calls pay a ~61s tax was a severe net
regression relative to the flakiness it was meant to fix. `receive_messages()`'s explicit
`WaitTimeSeconds` long-polling and `receive_n_messages`'s tolerance for surplus/stale
messages (both below) already directly address the specific cascading-failure risk a
slow-to-propagate purge creates, at far lower cost, so that's the layer this fix leans on
instead. If a future change wants to retry the propagation-wait idea, first fix
`queue_is_empty()`'s reliability (e.g. corroborate the approximate count with an actual
`receive_messages()` probe before deciding to purge) so the wait doesn't fire on false
positives.

**`receive_messages()` passes `WaitTimeSeconds` explicitly (resolving a longstanding TODO)
but deliberately keeps it at 2 seconds, matching the queue's own configured
`ReceiveMessageWaitTimeSeconds`**, rather than raising it toward SQS's 20s max as originally
attempted. That attempt (`WaitTimeSeconds=10`) was also reverted after live CI evidence, for
the same class of reason as the purge-wait above: `Indexer.get_messages_from_queue()`
(`snovault/elasticsearch/indexer.py`) checks all 3 `queue_targets` sequentially on every
call, and `Indexer.update_objects_queue` loops calling it until every target comes back
empty - the normal steady-state end of every `/index` request once a batch is drained.
Raising the long-poll duration multiplies that "confirm nothing's left to do" cost across 3
targets on every such request, and that cost compounds across every polling helper
(`index_n_items_for_testing`, `receive_n_messages`, etc.) used throughout the test suite.
Measured: reverting the purge-wait alone (keeping `WaitTimeSeconds=10`) still left the same
79-test INDEXING run at ~44 minutes (vs. the ~12.5 minute baseline) - reverting
`WaitTimeSeconds` back to 2 as well closed the rest of that gap. If a future change wants to
retry raising this, first make it apply only where it's actually likely to help (e.g. a
caller that already found messages this cycle and is checking for stragglers) rather than to
every steady-state "is there anything left" check.

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

## Testing `filter_invalidation_scope`'s shared-list-mutation fix: build a diffs dict with a real parent extension

`filter_invalidation_scope` (`snovault/elasticsearch/indexer_utils.py`) builds its internal
`diffs` dict fresh per call from `build_diff_metadata`, then does
`all_possible_diffs = list(diffs.get(base_field_item_type, []))` before `.extend`-ing in
parent/child-type diffs — the `list(...)` copy exists so those `.extend` calls don't mutate
the dict's own stored list permanently.

A regression test that only feeds a single-type diff (e.g. `['SomeType.field']`) does
**not** exercise this: with only one key in `diffs`, every `diffs.get(parent_or_child, [])`
lookup returns `[]`, so `.extend([])` is a no-op whether or not the list was copied — the
test passes on both the buggy and fixed code. To actually discriminate, mock
`build_diff_metadata` to return a `diffs` dict with a **populated parent-type entry** (e.g.
`{'TestingBiosampleSno': ['identifier'], 'SomeParentType': ['other_field']}` plus
`child_to_parent_type = {'TestingBiosampleSno': ['SomeParentType']}`), while still using the
real `testapp.app.registry` so `crawl_schema` can resolve the real embed path — see
`test_invalidation_scope_does_not_mutate_diffs_dict` in
`snovault/tests/test_invalidation_scope.py`. Before trusting a regression test for a
mutation/aliasing bug, temporarily revert the fix and confirm the test actually fails.

## `ItemNamespace.__getattr__` sharp edge (calculated.py)

`ItemNamespace.__getattr__` resolves unknown names via `self.registry` / `self._properties`
(both pyramid `reify` properties). If the namespace was built with `request=None` (or a
request lacking `.registry`), the reify body raises `AttributeError`, which re-enters
`__getattr__` and recurses to `RecursionError` instead of a clean `AttributeError`. Not
reachable in production (`calculate_properties` always passes a real request), but when
unit-testing the namespace directly, inject `registry`/`_properties` via the `ns` dict —
see `snovault/tests/test_calculated_registry.py`.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
