# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.

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
