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
