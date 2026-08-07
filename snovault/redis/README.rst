************
REDIS README
************

This file contains notes on the Redis implementation.

Redis is optional. It is enabled purely by the presence of a ``redis.server`` setting in the
application's PasteDeploy configuration; that same setting is what makes
``snovault/__init__.py::main`` do ``config.include('snovault.redis')``.

Two authentication modes
========================

``snovault/authentication.py`` supports exactly two mutually exclusive authentication modes. Which
one is active is decided by configuration alone -- never by inspecting a token value -- via
``redis_is_active(request)``. The canonical statement of the contract lives next to the code, in
``snovault.authentication.SESSION_TOKEN_MODE_NOTES``.

1. **Stateless JWT mode** (no ``redis.server``). The ``jwtToken`` cookie / ``Authorization: Bearer``
   value *is* the Auth0 JWT, and it is verified on every request. This is the historical behavior
   and is unchanged.

2. **Redis session mode** (``redis.server`` configured). ``POST /login`` (SPA flow) and
   ``GET /callback`` (Auth0/RAS redirect flow) verify the provider's JWT once, store it in Redis
   under ``<namespace>:session:<token>`` with a TTL, and hand the client only an opaque session
   token (``secrets.token_urlsafe``). The JWT never leaves the server afterwards. Every subsequent
   request must present the session token; the server resolves it back to the recorded identity
   before authorization.

Rules that hold in Redis session mode
-------------------------------------

* The caller-supplied credential is **never** decoded as a JWT. A raw JWT presented as a cookie or
  bearer token is simply an unknown Redis key and fails authentication -- there is no fallback to
  mode 1 once Redis mode is selected.
* An absent, unknown, expired (TTL lapsed), revoked or malformed session token is an
  **authentication failure** (401), and marks ``request.auth0_expired`` so ``renderers.py`` unsets
  the stale cookie.
* A Redis outage -- whether the connection failed at startup (``redis_connection.py::includeme``
  logs and stores ``None``) or fails mid-request -- is an **operational failure**:
  ``authentication.RedisSessionUnavailable`` (HTTP 503). It is never downgraded to either of the
  above.
* Anonymous requests (no credential at all) do not touch Redis, so an outage cannot take down
  unauthenticated traffic.
* ``POST /logout`` revokes the session server-side; re-login revokes the caller's previous session
  before minting the new one.

All Redis behavior lives in dcicutils
-------------------------------------

Snovault does **not** implement Redis connection, session or token behavior. Connection setup,
token generation, key layout, TTL, storage, lookup, rotation and revocation are all
``dcicutils.redis_utils`` / ``dcicutils.redis_tools``. Snovault contributes only what is
genuinely its own: reading pyramid settings, and translating outcomes into HTTP.

===================================  ==================================================
Snovault integration point           Canonical dcicutils call it delegates to
===================================  ==================================================
``redis_connection.py::includeme``   ``RedisBase(create_redis_client(url=...))``
``authentication.create_session_token``  ``RedisSessionToken(...)`` + ``store_session_token``
``authentication.resolve_session_token`` ``RedisSessionToken.from_redis``
``authentication.rotate_session_token``  ``RedisSessionToken.update_session_token``
``authentication.revoke_session_token``  ``RedisSessionToken.delete_session_token``
``authentication.decode_session_jwt``    ``RedisSessionToken.decode_jwt``
``authentication.session_identity``      ``RedisSessionToken.get_email`` / ``get_jwt``
===================================  ==================================================

In particular, snovault never derives a Redis key itself: revocation and rotation resolve the real
record through ``from_redis`` first, so ``<namespace>:session:<token>`` exists in exactly one place
(dcicutils) and cannot drift.

Known upstream gap: error normalization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``dcicutils.redis_tools`` normalizes driver errors to its own ``RedisException`` in exactly one of
the operations snovault uses -- ``store_session_token``, which wraps its body in
``except Exception -> raise RedisException``. ``from_redis``, ``delete_session_token`` and
``update_session_token`` (and every ``RedisBase`` primitive beneath them) let raw
``redis.exceptions.*`` through untouched.

Because ``from_redis`` is on the per-request authentication path, snovault's
``REDIS_OPERATIONAL_ERRORS`` must currently include ``redis.exceptions.RedisError`` alongside
``RedisException`` -- otherwise every request during a Redis outage would surface as an untyped 500
instead of the contracted 503. If dcicutils normalizes those three functions, that entry should be
dropped and snovault will need no knowledge of the driver at all.

Key namespace
-------------

``authentication.session_namespace(registry)`` resolves ``env.name``, else ``indexer.namespace``,
else the ``DEFAULT_SESSION_NAMESPACE`` constant. Every touchpoint must use it -- a namespace
mismatch between the writer and the reader silently 401s every session.

Note that test settings deliberately set neither ``env.name`` nor ``indexer.namespace``; a truthy
``env.name`` makes ``snovault.elasticsearch``'s ``includeme`` attempt a blue/green mirror lookup
that raises without an ``IDENTITY``. See the top-level ``AGENTS.md``.

Testing
-------

``snovault/tests/test_redis_session_auth.py`` covers both modes against a dict-backed fake
implementing the small slice of ``RedisBase`` that ``dcicutils.redis_tools.RedisSessionToken``
uses, plus locally-signed synthetic JWTs. No live Redis, no cloud credentials and no outbound
Auth0 calls are required.

Other Redis usage
=================

``RedisModel`` and ``RedisConnection`` in ``redis_connection.py`` sketch a future Redis-backed
item cache. They are **not used at this time**.
