"""
Coverage for the two-mode authentication contract described in
`snovault.authentication.SESSION_TOKEN_MODE_NOTES`.

Everything here runs against a dict-backed fake implementing the small slice of the `RedisBase`
surface that `dcicutils.redis_tools.RedisSessionToken` actually uses, and against synthetic,
locally-signed JWTs. No live Redis, no cloud credentials, no outbound Auth0 calls.
"""
import json
import jwt
import pytest
import requests

from pyramid.httpexceptions import HTTPForbidden, HTTPUnauthorized
from pyramid.registry import Registry
from pyramid.request import Request
from redis.exceptions import ConnectionError as RedisConnectionError
from unittest import mock

from .. import authentication as auth_module
from ..authentication import (
    Auth0AuthenticationPolicy,
    DEFAULT_SESSION_NAMESPACE,
    LoginDenied,
    RedisSessionUnavailable,
    SESSION_COOKIE_NAME,
    callback,
    create_session_token,
    create_unauthorized_user,
    get_auth_token,
    login,
    logout,
    redis_is_active,
    resolve_session_token,
    session_identity,
    session_namespace,
)
from ..interfaces import COLLECTIONS
from ..redis.interfaces import REDIS


pytestmark = [pytest.mark.unit]


AUTH0_CLIENT = 'dummy-client'
AUTH0_SECRET = 'dummy-secret'
AUTH0_DOMAIN = 'dummy.auth0.com'
AUTH0_OPTIONS = {'auth': {'params': {'scope': 'openid email'}}}
USER_EMAIL = 'somebody@example.com'


def make_jwt(email=USER_EMAIL, *, audience=AUTH0_CLIENT, secret=AUTH0_SECRET, email_verified=True):
    """ A locally-signed synthetic id_token; never leaves this process. """
    return jwt.encode({'email': email, 'email_verified': email_verified, 'aud': audience},
                      secret, algorithm='HS256')


class FakeRedis:
    """ Dict-backed stand-in for dcicutils.redis_utils.RedisBase.

        Only implements what RedisSessionToken calls: get/set/delete/ttl. `fail_with` makes every
        operation raise, which is how the "Redis is down" cases are driven.
    """

    def __init__(self, fail_with=None):
        self.store = {}
        self.fail_with = fail_with

    def _check(self):
        if self.fail_with is not None:
            raise self.fail_with

    def set(self, key, value, exp=None):
        self._check()
        self.store[key] = value
        return 'OK'

    def get(self, key):
        self._check()
        return self.store.get(key)

    def delete(self, key):
        self._check()
        return 1 if self.store.pop(key, None) is not None else 0

    def ttl(self, key):
        self._check()
        return 3600 if key in self.store else -2

    # test helper - simulates the key's TTL lapsing
    def expire_now(self, key):
        self.store.pop(key, None)


def make_registry(*, redis=True, redis_handler=None, **extra_settings):
    registry = Registry('testing')
    registry.settings = {
        'auth0.client': AUTH0_CLIENT,
        'auth0.secret': AUTH0_SECRET,
        'auth0.domain': AUTH0_DOMAIN,
        'g.recaptcha.secret': 'dummy-recaptcha-secret',
    }
    registry.settings.update(extra_settings)
    if redis:
        registry.settings['redis.server'] = 'redis://localhost:6379'
        # NOTE: redis_handler may deliberately be None - that is exactly the state
        # snovault/redis/redis_connection.py::includeme leaves behind when it cannot connect.
        registry[REDIS] = redis_handler
    return registry


def make_request(registry, *, cookie=None, auth_header=None, json_body=None, path='/'):
    request = Request.blank(path)
    request.registry = registry
    if cookie is not None:
        request.cookies[SESSION_COOKIE_NAME] = cookie
    if auth_header is not None:
        request.headers['Authorization'] = f'Bearer {auth_header}'
    if json_body is not None:
        request.method = 'POST'
        request.content_type = 'application/json'
        request.body = json.dumps(json_body).encode('utf-8')
    return request


def response_cookie(response, name=SESSION_COOKIE_NAME):
    """ Returns the value the response sets for `name`, or None if it sets no such cookie. """
    for header_name, header_value in response.headerlist:
        if header_name.lower() == 'set-cookie' and header_value.startswith(f'{name}='):
            return header_value.split('=', 1)[1].split(';', 1)[0]
    return None


# ---------------------------------------------------------------------------------------------
# Mode selection
# ---------------------------------------------------------------------------------------------

def test_redis_is_active_follows_redis_server_setting():
    assert redis_is_active(make_request(make_registry(redis=True, redis_handler=FakeRedis())))
    assert not redis_is_active(make_request(make_registry(redis=False)))


def test_session_namespace_prefers_env_name_then_indexer_namespace():
    assert session_namespace(make_registry(**{'env.name': 'some-env',
                                              'indexer.namespace': 'ns'})) == 'some-env'
    assert session_namespace(make_registry(**{'indexer.namespace': 'ns'})) == 'ns'
    # Test settings deliberately set neither (a truthy env.name breaks app construction - see
    # CLAUDE.md), so the constant fallback has to keep login and per-request auth in agreement.
    assert session_namespace(make_registry()) == DEFAULT_SESSION_NAMESPACE


# ---------------------------------------------------------------------------------------------
# No-Redis mode: existing stateless JWT behavior must be untouched
# ---------------------------------------------------------------------------------------------

def test_no_redis_login_stores_raw_jwt_cookie():
    id_token = make_jwt()
    request = make_request(make_registry(redis=False), json_body={'id_token': id_token})

    assert login(None, request) == {'saved_cookie': True}
    assert response_cookie(request.response) == id_token


def test_no_redis_request_authentication_decodes_jwt():
    id_token = make_jwt()
    request = make_request(make_registry(redis=False), cookie=id_token)

    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) == USER_EMAIL
    assert request.auth0_expired is False


def test_no_redis_expired_jwt_is_rejected():
    """ Baseline stateless-mode failure mode: a JWT we cannot verify authenticates nobody. """
    request = make_request(make_registry(redis=False), cookie=make_jwt(secret='wrong-secret'))

    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) is None
    assert request.auth0_expired is True


def test_no_redis_logout_clears_cookie_without_touching_redis():
    request = make_request(make_registry(redis=False), cookie=make_jwt())

    assert logout(None, request) == {'deleted_cookie': True}
    assert request.response.status_code == 401
    assert response_cookie(request.response) == ''


def test_no_redis_callback_is_refused():
    """ /callback is Redis-only; without Redis the stateless flow stays the only one. """
    with pytest.raises(HTTPForbidden):
        callback(None, make_request(make_registry(redis=False)))


def test_no_redis_registration_uses_auth0_authenticated_attribute():
    """ Unchanged legacy behavior: the email comes off request._auth0_authenticated. """
    request = _registration_request(make_registry(redis=False))
    request._auth0_authenticated = USER_EMAIL

    with _registration_harness() as recorded:
        create_unauthorized_user(None, request)
    assert recorded['user_props']['email'] == USER_EMAIL


# ---------------------------------------------------------------------------------------------
# Redis mode: login mints an opaque session token
# ---------------------------------------------------------------------------------------------

def test_redis_login_yields_opaque_session_token():
    redis = FakeRedis()
    id_token = make_jwt()
    request = make_request(make_registry(redis_handler=redis), json_body={'id_token': id_token})

    assert login(None, request) == {'saved_cookie': True}

    cookie_value = response_cookie(request.response)
    assert cookie_value
    # The cookie is NOT the JWT, and is not any substring/transform of it.
    assert cookie_value != id_token
    assert '.' not in cookie_value  # a JWT always has two dots; a urlsafe token has none
    # The JWT itself is held server-side, keyed by the opaque token.
    assert redis.store[f'{DEFAULT_SESSION_NAMESPACE}:session:{cookie_value}'].startswith(id_token)


def test_redis_login_rejects_unverifiable_jwt_and_writes_nothing():
    redis = FakeRedis()
    request = make_request(make_registry(redis_handler=redis),
                           json_body={'id_token': make_jwt(secret='wrong-secret')})

    with pytest.raises(LoginDenied):
        login(None, request)
    assert redis.store == {}


def test_redis_login_requires_a_token():
    redis = FakeRedis()
    request = make_request(make_registry(redis_handler=redis), json_body={})

    with pytest.raises(LoginDenied):
        login(None, request)
    assert redis.store == {}


def test_redis_relogin_revokes_the_previous_session():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)

    first = make_request(registry, json_body={'id_token': make_jwt()})
    login(None, first)
    first_token = response_cookie(first.response)

    second = make_request(registry, cookie=first_token, json_body={'id_token': make_jwt()})
    login(None, second)
    second_token = response_cookie(second.response)

    assert second_token != first_token
    assert len(redis.store) == 1
    # The old token no longer authenticates anybody.
    stale = make_request(registry, cookie=first_token)
    assert Auth0AuthenticationPolicy().unauthenticated_userid(stale) is None


# ---------------------------------------------------------------------------------------------
# Redis mode: per-request authentication resolves the token
# ---------------------------------------------------------------------------------------------

def test_redis_authenticated_request_resolves_identity():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)

    request = make_request(registry, cookie=session.get_session_token())
    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) == USER_EMAIL
    assert request.auth0_expired is False
    assert request._auth0_session_jwt == session.get_jwt()


def test_redis_authenticated_request_accepts_bearer_session_token():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)

    request = make_request(registry, auth_header=session.get_session_token())
    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) == USER_EMAIL


def test_redis_identity_falls_back_to_jwt_when_no_email_recorded():
    """ from_redis yields '' (not None) for a record stored without an email - the fallback has to
        be truthiness-based, and must decode the *server-held* JWT, never a caller-supplied one.
    """
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=None)

    request = make_request(registry, cookie=session.get_session_token())
    resolved = resolve_session_token(request, session.get_session_token())
    assert resolved.get_email() == ''
    assert session_identity(request, resolved) == USER_EMAIL


def test_redis_mode_never_authenticates_a_raw_jwt_cookie():
    """ The no-bypass rule: a perfectly valid JWT presented directly is just an unknown Redis key.

        Also asserts the JWT decode path is not even reached, which is what makes this a token
        lookup rather than a JWT check that happens to fail.
    """
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    id_token = make_jwt()

    for request in (make_request(registry, cookie=id_token),
                    make_request(registry, auth_header=id_token)):
        with mock.patch.object(Auth0AuthenticationPolicy, 'get_token_info') as mocked:
            assert Auth0AuthenticationPolicy().unauthenticated_userid(request) is None
            assert mocked.call_count == 0
        assert request.auth0_expired is True
    assert redis.store == {}


def test_redis_anonymous_request_does_not_touch_redis():
    """ No credential at all is anonymous, not an error - even while Redis is down. """
    redis = FakeRedis(fail_with=RedisConnectionError('redis is down'))
    request = make_request(make_registry(redis_handler=redis))

    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) is None
    assert not hasattr(request, 'auth0_expired')


@pytest.mark.parametrize('bad_token', [
    'not-a-real-token',
    'AAAA.BBBB.CCCC',
    '',
])
def test_redis_unknown_or_malformed_token_is_authentication_failure(bad_token):
    redis = FakeRedis()
    request = make_request(make_registry(redis_handler=redis), cookie=bad_token)

    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) is None


def test_redis_expired_session_is_rejected():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)
    token = session.get_session_token()

    assert Auth0AuthenticationPolicy().unauthenticated_userid(
        make_request(registry, cookie=token)) == USER_EMAIL

    redis.expire_now(session.get_redis_key())  # TTL lapses

    request = make_request(registry, cookie=token)
    assert Auth0AuthenticationPolicy().unauthenticated_userid(request) is None
    # marks the request so renderers.py unsets the stale cookie
    assert request.auth0_expired is True


# ---------------------------------------------------------------------------------------------
# Redis mode: logout revokes server-side
# ---------------------------------------------------------------------------------------------

def test_redis_logout_revokes_the_session_server_side():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)
    token = session.get_session_token()

    request = make_request(registry, cookie=token)
    assert logout(None, request) == {'deleted_cookie': True}
    assert response_cookie(request.response) == ''
    assert redis.store == {}

    # The revoked token is dead even though the client may still be presenting it.
    assert Auth0AuthenticationPolicy().unauthenticated_userid(
        make_request(registry, cookie=token)) is None


def test_redis_logout_revokes_token_supplied_by_header():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)

    logout(None, make_request(registry, auth_header=session.get_session_token()))
    assert redis.store == {}


# ---------------------------------------------------------------------------------------------
# Redis mode: outages are operational failures, never a downgrade
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize('handler', [
    None,                                                # never connected (includeme stored None)
    FakeRedis(fail_with=RedisConnectionError('down')),   # connection lost at call time
])
def test_redis_outage_on_authentication_is_operational_error(handler):
    request = make_request(make_registry(redis_handler=handler), cookie='some-session-token')

    with pytest.raises(RedisSessionUnavailable) as exc:
        Auth0AuthenticationPolicy().unauthenticated_userid(request)
    assert 500 <= exc.value.code < 600
    assert exc.value.code == 503


def test_redis_outage_on_authentication_does_not_fall_back_to_jwt():
    """ The token here is a *valid* JWT: if any fallback existed, this would authenticate. """
    request = make_request(make_registry(redis_handler=None), cookie=make_jwt())

    with mock.patch.object(Auth0AuthenticationPolicy, 'get_token_info') as mocked:
        with pytest.raises(RedisSessionUnavailable):
            Auth0AuthenticationPolicy().unauthenticated_userid(request)
        assert mocked.call_count == 0


def test_redis_outage_on_login_is_operational_error():
    request = make_request(make_registry(redis_handler=None), json_body={'id_token': make_jwt()})

    with pytest.raises(RedisSessionUnavailable) as exc:
        login(None, request)
    assert exc.value.code == 503
    # and no credential cookie was handed out
    assert response_cookie(request.response) is None


def test_redis_outage_on_logout_is_operational_error():
    request = make_request(make_registry(redis_handler=None), cookie='some-session-token')

    with pytest.raises(RedisSessionUnavailable) as exc:
        logout(None, request)
    assert exc.value.code == 503


def test_redis_outage_is_distinct_from_authentication_failure():
    """ The two rejection kinds must not collapse into each other. """
    healthy = make_request(make_registry(redis_handler=FakeRedis()), cookie='unknown-token')
    assert Auth0AuthenticationPolicy().unauthenticated_userid(healthy) is None  # 401-shaped

    broken = make_request(make_registry(redis_handler=None), cookie='unknown-token')
    with pytest.raises(RedisSessionUnavailable):
        Auth0AuthenticationPolicy().unauthenticated_userid(broken)  # 503-shaped


def test_session_errors_do_not_leak_the_token(caplog):
    token = 'super-secret-session-token'
    redis = FakeRedis(fail_with=RedisConnectionError('down'))
    request = make_request(make_registry(redis_handler=redis), cookie=token)

    with pytest.raises(RedisSessionUnavailable) as exc:
        Auth0AuthenticationPolicy().unauthenticated_userid(request)

    assert token not in str(exc.value)
    assert token not in (exc.value.detail or '')
    assert token not in caplog.text


# ---------------------------------------------------------------------------------------------
# Redis mode: registration
# ---------------------------------------------------------------------------------------------

class _FakeUserCollection:
    class type_info:
        schema = {'type': 'object', 'properties': {}}


def _registration_harness():
    """ Stubs out everything create_unauthorized_user needs downstream of authentication:
        the User collection, JSON-schema validation, the reCAPTCHA round trip and the POST.
        Records the props that made it through so tests can assert on them.
    """
    recorded = {}

    def fake_validate_request(schema, request, props):
        # This is the point at which the (already whitelisted) props are handed to validation,
        # so it is the faithful place to observe what would actually be written.
        recorded['user_props'] = dict(props)
        request.validated = props

    def fake_collection_add(collection, request, render):
        return {'status': 'success'}

    class _Harness:
        def __enter__(self):
            self.patches = [
                mock.patch.object(auth_module, 'validate_request', fake_validate_request),
                mock.patch.object(auth_module, 'sno_collection_add', fake_collection_add),
                mock.patch.object(requests, 'get',
                                  lambda *a, **kw: mock.Mock(json=lambda: {'success': True})),
            ]
            for p in self.patches:
                p.start()
            return recorded

        def __exit__(self, *exc):
            for p in self.patches:
                p.stop()
            return False

    return _Harness()


def _registration_request(registry, *, cookie=None, email=USER_EMAIL):
    request = make_request(registry, cookie=cookie,
                           json_body={'g-recaptcha-response': 'dummy', 'email': email,
                                      'first_name': 'Some', 'last_name': 'Body'})
    registry[COLLECTIONS] = {'User': _FakeUserCollection()}
    request.errors = []
    return request


def test_redis_registration_resolves_email_from_session():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)

    request = _registration_request(registry, cookie=session.get_session_token())
    with _registration_harness() as recorded:
        result = create_unauthorized_user(None, request)

    assert result == {'status': 'success'}
    assert recorded['user_props']['email'] == USER_EMAIL
    assert recorded['user_props']['was_unauthorized'] is True


def test_redis_registration_without_session_is_login_denied():
    request = _registration_request(make_registry(redis_handler=FakeRedis()),
                                    cookie='unknown-token')
    with _registration_harness():
        with pytest.raises(LoginDenied) as exc:
            create_unauthorized_user(None, request)
    assert exc.value.code == 401


def test_redis_registration_with_raw_jwt_cookie_is_login_denied():
    """ No bypass on the registration path either. """
    request = _registration_request(make_registry(redis_handler=FakeRedis()), cookie=make_jwt())
    with _registration_harness():
        with pytest.raises(LoginDenied):
            create_unauthorized_user(None, request)


def test_redis_registration_during_outage_is_operational_error():
    request = _registration_request(make_registry(redis_handler=None), cookie='some-token')
    with _registration_harness():
        with pytest.raises(RedisSessionUnavailable) as exc:
            create_unauthorized_user(None, request)
    assert exc.value.code == 503


def test_redis_registration_rejects_email_mismatch():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis)
    session = create_session_token(make_request(registry), jwt_token=make_jwt(), email=USER_EMAIL)

    request = _registration_request(registry, cookie=session.get_session_token(),
                                    email='someone.else@example.com')
    with _registration_harness():
        with pytest.raises(HTTPUnauthorized):
            create_unauthorized_user(None, request)


# ---------------------------------------------------------------------------------------------
# Redis mode: /callback
# ---------------------------------------------------------------------------------------------

def _auth0_callback_response(id_token):
    return mock.Mock(json=lambda: {'id_token': id_token, 'access_token': 'dummy-access-token'})


def test_redis_callback_mints_session_token_for_known_user():
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis, **{'auth0.options': AUTH0_OPTIONS})
    id_token = make_jwt()
    request = make_request(registry, path='/callback?code=some-auth0-code')

    with mock.patch.object(requests, 'post', lambda *a, **kw: _auth0_callback_response(id_token)):
        with mock.patch.object(Auth0AuthenticationPolicy, 'get_user_info',
                               lambda *a, **kw: {'details': {'email': USER_EMAIL}}):
            result = callback(None, request)

    assert result['@type'] == ['callback']
    token = response_cookie(request.response)
    assert token and token != id_token
    assert redis.store[f'{DEFAULT_SESSION_NAMESPACE}:session:{token}'].startswith(id_token)


def test_redis_callback_mints_session_token_for_unregistered_user():
    """ Unknown users still get a session so they can complete registration through Redis. """
    redis = FakeRedis()
    registry = make_registry(redis_handler=redis, **{'auth0.options': AUTH0_OPTIONS})
    id_token = make_jwt()
    request = make_request(registry, path='/callback?code=some-auth0-code')

    def unknown_user(*args, **kwargs):
        raise HTTPUnauthorized()

    with mock.patch.object(requests, 'post', lambda *a, **kw: _auth0_callback_response(id_token)):
        with mock.patch.object(Auth0AuthenticationPolicy, 'get_user_info', unknown_user):
            result = callback(None, request)

    assert result['@type'] == ['registration']
    assert result['@graph'] == [USER_EMAIL]
    token = response_cookie(request.response)
    assert token
    # ...and that session is immediately usable for the registration POST.
    assert session_identity(make_request(registry),
                            resolve_session_token(make_request(registry), token)) == USER_EMAIL


def test_redis_callback_requires_a_code():
    registry = make_registry(redis_handler=FakeRedis(), **{'auth0.options': AUTH0_OPTIONS})
    with pytest.raises(HTTPForbidden):
        callback(None, make_request(registry, path='/callback'))


def test_redis_callback_outage_is_operational_error():
    registry = make_registry(redis_handler=None, **{'auth0.options': AUTH0_OPTIONS})
    id_token = make_jwt()
    request = make_request(registry, path='/callback?code=some-auth0-code')

    with mock.patch.object(requests, 'post', lambda *a, **kw: _auth0_callback_response(id_token)):
        with pytest.raises(RedisSessionUnavailable) as exc:
            callback(None, request)
    assert exc.value.code == 503


# ---------------------------------------------------------------------------------------------
# Token extraction
# ---------------------------------------------------------------------------------------------

def test_get_auth_token_prefers_header_then_cookie():
    registry = make_registry(redis_handler=FakeRedis())
    assert get_auth_token(make_request(registry, cookie='c', auth_header='h')) == 'h'
    assert get_auth_token(make_request(registry, cookie='c')) == 'c'
    assert get_auth_token(make_request(registry)) is None
