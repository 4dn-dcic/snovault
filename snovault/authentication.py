import base64
import datetime
import json
import jwt
import os
import requests
import structlog

from dateutil.parser import isoparse
from dcicutils.misc_utils import remove_element, ignorable, ignored
from operator import itemgetter
from passlib.context import CryptContext
from pyramid.authentication import (
    BasicAuthAuthenticationPolicy as _BasicAuthAuthenticationPolicy,
    CallbackAuthenticationPolicy
)
from pyramid.httpexceptions import HTTPForbidden, HTTPServiceUnavailable, HTTPUnauthorized
from pyramid.path import DottedNameResolver, caller_package
from pyramid.security import NO_PERMISSION_REQUIRED
from pyramid.view import view_config
from snovault import ROOT, COLLECTIONS
from snovault.calculated import calculate_properties
from snovault.crud_views import collection_add as sno_collection_add
from snovault.project_app import app_project
from snovault.schema_utils import validate_request
from snovault.util import debug_log
from snovault.validation import ValidationFailure
from snovault.validators import no_validate_item_content_post
from urllib.parse import urlencode
from snovault.redis.interfaces import REDIS
from dcicutils.redis_tools import RedisSessionToken
from dcicutils.redis_utils import RedisException
from redis.exceptions import RedisError


log = structlog.getLogger(__name__)


CRYPT_CONTEXT = __name__ + ':crypt_context'


JWT_ENCODING_ALGORITHM = 'HS256'

# Might need to keep a list of previously used algorithms here, not just the one we use now.
# Decryption algorithm used to default to a long list, but more recent versions of jwt library
# say we should stop assuming that.
#
# In case it goes away, as far as I can tell, the default for decoding from their
# default_algorithms() method used to be what we've got in JWT_ALL_ALGORITHMS here.
#  -kmp 15-May-2020

# 'none' is intentionally excluded: allowing it in the decode algorithm list means an
# attacker-supplied, unsigned token (alg=none) would be accepted as authentic, which is a
# well-known JWT authentication bypass. See CWE-347 / "JWT alg:none" attack.
JWT_ALL_ALGORITHMS = ['ES512', 'RS384', 'HS512', 'ES256',
                      'RS256', 'PS512', 'ES384', 'HS384', 'ES521',
                      'PS384', 'HS256', 'PS256', 'RS512']

# Probably we could get away with fewer, but I think not as few as just our own encoding algorithm,
# so for now I believe the above list was the default, and this just rearranges it to prefer the one
# we use for encoding. -kmp 19-Jan-2021

JWT_DECODING_ALGORITHMS = [JWT_ENCODING_ALGORITHM] + remove_element(JWT_ENCODING_ALGORITHM, JWT_ALL_ALGORITHMS)

CONTENT_TYPE = "Content-Type"
JSON_CONTENT_TYPE = "application/json"
STANDARD_HEADERS = {CONTENT_TYPE: JSON_CONTENT_TYPE}

# Name of the cookie carrying the caller's credential. Historically this always held a raw
# Auth0 JWT; when Redis session configuration is present it instead holds an opaque, server-side
# session token. The name is unchanged in both modes so downstream front-ends keep working, but
# the *interpretation* is never ambiguous: it is decided solely by `redis_is_active(request)`,
# i.e. by the presence of the `redis.server` setting - the same condition that decides whether
# `snovault.redis` is even included (see snovault/__init__.py::main). See
# `SESSION_TOKEN_MODE_NOTES` below for the full contract.
SESSION_COOKIE_NAME = 'jwtToken'

# Fallback namespace for Redis session keys when neither `env.name` nor `indexer.namespace` is
# configured. Only reachable in local/test configurations; deployed environments always set one.
DEFAULT_SESSION_NAMESPACE = 'snovault'

# Redis failures we treat as *operational* (=> 5xx). Deliberately narrow: a cache miss is not an
# error (RedisSessionToken.from_redis returns None), and a broad `except Exception` here would
# collapse the exact distinction between "Redis is down" (5xx) and "this token is not valid"
# (401) that the two-mode contract depends on.
REDIS_OPERATIONAL_ERRORS = (RedisException, RedisError)

SESSION_TOKEN_MODE_NOTES = """
Snovault supports two mutually exclusive authentication modes, selected by configuration:

1. Stateless JWT mode (no `redis.server` setting). The `jwtToken` cookie / `Authorization: Bearer`
   value IS the Auth0 JWT. It is decoded and verified on every request. This is the historical
   behavior and is unchanged.

2. Redis session mode (`redis.server` configured). The `jwtToken` cookie / `Authorization: Bearer`
   value is an opaque session token minted by `make_session_token()`. The Auth0 JWT never leaves
   the server; it is stored in Redis under `<namespace>:session:<token>` with a TTL. In this mode
   the server NEVER attempts to interpret the caller-supplied value as a JWT, and never falls back
   to mode 1 - an unknown/expired/revoked/malformed token is an authentication failure (401) and a
   Redis outage is an operational failure (503).
"""


def includeme(config):
    config.include('.edw_hash')
    setting_prefix = 'passlib.'
    passlib_settings = {
        k[len(setting_prefix):]: v
        for k, v in config.registry.settings.items()
        if k.startswith(setting_prefix)
    }
    if not passlib_settings:
        passlib_settings = {'schemes': 'edw_hash, unix_disabled'}
    crypt_context = CryptContext(**passlib_settings)
    config.registry[CRYPT_CONTEXT] = crypt_context

    # basic login route
    config.add_route('login', '/login')
    config.add_route('logout', '/logout')
    config.add_route('me', '/me')
    config.add_route('impersonate-user', '/impersonate-user')
    config.add_route('session-properties', '/session-properties')
    config.add_route('create-unauthorized-user', '/create-unauthorized-user')
    config.add_route('callback', '/callback')
    config.scan(__name__)

class RedisSessionUnavailable(HTTPServiceUnavailable):
    """ Raised when Redis session mode is configured but the session store cannot be reached.

        This is deliberately a 5xx: it is an operational failure of a required dependency, NOT an
        authentication failure, and it must never be downgraded into a stateless-JWT fallback.
    """
    title = 'Session Store Unavailable'


def redis_is_active(request):
    """ Quick helper to standardize detecting whether redis is in use.

        NOTE: this intentionally mirrors the exact condition used in snovault/__init__.py::main to
        decide whether to `config.include('snovault.redis')`, so mode selection can never disagree
        with whether the Redis machinery was configured at all.
    """
    return 'redis.server' in request.registry.settings


def session_namespace(registry):
    """ Resolves the Redis key namespace for session tokens.

        Must be resolved identically at every touchpoint (login, callback, per-request auth,
        registration, logout) - a mismatch would silently 401 every session.

        `env.name` is preferred, but is deliberately NOT required: it is absent in test settings and
        must stay absent there (a truthy `env.name` makes snovault.elasticsearch's includeme attempt
        a blue/green mirror lookup that raises without an IDENTITY - see CLAUDE.md).
    """
    settings = registry.settings
    return settings.get('env.name') or settings.get('indexer.namespace') or DEFAULT_SESSION_NAMESPACE


def get_redis_handler(request):
    """ Returns the RedisBase handle for this app, or raises RedisSessionUnavailable (503).

        snovault/redis/redis_connection.py::includeme swallows connection errors at startup and
        stores None, so a Redis that was unreachable at boot is indistinguishable from a healthy one
        by looking at settings alone - it has to be caught here.
    """
    handler = request.registry.get(REDIS)
    if handler is None:
        log.error('Redis session mode is configured but no Redis connection is available')
        raise RedisSessionUnavailable(
            detail='Session store is not available. Authentication cannot be performed.'
        )
    return handler


def resolve_session_token(request, token):
    """ Resolves an opaque session token to its stored RedisSessionToken record.

        Tri-state, and the distinction matters (each is separately asserted in the test suite):
          * returns a RedisSessionToken  -> the session is live
          * returns None                 -> absent / unknown / expired / revoked / malformed token,
                                            i.e. an authentication failure. NO JWT decode is ever
                                            attempted on the caller-supplied value, so a raw JWT
                                            presented in Redis mode simply misses and is rejected.
          * raises RedisSessionUnavailable -> Redis itself is unreachable (operational, 5xx)
    """
    if not token:
        return None
    handler = get_redis_handler(request)
    try:
        return RedisSessionToken.from_redis(
            redis_handler=handler,
            namespace=session_namespace(request.registry),
            token=token
        )
    except REDIS_OPERATIONAL_ERRORS as e:
        # Note: no token value in the log message - session tokens are credentials.
        log.error(f'Redis error resolving session token: {type(e).__name__}: {e}')
        raise RedisSessionUnavailable(
            detail='Session store is not available. Authentication cannot be performed.'
        )


def create_session_token(request, *, jwt_token, email):
    """ Mints and persists a new opaque session token wrapping the given (already validated) JWT. """
    handler = get_redis_handler(request)
    session = RedisSessionToken(
        namespace=session_namespace(request.registry),
        jwt=jwt_token,
        email=email
    )
    try:
        session.store_session_token(redis_handler=handler)
    except REDIS_OPERATIONAL_ERRORS as e:
        log.error(f'Redis error storing session token: {type(e).__name__}: {e}')
        raise RedisSessionUnavailable(detail='Session store is not available. Cannot establish session.')
    return session


def revoke_session_token(request, token):
    """ Deletes the given session token from Redis, immediately invalidating it server-side.

        Constructs the key directly rather than reading the record first - revocation should not
        depend on the record being readable, and it saves a round trip.
    """
    if not token:
        return False
    handler = get_redis_handler(request)
    session = RedisSessionToken(
        namespace=session_namespace(request.registry),
        jwt='', email='', token=token
    )
    try:
        return session.delete_session_token(redis_handler=handler)
    except REDIS_OPERATIONAL_ERRORS as e:
        log.error(f'Redis error revoking session token: {type(e).__name__}: {e}')
        raise RedisSessionUnavailable(detail='Session store is not available. Cannot revoke session.')


def decode_session_jwt(request, session):
    """ Decodes the JWT held server-side by a resolved session, handling both the Auth0 (HS256 +
        shared secret) and RAS (RS256 + public key) configurations. Returns None if it cannot be
        decoded - callers must treat that as an authentication failure, not as an outage.
    """
    settings = request.registry.settings
    auth0_domain = settings.get('auth0.domain') or ''
    if 'auth0' in auth0_domain:
        secret = settings.get('auth0.secret')
        algorithms = JWT_DECODING_ALGORITHMS
    else:  # RAS
        secret = settings.get('auth0.public.key')
        algorithms = ['RS256']
    if not secret:
        log.error('No key configured with which to decode the session JWT')
        return None
    try:
        return session.decode_jwt(
            audience=settings.get('auth0.client'),
            secret=secret,
            algorithms=algorithms
        )
    except jwt.exceptions.PyJWTError as e:
        log.error(f'Could not decode JWT held by session: {type(e).__name__}: {e}')
        return None


def session_identity(request, session):
    """ Returns the lower-cased email identifying a resolved session, or None.

        The stored email is authoritative: it was established by a validated Auth0/RAS login before
        the session was ever written, and using it (rather than re-decoding the JWT on every
        request) keeps the session's lifetime governed by exactly one clock - the Redis TTL - and
        keeps the RAS flow working, whose JWTs `Auth0AuthenticationPolicy.get_token_info` cannot
        verify. The JWT is only consulted if no email was recorded.
    """
    email = session.get_email()
    if email:  # note: from_redis yields '' (not None) when no email was stored
        return email.lower()
    jwt_info = decode_session_jwt(request, session)
    if not jwt_info:
        return None
    email = jwt_info.get('email')
    return email.lower() if email else None


def set_session_cookie(request, value, *, samesite):
    """ Sets the credential cookie. `value` is a raw JWT in stateless mode and an opaque session
        token in Redis mode; see SESSION_TOKEN_MODE_NOTES.
    """
    request.response.set_cookie(
        SESSION_COOKIE_NAME,
        value=value,
        domain=request.domain,
        path='/',
        httponly=True,
        samesite=samesite,
        overwrite=True,
        secure=(request.scheme == 'https')
    )


@view_config(route_name='callback', request_method='GET', permission=NO_PERMISSION_REQUIRED)
def callback(context, request):
    """ /callback for Fourfront that will result in a session token
        Note that this sets jwtToken as to not break the front-end
    """
    if not redis_is_active(request):
        raise HTTPForbidden('Calls to /callback are not allowed when Redis not in use - check your ini file')
    auth0_code = request.params.get('code', None)
    if not auth0_code:
        raise HTTPForbidden('No code sent back from Auth0')

    # Acquire Auth0 configuration
    registry = request.registry
    auth0_domain = registry.settings.get('auth0.domain')
    auth0_client = registry.settings.get('auth0.client')
    auth0_secret = registry.settings.get('auth0.secret')
    auth0_options = registry.settings.get('auth0.options')
    if not (auth0_domain and auth0_client and auth0_secret and auth0_options):
        raise HTTPForbidden('Auth0 not configured, no callback possible')

    # Create auth0 payload, send and get JWT back
    auth0_redirect_uri = f'{request.host_url}'
    auth0_payload = {
        'grant_type': 'authorization_code',
        'client_id': auth0_client,
        'client_secret': auth0_secret,
        'code': auth0_code,
        'redirect_uri': auth0_redirect_uri
    }
    auth0_response = None
    if 'auth0' in auth0_domain:
        auth0_post_url = f'https://{auth0_domain}/oauth/token'
        auth0_payload_json = json.dumps(auth0_payload)
        auth0_headers = STANDARD_HEADERS
        auth0_response = requests.post(auth0_post_url, data=auth0_payload_json, headers=auth0_headers)
    elif 'nih.gov' in auth0_domain:
        # RAS
        auth0_payload['scope'] = auth0_options.get('auth', {}).get('params', {}).get('scope', 'openid profile email ga4gh_passport_v1')
        auth0_payload['redirect_uri'] += '/callback'
        auth0_post_url = f'https://{auth0_domain}/auth/oauth/v2/token'
        auth0_headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'}
        auth0_response = requests.post(auth0_post_url, data=auth0_payload, headers=auth0_headers)
    else:
        raise HTTPForbidden('Unknown authentication domain, no callback possible')
   
    try:
        auth0_response_json = auth0_response.json()
    except ValueError:
        raise LoginDenied('Malformed response from the authentication provider')
    auth0_jwt = auth0_response_json.get('id_token')
    if not auth0_jwt:
        raise LoginDenied('No JWT returned from Auth0, check Auth0 configuration')

    # email
    email = ''
    if 'auth0' in auth0_domain:
        # Check that the user exists in our database, if they do not, redirect them to /registration
        token_info = Auth0AuthenticationPolicy.get_token_info(auth0_jwt, request) or {}
        email = (token_info.get('email') or '').lower()
    elif 'nih.gov' in auth0_domain:
        # In RAS authentication, user info is not included in the JWT token, but in a passport that requires an extra request.
        passport_post_url = f'https://{auth0_domain}/openid/connect/v1/userinfo'
        passport_headers = {'Authorization': f'Bearer {auth0_response_json["access_token"]}'}
        passport_response = requests.post(passport_post_url, headers=passport_headers)
        passport_response_json = passport_response.json()
        email = (passport_response_json.get('email') or '').lower()

    if not email:
        raise LoginDenied('No email extracted from JWT, not possible to continue')

    # Re-login: if the caller already holds a session token, revoke it before minting a new one so
    # a single browser never leaves live orphaned sessions behind in Redis.
    revoke_session_token(request, request.cookies.get(SESSION_COOKIE_NAME))

    # Generate a session from Redis. Note this is stored *before* the DB lookup below: the token is
    # issued unconditionally (see comment further down), and storing first keeps the "what is in
    # Redis" and "what is in the cookie" answers identical on every exit path from here.
    redis_session_token = create_session_token(request, jwt_token=auth0_jwt, email=email)

    try:
        Auth0AuthenticationPolicy.get_user_info(request, email, redis_session_token.get_session_token())
    except HTTPUnauthorized:
        # in this case return a different response that the UI can interpret to pull up the registration modal
        resp_json = {
            '@type': ['registration'],
            '@context': '/callback',
            'title': 'registration',
            '@graph': [
                email  # this is needed by the front-end to render the UserRegistrationModal
            ]
        }
    except Exception as e:
        raise LoginDenied(f'Unknown error encountered trying to extract user from DB {str(e)}')
    else:
        resp_json = {
            '@type': ['callback'],
            '@context': '/callback',
            'title': 'callback'
    }

    # The session token is handed back unconditionally so we can retrieve the JWT later on, either
    # in the registration scenario (if an unknown user) or to make auth'd requests as an existing
    # user. Note that although the cookie is named jwtToken, its value here is NOT a JWT - it is the
    # opaque session token. See SESSION_TOKEN_MODE_NOTES.
    # samesite is 'lax' rather than 'strict' because this cookie is set on the top-level navigation
    # back from the identity provider.
    set_session_cookie(request, redis_session_token.get_session_token(), samesite='lax')
    return resp_json


class NamespacedAuthenticationPolicy(object):
    """ Wrapper for authentication policy classes

    As userids are included in the list of principals, it seems good practice
    to namespace them to avoid clashes.

    Constructor Arguments

    ``namespace``

        The namespace used (string).

    ``base``

        The base authentication policy (class or dotted name).

    Remaining arguments are passed to the ``base`` constructor.

    Example

    To make a ``REMOTE_USER`` 'admin' be 'user.admin'

    .. code-block:: python

        policy = NamespacedAuthenticationPolicy('user',
            'pyramid.authentication.RemoteUserAuthenticationPolicy')
    """

    def __new__(cls, namespace, base, *args, **kw):
        # Dotted name support makes it easy to configure with pyramid_multiauth
        name_resolver = DottedNameResolver(caller_package())
        base = name_resolver.maybe_resolve(base)
        # Dynamically create a subclass
        name = 'Namespaced_%s_%s' % (namespace, base.__name__)
        klass = type(name, (cls, base), {'_namespace_prefix': namespace + '.'})
        return super(NamespacedAuthenticationPolicy, klass).__new__(klass)

    def __init__(self, namespace, base, *args, **kw):
        ignored(namespace, base)  # TODO: SHOULD this be ignored?
        super().__init__(*args, **kw)

    def unauthenticated_userid(self, request):
        return app_project().namespaced_authentication_policy_unauthenticated_userid(self, request)

    def _unauthenticated_userid_implementation(self, request):
        userid = super().unauthenticated_userid(request)
        if userid is not None:
            userid = self._namespace_prefix + userid
        return userid

    def authenticated_userid(self, request, set_user_info_property=True):
        # TODO: Maybe something like ...
        # return app_project().login_policy.authenticated_userid(request, set_user_info_property)
        return app_project().namespaced_authentication_policy_authenticated_userid(self, request, set_user_info_property)

    def _authenticated_userid_implementation(self, request, set_user_info_property=True):
        """
        Adds `request.user_info` for all authentication types.
        Fetches and returns some user details if called.
        """
        namespaced_userid = super().authenticated_userid(request)

        if not set_user_info_property:
            return namespaced_userid

        if namespaced_userid is not None:
            # userid, if present, may be in form of UUID (if remoteuser) or an email (if Auth0).
            namespace, userid = namespaced_userid.split(".", 1)

            # Allow access basic user credentials from request obj after authenticating & saving request
            def get_user_info(request):
                user_props = request.embed('/session-properties', as_user=userid)  # Performs an authentication against DB for user.
                if not user_props.get('details'):
                    raise HTTPUnauthorized(
                        title="Could not find user info for {}".format(userid),
                        headers={
                            'WWW-Authenticate':
                                "Bearer realm=\"{}\"; Basic realm=\"{}\"".format(request.domain, request.domain)
                        }
                    )
                return user_props

            # If not authenticated (not in our DB), request.user_info will throw an HTTPUnauthorized error.
            request.set_property(get_user_info, "user_info", True)

        return namespaced_userid

    def remember(self, request, principal, **kw):
        if not principal.startswith(self._namespace_prefix):
            return []
        principal = principal[len(self._namespace_prefix):]
        return super().remember(request, principal, **kw)


class BasicAuthAuthenticationPolicy(_BasicAuthAuthenticationPolicy):
    def __init__(self, check, *args, **kw):
        # Dotted name support makes it easy to configure with pyramid_multiauth
        name_resolver = DottedNameResolver(caller_package())
        check = name_resolver.maybe_resolve(check)
        super().__init__(check, *args, **kw)


class LoginDenied(HTTPUnauthorized):
    title = 'Login Failure'

    def __init__(self, domain=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.headers.get('WWW-Authenticate') and domain:
            # headers['WWW-Authenticate'] might be set in constructor thru headers
            self.headers['WWW-Authenticate'] = "Bearer realm=\"{}\"; Basic realm=\"{}\"".format(domain, domain)


_fake_user = object()


class Auth0AuthenticationPolicy(CallbackAuthenticationPolicy):

    login_path = '/login'
    method = 'POST'

    def unauthenticated_userid(self, request):
        """
        So basically this is used to do a login, instead of the actual
        login view... not sure why, but yeah..
        """

        # we will cache it for the life of this request, cause pyramids does traversal
        cached = getattr(request, '_auth0_authenticated', _fake_user)

        if cached is not _fake_user:
            return cached

        # try to find the token in the request (should be in the header)
        id_token = get_auth_token(request)
        if not id_token:
            # No credential at all: this is an anonymous request. Return before touching Redis so a
            # session-store outage cannot take down unauthenticated traffic.
            # can I thrown an 403 here?
            # print('Missing assertion.', 'unauthenticated_userid', request)
            return None

        if redis_is_active(request):
            return self._redis_unauthenticated_userid(request, id_token)

        jwt_info = self.get_token_info(id_token, request)
        if not jwt_info:
            return None

        email = request._auth0_authenticated = jwt_info['email'].lower()

        # At this point, email has been authenticated with their Auth0 provider and via `get_token_info`,
        # but we don't know yet if this email is in our database. `authenticated_userid` should take care of this.

        app_project().note_auth0_authentication_policy_unauthenticated_userid(self, request, email, id_token)

        return email

    def _redis_unauthenticated_userid(self, request, session_token):
        """ Redis session mode: resolve the caller-supplied opaque session token to the identity
            recorded server-side. The supplied value is NEVER decoded as a JWT here, so presenting a
            raw JWT in this mode is just an unknown key and fails authentication like any other.

            A Redis outage propagates out of `resolve_session_token` as a 503; it is never
            downgraded to the stateless JWT path.
        """
        session = resolve_session_token(request, session_token)
        email = session_identity(request, session) if session is not None else None
        if not email:
            # Absent / unknown / expired / revoked / malformed session token. Mark the request the
            # same way an expired JWT is marked so renderers.py unsets the stale cookie and reports
            # the expired session to the front-end.
            request.set_property(lambda r: True, 'auth0_expired')
            return None

        request.set_property(lambda r: False, 'auth0_expired')
        request._auth0_authenticated = email
        # The JWT stays server-side; stash it on the request for the (few) call sites that need the
        # underlying provider token without going back to Redis.
        request._auth0_session_jwt = session.get_jwt()

        app_project().note_auth0_authentication_policy_unauthenticated_userid(
            self, request, email, session.get_jwt()
        )

        return email

    @staticmethod
    def get_user_info(request, email, id_token):
        """
        Previously an inner method, redefined here so can be used outside, but can only be used within a route
        Allow access basic user credentials from request obj after authenticating & saving request
        """
        user_props = request.embed('/session-properties', as_user=email)  # Performs an authentication against DB for user.
        if not user_props.get('details'):
            raise HTTPUnauthorized(
                title="Could not find user info for {}".format(email),
                headers={'WWW-Authenticate': "Bearer realm=\"{}\"; Basic realm=\"{}\"".format(request.domain, request.domain) }
            )
        user_props['id_token'] = id_token
        return user_props

    @staticmethod
    def email_is_partners_or_hms(payload):
        """
        Checks that the given JWT payload belongs to a partners email.
        """
        for identity in payload.get('identities', []):  # if auth0 decoded
            if identity.get('connection', '') in ['partners', 'hms-it']:
                return True

        # XXX: Refactor to use regex? Also should potentially be data-driven?
        if 'partners' in payload.get('sub', ''):
            return True
        elif 'harvard.edu' in payload.get('sub', ''):
            return True
        elif payload.get('email_verified'):
            return True
        else:
            return False

    @staticmethod
    def get_token_info(token, request):
        """
        Given a jwt get token info from auth0, handle retrying and whatnot.
        This is only called if we receive a Bearer token in Authorization header.
        """
        try:
            # lets see if we have an auth0 token or our own
            registry = request.registry
            auth0_client = registry.settings.get('auth0.client')
            auth0_secret = registry.settings.get('auth0.secret')
            if auth0_client and auth0_secret:
                # leeway accounts for clock drift between us and auth0
                payload = jwt.decode(token, auth0_secret,
                                     algorithms=JWT_DECODING_ALGORITHMS,
                                     audience=auth0_client, leeway=30)
                if 'email' in payload and Auth0AuthenticationPolicy.email_is_partners_or_hms(payload):
                    request.set_property(lambda r: False, 'auth0_expired')
                    return payload

            else:  # we don't have the key, let auth0 do the work for us
                warn_msg = "No Auth0 keys present - falling back to making outbound network request to have Auth0 validate for us"
                log.warning(warn_msg)
                user_url = "https://{domain}/tokeninfo".format(domain='hms-dbmi.auth0.com')
                resp = requests.post(user_url, {'id_token': token})
                payload = resp.json()
                if 'email' in payload and Auth0AuthenticationPolicy.email_is_partners_or_hms(payload):
                    request.set_property(lambda r: False, 'auth0_expired')
                    return payload

        except jwt.exceptions.ExpiredSignatureError as e:
            ignorable(e)
            # Normal/expected expiration.

            # Allow us to return 403 code &or unset cookie in renderers.py
            request.set_property(lambda r: True, 'auth0_expired')

            return None

        except (ValueError, jwt.exceptions.InvalidTokenError, jwt.exceptions.InvalidKeyError) as e:
            # Catch errors from decoding JWT or unauthorized users.
            print('Invalid JWT assertion : %s (%s)' % (e, type(e).__name__))
            log.error("Error with JWT token (now unset) - " + str(e))
            request.set_property(lambda r: True, 'auth0_expired')  # Allow us to return 403 code &or unset cookie in renderers.py
            return None

        print("didn't get email or email is not verified")
        return None


def get_jwt_from_auth_header(request):
    if "Authorization" in request.headers:
        try:
            # Ensure this is a JWT token, not basic auth.
            # Per https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication and
            # https://tools.ietf.org/html/rfc6750, JWT is introduced by 'bearer', as in
            #   Authorization: Bearer something.something.something
            # rather than, for example, the 'basic' key information, which as discussed in
            # https://tools.ietf.org/html/rfc7617 is base64 encoded and looks like:
            #   Authorization: Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==
            # See also https://jwt.io/introduction/ for other info specific to JWT.
            [auth_type, auth_data] = request.headers['Authorization'].strip().split(' ', 1)
            if auth_type.lower() == 'bearer':
                return auth_data.strip()  # The spec says exactly one space, but then a token, so spaces don't matter
        except Exception:
            return None
    return None


def get_auth_token(request):
    """ Returns the caller-supplied credential, from the Authorization header if present and the
        cookie otherwise.

        In stateless mode this is a raw Auth0 JWT; in Redis session mode it is an opaque session
        token. This function does not (and must not) attempt to tell which - that is decided by
        configuration, not by inspecting the value. See SESSION_TOKEN_MODE_NOTES.
    """
    # First try to obtain the token from headers (case: some REST API requests)
    token = get_jwt_from_auth_header(request)

    # If not in the headers, get it from cookies (case: AJAX requests from portal & other clients)
    if not token:
        token = request.cookies.get(SESSION_COOKIE_NAME)

    return token


def get_jwt(request):
    """ Retained under its historical name for downstream callers. Prefer `get_auth_token`: in
        Redis session mode the value returned is an opaque session token, not a JWT.
    """
    return get_auth_token(request)


@view_config(route_name='login', request_method='POST', permission=NO_PERMISSION_REQUIRED)
@debug_log
def login_view(context, request, samesite: str = "strict"):
    return app_project().login(context, request, samesite=samesite)


def login(context, request, *, samesite: str = "strict"):
    """
    Establish a session for the caller.

    Stateless mode (no Redis configured): save the caller-supplied Auth0 JWT as an httpOnly cookie,
    exactly as before.

    Redis session mode: validate the caller-supplied Auth0 JWT, keep it server-side in Redis, and
    hand back only an opaque session token. This is the SPA login flow - the same flow the
    front-end already uses - so Redis mode is reachable without also adopting the /callback flow.
    """
    ignored(context)

    # Allow providing token thru Authorization header as well as POST request body.
    # Should be about equally secure if using HTTPS.
    request_token = get_jwt_from_auth_header(request)
    if request_token is None:
        request_token = request.json_body.get("id_token", None)

    if redis_is_active(request):
        return _redis_login(request, request_token, samesite=samesite)

    set_session_cookie(request, request_token, samesite=samesite)

    return {"saved_cookie": True}


def _redis_login(request, id_token, *, samesite: str):
    """ Redis session mode implementation of `login`.

        The JWT supplied here is the provider's token from the SPA's Auth0 handshake - this is the
        one place it is legitimately accepted from the caller, and it is verified before anything is
        written to Redis. Every *subsequent* request must present the opaque session token instead.
    """
    if not id_token:
        raise LoginDenied(domain=request.domain, title='No id_token supplied')

    jwt_info = Auth0AuthenticationPolicy.get_token_info(id_token, request)
    if not jwt_info:
        raise LoginDenied(domain=request.domain)

    email = (jwt_info.get('email') or '').lower()
    if not email:
        raise LoginDenied(domain=request.domain, title='No email present on supplied id_token')

    # Re-login: revoke whatever session the caller was previously holding rather than leaving it
    # live in Redis until its TTL lapses. Read the cookie directly - `get_auth_token` prefers the
    # Authorization header, which on this route carries the *new* id_token, not the old session.
    revoke_session_token(request, request.cookies.get(SESSION_COOKIE_NAME))

    session = create_session_token(request, jwt_token=id_token, email=email)
    set_session_cookie(request, session.get_session_token(), samesite=samesite)

    return {"saved_cookie": True}


@view_config(route_name='logout',
             permission=NO_PERMISSION_REQUIRED, http_cache=0)
@debug_log
def logout_view(context, request):
    return app_project().logout(context, request)


def logout(context, request):
    """
    This endpoint proxies a request to Auth0 for it to remove its session cookies.
    See https://auth0.com/docs/api/authentication#enterprise-saml-and-others-

    The Auth0 endpoint is meant to be navigated to by end-user as part of SSO logout (?)
    So this endpoint may not be needed at moment. Kept for reference.

    The front-end handles logging out by discarding the locally-held JWT from
    browser cookies and re-requesting the current 4DN URL.

    In Redis session mode the server-side session is revoked first, so the token is dead even if
    the client keeps presenting it. If Redis is unreachable the revocation cannot be honored, and
    that surfaces as a 503 rather than a logout that silently did nothing.
    """
    ignored(context)

    if redis_is_active(request):
        revoke_session_token(request, get_auth_token(request))

    # Deletes the cookie
    request.response.set_cookie(
        name=SESSION_COOKIE_NAME,
        value=None,
        domain=request.domain,
        max_age=0,
        path='/',
        overwrite=True
    )

    request.response.status_code = 401
    request.response.headers['WWW-Authenticate'] = (
        "Bearer realm=\"{}\", title=\"Session Expired\"; Basic realm=\"{}\""
        .format(request.domain, request.domain)
    )

    return {"deleted_cookie": True}

    # TODO: NEED DO THIS CLIENTSIDE SO IT UNSETS USER'S COOKIE - MUST BE THRU REDIRECT NOT AJAX
    # (we don't do this - i.e. we don't bother to log user out of all of Auth0 session, just out of
    # own web app)

    # call auth0 to logout -
    # auth0_logout_url = "https://{domain}/v2/logout" \
    #             .format(domain='hms-dbmi.auth0.com')

    # requests.get(auth0_logout_url)

    # if asbool(request.params.get('redirect', True)):
    #     raise HTTPFound(location=request.resource_path(request.root))

    # return {}


@view_config(route_name='me', request_method='GET', permission=NO_PERMISSION_REQUIRED)
@debug_log
def me(context, request):
    """Alias /users/<uuid-of-current-user>"""
    ignored(context)
    for principal in request.effective_principals:
        if principal.startswith('userid.'):
            break
    else:
        raise HTTPForbidden(title="Not logged in.")

    namespace, userid = principal.split('.', 1)

    # return { "uuid" : userid } # Uncomment and delete below code to just grab UUID.

    request.response.status_code = 307  # Prevent from creating 301 redirects that get cached permanently by browser
    properties = request.embed('/users/' + userid, as_user=userid)
    return properties


def get_basic_properties_for_user(request, userid):
    user = request.registry[COLLECTIONS]['user'][userid]
    user_dict = user.__json__(request)

    # Only include certain/applicable fields from profile
    include_detail_fields = ['email', 'first_name', 'last_name', 'groups', 'timezone', 'status', 'project_roles']
    user_actions = calculate_properties(user, request, category='user_action')

    properties = {
        # 'user': request.embed(request.resource_path(user)),
        'details': {p: v for p, v in user_dict.items() if p in include_detail_fields},
        'user_actions': [v for k, v in sorted(user_actions.items(), key=itemgetter(0))]
    }

    # add uuid to user details
    properties['details']['uuid'] = userid

    return properties


@view_config(route_name='session-properties', request_method='GET',
             permission=NO_PERMISSION_REQUIRED)
@debug_log
def session_properties(context, request):
    ignored(context)
    for principal in request.effective_principals:
        if principal.startswith('userid.'):
            break
    else:
        # NOTE: returning details below allows internal remoteuser (TEST for example) to run DELETE requests
        # previously in downstream portal applications, the LoginDenied error was raised, preventing such
        # DELETE requests from occurring within unit testing. This can be re-enabled if desired in downstream
        # applications, but for now should stay like this so we can unit test DELETEs - Will April 6 2023
        if 'group.admin' in request.effective_principals:
            return {
                'details': {
                    'groups': [
                        'admin'
                    ]
                }
            }
        else:
            raise LoginDenied(domain=request.domain)

    namespace, userid = principal.split('.', 1)
    properties = get_basic_properties_for_user(request, userid)

    # if 'auth.userid' in request.session:
    #     properties['auth.userid'] = request.session['auth.userid']

    return properties


def basic_auth_check(username, password, request):
    """ This function implements the functionality that does the actual checking of the
        access key against what is in the database. It is thus very important. Access
        key expiration is implemented here - auth will fail if it has expired
    """
    # We may get called before the context is found and the root set
    root = request.registry[ROOT]
    collection = root['access-keys']
    try:
        access_key = collection[username]
    except KeyError:
        return None

    # Check expiration first
    # Note that access keys generated awhile ago will remain valid (for now) - will 6/14/21
    properties = access_key.properties
    expiration_date = properties.get('expiration_date')
    if expiration_date:
        dt = isoparse(expiration_date)  # datetime.date.fromisoformat in Python3.7
        now = datetime.datetime.utcnow()
        if now > dt:
            return None

    # If expiration valid, check hash
    hash = properties['secret_access_key_hash']
    crypt_context = request.registry[CRYPT_CONTEXT]
    valid = crypt_context.verify(password, hash)
    if not valid:
        return None

    return []  # success


@view_config(route_name='impersonate-user', request_method='POST',
             validators=[no_validate_item_content_post],
             permission='impersonate')
@debug_log
def impersonate_user(context, request):
    """As an admin, impersonate a different user."""
    ignored(context)

    userid = request.validated['userid']
    users = request.registry[COLLECTIONS]['user']

    try:
        user = users[userid]
    except KeyError:
        raise ValidationFailure('body', ['userid'], 'User not found.')

    if user.properties.get('status') != 'current':
        raise ValidationFailure('body', ['userid'], 'User is not enabled.')

    user_properties = get_basic_properties_for_user(request, userid)
    # pop off impersonate user action if not admin
    user_properties['user_actions'] = [x for x in user_properties['user_actions'] if (x['id'] and x['id'] != 'impersonate')]
    # make a key
    registry = request.registry
    auth0_client = registry.settings.get('auth0.client')
    auth0_secret = registry.settings.get('auth0.secret')
    if not (auth0_client and auth0_secret):
        raise HTTPForbidden(title="No keys to impersonate user")

    jwt_contents = {
        'email': userid,
        'email_verified': True,
        'aud': auth0_client,
    }

    id_token = jwt.encode(
        jwt_contents,
        auth0_secret,
        algorithm=JWT_ENCODING_ALGORITHM
    )

    token_value = id_token.decode('utf-8') if isinstance(id_token, bytes) else id_token

    if redis_is_active(request):
        # In Redis mode a raw JWT cookie would simply fail to resolve on the next request (it is
        # never decoded as a JWT), so the impersonation token has to be wrapped in a session too.
        revoke_session_token(request, request.cookies.get(SESSION_COOKIE_NAME))
        session = create_session_token(request, jwt_token=token_value, email=userid)
        token_value = session.get_session_token()

    set_session_cookie(request, token_value, samesite="strict")

    return user_properties


def generate_user():
    """ Generate a random user name with 64 bits of entropy
        Used to generate access_key
    """
    # Take a random 5 char binary string (80 bits of
    # entropy) and encode it as upper cased base32 (8 chars)
    random_bytes = os.urandom(5)
    user = base64.b32encode(random_bytes).decode('ascii').rstrip('=').upper()
    return user


def generate_password():
    """ Generate a password with 80 bits of entropy
    """
    # Take a random 10 char binary string (80 bits of
    # entropy) and encode it as lower cased base32 (16 chars)
    random_bytes = os.urandom(10)
    password = base64.b32encode(random_bytes).decode('ascii').rstrip('=').lower()
    return password


# Fields the self-registration endpoint (create_unauthorized_user) is allowed to accept from
# the caller-submitted request body. This is a whitelist rather than a blocklist: the endpoint
# sets request.remote_user = 'EMBED' (which carries the 'restricted_fields' write permission)
# before validating/creating the User, so any field NOT filtered out here would be written
# through as submitted, letting a caller self-assign privileged fields such as "groups" or
# "submits_for". A blocklist only protects against field names its author thought to
# enumerate, and snovault is consumed by apps with different User schemas (e.g. fourfront's
# lab/submits_for/groups/viewing_groups vs. other consumers' equivalents), so a whitelist of
# known-safe fields fails safe across all current and future consumers where a blocklist would
# not. `pending_lab` is included as a documented exception: although its schema permission is
# restricted like the truly privileged fields, it is only a self-declared request that an admin
# must separately review and promote to `lab` before it grants any real access - unlike
# lab/groups/submits_for/viewing_groups, which grant real access immediately if set.
SELF_REGISTRATION_ALLOWED_FIELDS = frozenset({
    'email',
    'first_name',
    'last_name',
    'preferred_email',
    'job_title',
    'institution',
    'pending_lab',
})


@view_config(route_name='create-unauthorized-user', request_method='POST',
             permission=NO_PERMISSION_REQUIRED)
@debug_log
def create_unauthorized_user(context, request):
    """
    Endpoint that creates an unauthorized user - so we can distinguish between those added by admins
    and through this API.
    For CGAP, an "unauthorized user" has cgap-core project association and nothing else.
    Requires a reCAPTCHA response, which is propogated from the front end
    registration form. This is so the endpoint cannot be abused.
    TODO: propagate key, secret from GAC

    Given a user properties in the request body, will validate those and also
    validate the reCAPTCHA response using the reCAPTCHA server. If all checks
    are successful, POST a new user and login

    Args:
        context: (ignored)
        request: Request object

    Returns:
        dictionary User creation response from collection_add

    Raises:
        LoginDenied, HTTPForbidden, or ValidationFailure
    """
    ignored(context)
    # env check
    env_name = request.registry.settings.get('env.name')
    if not app_project().env_allows_auto_registration(env_name):
        raise LoginDenied(f'Tried to register on {env_name} but it is disallowed')

    recaptcha_resp = request.json.get('g-recaptcha-response')
    if not recaptcha_resp:
        raise LoginDenied(f'Did not receive response from recaptcha!')
    
    # old method for retrieving auth'd email - request object should have _auth0_authenticated set
    # NOTE: it is not obvious to me how this works... probably should be looked into - Will March 29 2023
    if not redis_is_active(request):
        email = "<no auth0 authenticated e-mail supplied>"
        if hasattr(request, "_auth0_authenticated"):
            email = request._auth0_authenticated # equal to: jwt_info['email'].lower()

    # new method for retrieving auth'd email - request should have transmitted a session token
    # from which we can get the JWT and the email they auth'd with
    else:
        # A miss here (unknown/expired/revoked/malformed token) is an authentication failure, not a
        # server error; a Redis outage raises RedisSessionUnavailable (503) out of this call.
        session = resolve_session_token(request, get_auth_token(request))
        email = session_identity(request, session) if session is not None else None
        if not email:
            raise LoginDenied(domain=request.domain,
                              title='No valid session; log in again before registering')

    user_props = request.json
    user_props_email = user_props.get("email", "<no e-mail supplied>").lower()
    if user_props_email != email:
        raise HTTPUnauthorized(
            title="Provided email {} not validated with Auth0. Try logging in again.".format(user_props_email),
            headers={'WWW-Authenticate': "Bearer realm=\"{}\"; Basic realm=\"{}\"".format(request.domain, request.domain)}
        )

    # set user insert props
    del user_props['g-recaptcha-response']
    # Strip any field not explicitly whitelisted for self-registration before validation, so a
    # caller cannot self-assign privileged fields (e.g. "groups": ["admin"]) via the elevated
    # write permission granted below. See SELF_REGISTRATION_ALLOWED_FIELDS for rationale.
    for key in list(user_props):
        if key not in SELF_REGISTRATION_ALLOWED_FIELDS:
            del user_props[key]
    user_props['was_unauthorized'] = True
    user_props['email'] = user_props_email  # lower-cased
    user_coll = request.registry[COLLECTIONS]['User']
    request.remote_user = 'EMBED'  # permission = restricted_fields

    # validate the User json
    validate_request(user_coll.type_info.schema, request, user_props)
    if request.errors:
        raise ValidationFailure('body', 'create_unauthorized_user', 'Cannot validate request')

    # validate recaptcha_resp
    # https://developers.google.com/recaptcha/docs/verify
    recap_url = 'https://www.google.com/recaptcha/api/siteverify'
    recap_secret = request.registry.settings['g.recaptcha.secret']
    recap_values = {
        'secret': recap_secret,
        'response': recaptcha_resp
    }
    data = urlencode(recap_values).encode()
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"}
    recap_res = requests.get(recap_url, params=data, headers=headers).json()

    if recap_res['success']:
        sno_res = sno_collection_add(user_coll, request, False)  # POST User
        if sno_res.get('status') == 'success':
            return sno_res
        else:
            raise HTTPForbidden(title="Could not create user. Try logging in again.")
    else:
        # error with re-captcha
        raise HTTPUnauthorized(
            title="Invalid reCAPTCHA. Try logging in again.",
            headers={
                'WWW-Authenticate':
                    "Bearer realm=\"{}\"; Basic realm=\"{}\"".format(request.domain, request.domain)}
        )
