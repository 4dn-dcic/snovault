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
from pyramid.httpexceptions import HTTPForbidden, HTTPUnauthorized
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

JWT_ALL_ALGORITHMS = ['ES512', 'RS384', 'HS512', 'ES256', 'none',
                      'RS256', 'PS512', 'ES384', 'HS384', 'ES521',
                      'PS384', 'HS256', 'PS256', 'RS512']

# Probably we could get away with fewer, but I think not as few as just our own encoding algorithm,
# so for now I believe the above list was the default, and this just rearranges it to prefer the one
# we use for encoding. -kmp 19-Jan-2021

JWT_DECODING_ALGORITHMS = [JWT_ENCODING_ALGORITHM] + remove_element(JWT_ENCODING_ALGORITHM, JWT_ALL_ALGORITHMS)

CONTENT_TYPE = "Content-Type"
JSON_CONTENT_TYPE = "application/json"
STANDARD_HEADERS = {CONTENT_TYPE: JSON_CONTENT_TYPE}


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

def redis_is_active(request):
    """ Quick helper to standardize detecting whether redis is in use """
    return 'redis.server' in request.registry.settings

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
    is_https = request.scheme == "https"

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
        auth0_response_json = auth0_response.json()
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
   
    auth0_response_json = auth0_response.json()
    auth0_jwt = auth0_response_json.get('id_token')
    if not auth0_jwt:
        raise LoginDenied('No JWT returned from Auth0, check Auth0 configuration')
    
    # email
    if 'auth0' in auth0_domain:
        # Check that the user exists in our database, if they do not, redirect them to /registration
        email = Auth0AuthenticationPolicy.get_token_info(auth0_jwt, request).get('email', '').lower()
    elif 'nih.gov' in auth0_domain:
        # In RAS authentication, user info is not included in the JWT token, but in a passport that requires an extra request.
        passport_post_url = f'https://{auth0_domain}/openid/connect/v1/userinfo'
        passport_headers = {'Authorization': f'Bearer {auth0_response_json["access_token"]}'}
        passport_response = requests.post(passport_post_url, headers=passport_headers)
        passport_response_json = passport_response.json()
        email = passport_response_json.get('email', '').lower()

    if not email:
        raise LoginDenied('No email extracted from JWT, not possible to continue')
    
    # Generate a session from Redis
    redis_handler = registry[REDIS]
    env_name = registry.settings['env.name']
    redis_session_token = RedisSessionToken(
        namespace=env_name,
        jwt=auth0_jwt,
        email=email
    )

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

    # Give a session token unconditionally so we can retrieve JWT later on
    # in the registration scenario (if an unknown user) or make auth'd requests
    # as an existing user
    redis_session_token.store_session_token(redis_handler=redis_handler)
    request.response.set_cookie(
        'jwtToken',  # note that although we are setting jwtToken, it is NOT a JWT when going through this route
        value=redis_session_token.get_session_token(),
        domain=request.domain,
        path='/',
        httponly=True,
        samesite='lax',
        overwrite=True,
        secure=is_https
    )
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
        id_token = get_jwt(request)
        if not id_token:
            # can I thrown an 403 here?
            # print('Missing assertion.', 'unauthenticated_userid', request)
            return None

        jwt_info = self.get_token_info(id_token, request)
        if not jwt_info:
            return None

        email = request._auth0_authenticated = jwt_info['email'].lower()

        # At this point, email has been authenticated with their Auth0 provider and via `get_token_info`,
        # but we don't know yet if this email is in our database. `authenticated_userid` should take care of this.

        app_project().note_auth0_authentication_policy_unauthenticated_userid(self, request, email, id_token)

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


def get_jwt(request):

    # First try to obtain JWT from headers (case: some REST API requests)
    token = get_jwt_from_auth_header(request)

    # If the JWT is not in the headers, get it from cookies (case: AJAX requests from portal & other clients)
    if not token:
        token = request.cookies.get('jwtToken')

    return token


@view_config(route_name='login', request_method='POST', permission=NO_PERMISSION_REQUIRED)
@debug_log
def login_view(context, request, samesite: str = "strict"):
    return app_project().login(context, request, samesite=samesite)


def login(context, request, *, samesite: str = "strict"):
    """
    Save JWT as httpOnly cookie
    """
    ignored(context)

    # Allow providing token thru Authorization header as well as POST request body.
    # Should be about equally secure if using HTTPS.
    request_token = get_jwt_from_auth_header(request)
    if request_token is None:
        request_token = request.json_body.get("id_token", None)

    is_https = (request.scheme == "https")

    request.response.set_cookie(
        "jwtToken",
        value=request_token,
        domain=request.domain,
        path="/",
        httponly=True,
        samesite=samesite,
        overwrite=True,
        secure=is_https
    )

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
    """
    ignored(context)

    # Deletes the cookie
    request.response.set_cookie(
        name='jwtToken',
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

    is_https = request.scheme == "https"
    token_value = id_token.decode('utf-8') if isinstance(id_token, bytes) else id_token
    request.response.set_cookie(
        "jwtToken",
        value=token_value,
        domain=request.domain,
        path="/",
        httponly=True,
        samesite="strict",
        overwrite=True,
        secure=is_https
    )

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
    
    registry = request.registry

    # old method for retrieving auth'd email - request object should have _auth0_authenticated set
    # NOTE: it is not obvious to me how this works... probably should be looked into - Will March 29 2023
    if not redis_is_active(request):
        email = "<no auth0 authenticated e-mail supplied>"
        if hasattr(request, "_auth0_authenticated"):
            email = request._auth0_authenticated # equal to: jwt_info['email'].lower()

    # new method for retrieving auth'd email - request should have transmitted a session token
    # from which we can get the JWT and the email they auth'd with
    else:
        id_token = get_jwt(request)
        redis_handler = registry[REDIS]
        env_name = registry.settings['env.name']
        auth0_domain = request.registry.settings['auth0.domain']
        if 'auth0' in auth0_domain:
            secret = request.registry.settings['auth0.secret']
            algorithms = JWT_DECODING_ALGORITHMS
        else:
            # RAS
            secret = request.registry.settings['auth0.public.key']
            algorithms = ['RS256']

        redis_session_token = RedisSessionToken.from_redis(
            redis_handler=redis_handler,
            namespace=env_name,
            token=id_token
        )
        jwt_info = redis_session_token.decode_jwt(
                audience=request.registry.settings['auth0.client'],
                secret=secret,
                algorithms=algorithms
        )
        if jwt_info.get('email') is None:
            jwt_info['email'] = redis_session_token.get_email()
        email = jwt_info.get('email', '<no e-mail supplied>').lower()

    user_props = request.json
    user_props_email = user_props.get("email", "<no e-mail supplied>").lower()
    if user_props_email != email:
        raise HTTPUnauthorized(
            title="Provided email {} not validated with Auth0. Try logging in again.".format(user_props_email),
            headers={'WWW-Authenticate': "Bearer realm=\"{}\"; Basic realm=\"{}\"".format(request.domain, request.domain)}
        )

    # set user insert props
    del user_props['g-recaptcha-response']
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
