import logging
import os
import psutil
import time

# from pkg_resources import resource_filename
from pyramid.events import BeforeRender, subscriber
from pyramid.httpexceptions import (
    HTTPMovedPermanently,
    HTTPPreconditionFailed,
    HTTPUnauthorized,
    HTTPUnsupportedMediaType,
)
from pyramid.security import forget
from pyramid.settings import asbool
from pyramid.threadlocal import manager
from pyramid.traversal import split_path_info, _join_path_tuple
from subprocess_middleware.tween import SubprocessTween
from webob.cookies import Cookie
from .validation import CSRFTokenError


log = logging.getLogger(__name__)


def includeme(config):
    config.add_tween('.renderers.fix_request_method_tween_factory',
                     under='snovault.stats.stats_tween_factory')
    config.add_tween('.renderers.normalize_cookie_tween_factory',
                     under='.renderers.fix_request_method_tween_factory')
    config.add_tween('.renderers.set_x_request_url_tween_factory',
                     under='.renderers.normalize_cookie_tween_factory')
    config.add_tween('.renderers.security_tween_factory',
                     under='pyramid_tm.tm_tween_factory')
    config.scan(__name__)


def fix_request_method_tween_factory(handler, registry):
    """ Fix Request method changed by mod_wsgi.

    See: https://github.com/GrahamDumpleton/mod_wsgi/issues/2

    Apache config:
        SetEnvIf Request_Method HEAD X_REQUEST_METHOD=HEAD
    """

    def fix_request_method_tween(request):
        environ = request.environ
        if 'X_REQUEST_METHOD' in environ:
            environ['REQUEST_METHOD'] = environ['X_REQUEST_METHOD']
        return handler(request)

    return fix_request_method_tween


def security_tween_factory(handler, registry):

    def security_tween(request):
        login = None
        expected_user = request.headers.get('X-If-Match-User')
        if expected_user is not None:
            login = request.authenticated_userid
            if login != 'mailto.' + expected_user:
                detail = 'X-If-Match-User does not match'
                raise HTTPPreconditionFailed(detail)

        # wget may only send credentials following a challenge response.
        auth_challenge = asbool(request.headers.get('X-Auth-Challenge', False))
        if auth_challenge or request.authorization is not None:
            login = request.authenticated_userid
            if login is None:
                raise HTTPUnauthorized(headerlist=forget(request))

        if request.method in ('GET', 'HEAD'):
            return handler(request)

        if request.content_type != 'application/json':
            detail = "%s is not 'application/json'" % request.content_type
            raise HTTPUnsupportedMediaType(detail)

        token = request.headers.get('X-CSRF-Token')
        if token is not None:
            # Avoid dirtying the session and adding a Set-Cookie header
            # XXX Should consider if this is a good idea or not and timeouts
            if token == dict.get(request.session, '_csrft_', None):
                return handler(request)
            raise CSRFTokenError('Incorrect CSRF token')

        # NOTE: cutting out CSRF protection here ... why protect against CSRF if you provide an
        # unathenticated endpoint that will delivery the CSRF token? I'm looking at you /session.
        # this should be revisted, either embed the csrf token in the index.html as part of the
        # rendering subprocess somehow, or return it from the login view and let the client store it
        # but of course that sounds a lot like JWT...
        return handler(request)

        if login is None:
            login = request.authenticated_userid
        if login is not None:
            namespace, userid = login.split('.', 1)
            if namespace not in ('mailto', 'persona'):
                return handler(request)
        raise CSRFTokenError('Missing CSRF token')

    return security_tween


def normalize_cookie_tween_factory(handler, registry):

    ignore = {
        '/favicon.ico',
    }

    def normalize_cookie_tween(request):
        if request.path in ignore or request.path.startswith('/static/'):
            return handler(request)

        session = request.session
        if session or session._cookie_name not in request.cookies:
            return handler(request)

        response = handler(request)
        existing = response.headers.getall('Set-Cookie')
        if existing:
            cookies = Cookie()
            for header in existing:
                cookies.load(header)
            if session._cookie_name in cookies:
                return response

        response.delete_cookie(
            session._cookie_name,
            path=session._cookie_path,
            domain=session._cookie_domain,
        )

        return response

    return normalize_cookie_tween


def set_x_request_url_tween_factory(handler, registry):

    def set_x_request_url_tween(request):
        response = handler(request)
        response.headers['X-Request-URL'] = request.url
        return response

    return set_x_request_url_tween


@subscriber(BeforeRender)
def canonical_redirect(event):
    request = event['request']

    # Ignore subrequests
    if len(manager.stack) > 1:
        return

    if request.method not in ('GET', 'HEAD'):
        return
    if request.response.status_int != 200:
        return
    if not request.environ.get('snowflakes.canonical_redirect', True):
        return
    if request.path_info == '/':
        return

    if not isinstance(event.rendering_val, dict):
        return

    canonical = event.rendering_val.get('@id', None)
    if canonical is None:
        return
    canonical_path, _, canonical_qs = canonical.partition('?')

    request_path = _join_path_tuple(('',) + split_path_info(request.path_info))
    if (request_path == canonical_path.rstrip('/') and
            request.path_info.endswith('/') == canonical_path.endswith('/') and
            (canonical_qs in ('', request.query_string))):
        return

    if '/@@' in request.path_info:
        return

    qs = canonical_qs or request.query_string
    location = canonical_path + ('?' if qs else '') + qs
    raise HTTPMovedPermanently(location=location)
