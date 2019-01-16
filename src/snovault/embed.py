from copy import deepcopy
from .interfaces import CONNECTION
from past.builtins import basestring
from posixpath import join
from pyramid.compat import (
    native_,
    unquote_bytes_to_wsgi,
)
from pyramid.httpexceptions import HTTPNotFound
from pyramid.exceptions import URLDecodeError
from pyramid.traversal import find_resource
from pyramid.interfaces import IRoutesMapper
import logging
log = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)
    config.add_renderer('null_renderer', NullRenderer)
    config.add_request_method(embed, 'embed')
    config.add_request_method(embed, 'invoke_view')
    config.add_request_method(lambda request: {}, '_linked_uuids', reify=True)
    config.add_request_method(lambda request: set(), '_audit_uuids', reify=True)
    config.add_request_method(lambda request: {}, '_sid_cache', reify=True)
    config.add_request_method(lambda request: {}, '_rev_linked_uuids_by_item', reify=True)
    config.add_request_method(lambda request: {}, '_aggregated_items', reify=True)
    config.add_request_method(lambda request: {}, '_aggregate_for', reify=True)
    config.add_request_method(lambda request: False, '_indexing_view', reify=True)
    config.add_request_method(lambda request: None, '__parent__', reify=True)


def make_subrequest(request, path):
    """ Make a subrequest

    Copies request environ data for authentication.

    May be better to just pull out the resource through traversal and manually
    perform security checks.
    """
    env = request.environ.copy()
    if path and '?' in path:
        path_info, query_string = path.split('?', 1)
        path_info = path_info
    else:
        path_info = path
        query_string = ''
    env['PATH_INFO'] = path_info
    env['QUERY_STRING'] = query_string
    subreq = request.__class__(env, method='GET', content_type=None,
                               body=b'')
    subreq.remove_conditional_headers()
    # XXX "This does not remove headers like If-Match"
    subreq.__parent__ = request
    return subreq


def embed(request, *elements, **kw):
    """
    as_user=True for current user
    Pass in fields_to_embed as a keyword arg
    """
    # Should really be more careful about what gets included instead.
    # Cache cut response time from ~800ms to ~420ms.
    embed_cache = request.registry[CONNECTION].embed_cache
    as_user = kw.get('as_user')
    index_uuid = kw.get('index_uuid')
    path = join(*elements)
    path = unquote_bytes_to_wsgi(native_(path))
    # as_user controls whether or not the embed_cache is used
    # if request._indexing_view is True, always use the cache
    if as_user is not None and not request._indexing_view:
        result, linked_uuids, rev_linked_uuids_by_item, agg_items = _embed(request, path, as_user)
    else:
        cached = embed_cache.get(path, None)
        if cached is None:
            # handle common cases of as_user, otherwise use what's given
            subreq_user = 'EMBED' if as_user is None else as_user
            cached = _embed(request, path, as_user=subreq_user)
            # caching audits is safe because they don't add to linked_uuids
            embed_cache[path] = cached
        result, linked_uuids, rev_linked_uuids_by_item, agg_items = cached
        result = deepcopy(result)
    # aggregated_items may be cached; if so, add them to the request
    # these conditions only fulfilled when using @@embedded and aggregated
    # items have NOT yet been processed (_aggregate_for is removed if so)
    if index_uuid and getattr(request, '_aggregate_for').get('uuid') == index_uuid:
        request._aggregated_items = agg_items
        request._aggregate_for['uuid'] = None
    # hardcode this because audits can cause serious problems with frame=page
    if '@@audit' not in path:
        try:
            request._linked_uuids.update(linked_uuids)
        except ValueError:
            import pdb; pdb.set_trace()
        # this is required because rev_linked_uuids_by_item is formatted as
        # a dict keyed by item with value of set of uuids rev linking to that item
        for item, rev_links in rev_linked_uuids_by_item.items():
            if item in request._rev_linked_uuids_by_item:
                request._rev_linked_uuids_by_item[item].update(rev_links)
            else:
                request._rev_linked_uuids_by_item[item] = rev_links
    return result


def _embed(request, path, as_user='EMBED'):
    # Carl: the subrequest is 'built' here, but not actually invoked
    subreq = make_subrequest(request, path)
    subreq.override_renderer = 'null_renderer'
    subreq._indexing_view = request._indexing_view
    subreq._aggregate_for = request._aggregate_for
    subreq._aggregated_items = request._aggregated_items
    # pass the uuids we want to run audits on
    if '@@audit' in path:
        subreq._audit_uuids = request._audit_uuids
    if as_user is not True:
        if 'HTTP_COOKIE' in subreq.environ:
            del subreq.environ['HTTP_COOKIE']
        subreq.remote_user = as_user
    # _linked_uuids are populated in item_view_object of resource_views.py
    try:
        result = request.invoke_subrequest(subreq)
    except HTTPNotFound:
        raise KeyError(path)
    return (result, subreq._linked_uuids,
            subreq._rev_linked_uuids_by_item, subreq._aggregated_items)


class NullRenderer:
    '''Sets result value directly as response.
    '''
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        if request is None:
            return value
        request.response = value
        return None
