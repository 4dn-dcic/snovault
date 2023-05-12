import logging
from copy import deepcopy
from posixpath import join
from pyramid.compat import (
    native_,
    unquote_bytes_to_wsgi,
)
from pyramid.httpexceptions import HTTPNotFound, HTTPServerError
import pyramid.request
from .crud_views import collection_add as sno_collection_add
from .interfaces import COLLECTIONS, CONNECTION
from .resources import Collection
from .schema_utils import validate_request
from dcicutils.misc_utils import check_true

log = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)
    config.add_renderer('null_renderer', NullRenderer)
    config.add_request_method(embed, 'embed')
    config.add_request_method(embed, 'invoke_view')
    config.add_request_method(lambda request: set(), '_linked_uuids', reify=True)
    config.add_request_method(lambda request: {}, '_sid_cache', reify=True)
    config.add_request_method(lambda request: {}, '_rev_linked_uuids_by_item', reify=True)
    config.add_request_method(lambda request: {}, '_aggregated_items', reify=True)
    config.add_request_method(lambda request: {}, '_aggregate_for', reify=True)
    config.add_request_method(lambda request: False, '_indexing_view', reify=True)
    config.add_request_method(lambda request: None, '__parent__', reify=True)


# really simple exception for when a primary indexed item gets HTTPNotFound
class MissingIndexItemException(Exception):
    pass


def make_subrequest(request, path, method='GET', json_body=None, inherit_user=False, inherit_registry=False):
    """
    Make a subrequest from a parent request given a request path.
    Copies request environ data for authentication. Handles making the path
    WSGI compatible. Optionally can take a JSON body to attach to subrequest.
    Used in _embed to form requests to invoke, and can also be used externally

    May be better to just pull out the resource through traversal and manually
    perform security checks.

    Args:
        request: current Request object
        path (str): path for the subrequest. Can include query string
        method (str): subrequest method, defaults to GET
        json_body (dict): optional dict to attach as json_body to subrequest
        inherit_user (bool) : optional to inherit remote_user from parent request
        inherit_registry (bool): optional to inherit registry from parent request

    Returns:
        Request: the subrequest
    """
    env = request.environ.copy()
    # handle path, include making wsgi compatible and splitting out query string
    path = unquote_bytes_to_wsgi(native_(path))
    if path and '?' in path:
        path_info, query_string = path.split('?', 1)
        path_info = path_info
    else:
        path_info = path
        query_string = ''
    env['PATH_INFO'] = path_info
    env['QUERY_STRING'] = query_string
    subreq = request.__class__(env, method=method, content_type=None)
    if json_body:
        subreq.json = json_body
    else:
        subreq.body = b''
    subreq.remove_conditional_headers()
    # XXX "This does not remove headers like If-Match"
    subreq.__parent__ = request
    if inherit_user:
        subreq.remote_user = request.remote_user
    if inherit_registry:
        subreq.registry = request.registry
    return subreq


def embed(request, *elements, **kw):
    """
    Incredibly important function that is central to getting views in snovault.
    Since it is a reified method on Request, you can call it like:
    `request.embed(<elements to be joined in path>)`
    This function handles propogation of important request attrs to subrequests,
    as well as caching of requests and grabbing attrs from the subreq result.

    Check connection.py and cache.py for details on the embed_cache

    NOTES:
        path is formed by joining all positional args
        as_user=True for current user
        Pass in fields_to_embed as a keyword arg

    Args:
        request: Request calling this method
        *elements: variable length positional args used to make path
        **kw: arbitrary keyword arguments

    Returns:
        result of the invoked request
    """
    # Should really be more careful about what gets included instead.
    # Cache cut response time from ~800ms to ~420ms.
    embed_cache = request.registry[CONNECTION].embed_cache
    as_user = kw.get('as_user')
    index_uuid = kw.get('index_uuid')
    path = join(*elements)
    # as_user controls whether or not the embed_cache is used
    # if request._indexing_view is True, always use the cache
    if as_user is not None and not request._indexing_view:
        cached = _embed(request, path, as_user)
    else:
        cached = embed_cache.get(path, None)
        if cached is None:
            # handle common cases of as_user, otherwise use what's given
            subreq_user = 'EMBED' if as_user is None else as_user
            cached = _embed(request, path, as_user=subreq_user)
            embed_cache[path] = cached

    # NOTE: if result was retrieved from ES, the following cached attrs will be
    # empty: _aggregated_items, _linked_uuids, _rev_linked_by_item
    result = deepcopy(cached['result'])

    # aggregated_items may be cached; if so, add them to the request
    # these conditions only fulfilled when using @@embedded and aggregated
    # items have NOT yet been processed (_aggregate_for is removed if so)
    if index_uuid and getattr(request, '_aggregate_for').get('uuid') == index_uuid:
        request._aggregated_items = cached['_aggregated_items']
        request._aggregate_for['uuid'] = None
    request._linked_uuids.update(cached['_linked_uuids'])
    request._sid_cache.update(cached['_sid_cache'])
    # this is required because rev_linked_uuids_by_item is formatted as
    # a dict keyed by item with value of set of uuids rev linking to that item
    for item, rev_links in cached['_rev_linked_by_item'].items():
        if item in request._rev_linked_uuids_by_item:
            request._rev_linked_uuids_by_item[item].update(rev_links)
        else:
            request._rev_linked_uuids_by_item[item] = rev_links
    return result


def _embed(request, path, as_user='EMBED'):
    """
    Helper function used in embed() that creates the subrequest and actually
    invokes it. Sets a number of attributes from the parent request and
    returns a dictionary containing the result and a number of attributes
    from the invoked subreq.

    Another consideration, now that we're purging items from the DB, is that
    primarily indexed items may be purged by the time they make it to _embed.
    Check if @@index-data is in the request path to ensure this is the case,
    and gracefully exit with MissingIndexItemException on HTTPNotFound if so.

    Args:
        request: Request object
        path (str): subrequest path to invoke
        as_user (str/bool): involved in setting subreq.remote_user

    Returns:
        dict containing the result and a number of subrequest attributes
    """
    # Carl: the subrequest is 'built' here, but not actually invoked
    subreq = make_subrequest(request, path)
    # these attributes are propogated across the subrequest
    subreq.override_renderer = 'null_renderer'
    subreq._indexing_view = request._indexing_view
    subreq._aggregate_for = request._aggregate_for
    subreq._aggregated_items = request._aggregated_items
    subreq._sid_cache = request._sid_cache
    if as_user is not True:
        if 'HTTP_COOKIE' in subreq.environ:
            del subreq.environ['HTTP_COOKIE']
        subreq.remote_user = as_user
    # _linked_uuids are populated in item_view_object of resource_views.py
    try:
        result = request.invoke_subrequest(subreq)
    except HTTPNotFound:
        if '@@index-data' in path:
            # the resource to index is missing; likely purged
            raise MissingIndexItemException(path)
        else:
            # the resource is unexpectedly missing
            raise KeyError(path)
    return {'result': result, '_linked_uuids': subreq._linked_uuids,
            '_rev_linked_by_item': subreq._rev_linked_uuids_by_item,
            '_aggregated_items': subreq._aggregated_items,
            '_sid_cache': subreq._sid_cache}


def subrequest_object(request, object_id):
    subreq = make_subrequest(request, "/" + object_id)
    subreq.headers['Accept'] = 'application/json'
    # Tweens are suppressed here because this is an internal call and doesn't need things like HTML processing.
    # -kmp 2-Feb-2021
    response = request.invoke_subrequest(subreq, use_tweens=False)
    if response.status_code >= 300:  # alas, the response from a pyramid subrequest has no .raise_for_status()
        raise HTTPServerError("Error obtaining object: %s" % object_id)
    object_json = response.json
    return object_json


def subrequest_item_creation(request: pyramid.request.Request, item_type: str, json_body: dict = None) -> dict:
    """
    Acting as proxy on behalf of request, this creates a new item of the given item_type with attributes per json_body.

    For example,

        subrequest_item_creation(request=request, item_type='NobelPrize',
                                 json_body={'category': 'peace', 'year': 2016))

    Args:
        request: the request on behalf of which this subrequest is done
        item_type: the name of the item item type to be created
        json_body: a python dictionary representing JSON containing data to use in initializing the newly created item

    Returns:
        a python dictionary (JSON description) of the item created

    """

    if json_body is None:
        json_body = {}
    collection_path = '/' + item_type
    method = 'POST'
    # json_utf8 = json.dumps(json_body).encode('utf-8')  # Unused, but here just in case
    check_true(not request.remote_user, "request.remote_user has %s before we set it." % request.remote_user)
    request.remote_user = 'EMBED'
    subrequest = make_subrequest(request=request, path=collection_path, method=method, json_body=json_body)
    subrequest.remote_user = 'EMBED'
    subrequest.registry = request.registry
    # Maybe...
    # validated = json_body.copy()
    # subrequest.validated = validated
    registry: Registry = subrequest.registry  # noQA - PyCharm can't tell subrequest.registry IS a Registry
    collection: Collection = registry[COLLECTIONS][item_type]
    check_true(subrequest.json_body, "subrequest.json_body is not properly initialized.")
    check_true(not subrequest.validated, "subrequest was unexpectedly validated already.")
    check_true(not subrequest.errors, "subrequest.errors already has errors before trying to validate.")
    check_true(subrequest.remote_user == request.remote_user,
               "Mismatch: subrequest.remote_user=%r request.remote_user=%r"
               % (subrequest.remote_user, request.remote_user))
    validate_request(schema=collection.type_info.schema, request=subrequest, data=json_body)
    if not subrequest.validated:
        return {
            "@type": ["Exception"],
            "errors": subrequest.errors
        }
    else:
        json_result: dict = sno_collection_add(context=collection, request=subrequest, render=False)
        return json_result


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
