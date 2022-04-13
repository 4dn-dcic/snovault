import sys
from itertools import islice
from urllib.parse import urlencode

from future.utils import (
    raise_with_traceback,
    itervalues,
)
from past.builtins import basestring
from pyramid.exceptions import PredicateMismatch
from pyramid.httpexceptions import HTTPNotFound
from pyramid.settings import asbool
from pyramid.view import (
    render_view_to_response,
    view_config,
)

from .calculated import calculate_properties
from .resources import (
    AbstractCollection,
    Item,
    Root,
)
from .util import (
    expand_path,
    build_embedded_model,
    expand_embedded_model,
    process_aggregated_items,
    check_es_and_cache_linked_sids,
    validate_es_content,
    debug_log,
    merge_calculated_into_properties
)
from .util import filter_embedded


def includeme(config):
    config.scan(__name__)


@view_config(context=AbstractCollection, permission='list', request_method='GET',
             name='listing')
@debug_log
def collection_view_listing_db(context, request):
    result = {}

    frame = request.params.get('frame', 'columns')

    limit = request.params.get('limit', 25)
    if limit in ('', 'all'):
        limit = None
    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            limit = 25

    items = (
        item for item in itervalues(context)
        if request.has_permission('view', item)
    )

    if limit is not None:
        items = islice(items, limit)

    result['@graph'] = [
        request.embed(request.resource_path(item, '@@' + frame))
        for item in items
    ]

    if limit is not None and len(result['@graph']) == limit:
        params = [(k, v) for k, v in request.params.items() if k != 'limit']
        params.append(('limit', 'all'))
        result['all'] = '%s?%s' % (request.resource_path(context), urlencode(params))

    return result


@view_config(context=Root, request_method='GET', name='page')
@debug_log
def home(context, request):
    properties = request.embed(request.resource_path(context), '@@object')
    calculated = calculate_properties(context, request, properties, category='page')
    merge_calculated_into_properties(properties, calculated)
    return properties


@view_config(context=Root, request_method='GET', name='object')
@view_config(context=AbstractCollection, permission='list', request_method='GET', name='object')
@debug_log
def collection_view_object(context, request):
    properties = context.__json__(request)
    calculated = calculate_properties(context, request, properties)
    merge_calculated_into_properties(properties, calculated)
    return properties


@view_config(context=AbstractCollection, permission='list', request_method='GET', name='page')
@debug_log
def collection_list(context, request):
    path = request.resource_path(context)
    properties = request.embed(path, '@@object')
    calculated = calculate_properties(context, request, properties, category='page')
    merge_calculated_into_properties(properties, calculated)

    if request.query_string:
        properties['@id'] += '?' + request.query_string

    result = request.embed(path, '@@listing?' + request.query_string, as_user=True)
    result.update(properties)
    return result


@view_config(context=Root, request_method='GET')
@view_config(context=AbstractCollection, permission='list', request_method='GET')
@view_config(context=Item, permission='view', request_method='GET')
@debug_log
def item_view(context, request):
    frame = request.params.get('frame', 'page')
    if getattr(request, '__parent__', None) is None:
        # We need the response headers from non subrequests
        try:
            response = render_view_to_response(context, request, name=frame)
        except PredicateMismatch:
            # Avoid this view emitting PredicateMismatch
            exc_class, exc, tb = sys.exc_info()
            exc.__class__ = HTTPNotFound
            raise_with_traceback(exc, tb)
        else:
            if response is None:
                raise HTTPNotFound('?frame=' + frame)
            return response
    path = request.resource_path(context, '@@' + frame)
    if request.query_string:
        path += '?' + request.query_string
    return request.embed(path, as_user=True)


@view_config(context=Item, permission='view', request_method='GET',
             name='object')
@debug_log
def item_view_object(context, request):
    """
    Render json structure. This is the most basic view and contains the scope
    of only one item. The steps to generate it are:
    1. Fetch stored properties, possibly upgrading.
    2. Link canonicalization (overwriting uuids with links)
       - adds uuid to request._linked_uuids if request._indexing_view
    3. Calculated properties

    On a DB request, will use the Elasticsearch result for the view if the ES
    result passes `validate_es_content` (has valid sids and rev_links)

    Args:
        context: current Item
        request: current Request

    Returns:
        Dictionary item properties
    """
    if request.datastore == 'elasticsearch':
        es_res = check_es_and_cache_linked_sids(context, request, 'object')
        # validate_es_content also checks/updates rev links
        if es_res and validate_es_content(context, request, es_res, 'object'):
            # if indexing, handle linked_uuids
            if request._indexing_view is True:
                request._linked_uuids = [link['uuid'] for link in es_res['linked_uuids_object']]
            return es_res['object']

    properties = context.item_with_links(request)
    calculated = calculate_properties(context, request, properties)
    merge_calculated_into_properties(properties, calculated)
    return properties


@view_config(context=Item, permission='view', request_method='GET', name='embedded')
@debug_log
def item_view_embedded(context, request):
    """
    Calculate and return the embedded view for an item. This is an intensive
    process that requires an embedded model to be created from the
    context.embedded_list and then build from traversal of individual object
    views from the items needed.
    This function also takes care of processing aggregated_items, which are
    held on the request object (and transferred to subrequests in embed.py)

    On a DB request, will use the Elasticsearch result for the view if the ES
    result passes `validate_es_content` (has valid sids and rev_links)

    Args:
        context: current Item
        request: current Request

    Returns:
        Dictionary item properties
    """
    if request.datastore == 'elasticsearch':
        es_res = check_es_and_cache_linked_sids(context, request, 'embedded')
        # validate_es_content also checks/updates rev links
        if es_res and validate_es_content(context, request, es_res, 'embedded'):
            # if indexing, handle aggregated_items and linked_uuids
            if request._indexing_view is True:
                request._linked_uuids = [link['uuid'] for link in es_res['linked_uuids_embedded']]
            if getattr(request, '_aggregate_for').get('uuid') == str(context.uuid):
                # format this in a specific way to work with further processing
                request._aggregated_items = {agg: {'items': val} for agg, val in
                                             es_res['aggregated_items'].items()}
                request._aggregate_for['uuid'] = None
            return filter_embedded(es_res['embedded'], request.effective_principals)

    # set up _aggregated_items if we want to aggregate this target
    will_aggregate = getattr(request, '_aggregate_for').get('uuid') == str(context.uuid)
    if will_aggregate:
        parent_path = request.resource_path(context)
    else:
        parent_path = None

    item_path = request.resource_path(context)
    properties = request.embed(item_path, '@@object', as_user=True)
    embedded_model = build_embedded_model(context.embedded)
    embedded = expand_embedded_model(request, properties, embedded_model,
                                     parent_path=parent_path)
    if will_aggregate:
        process_aggregated_items(request)
        # we don't want to aggregate this item again, such as if the same item
        # is used in a frame=embedded calc property.
        # use a list here so that the reference is maintained through subreq
        request._aggregate_for['uuid'] = None

    return embedded


@view_config(context=Item, permission='view', request_method='GET',
             name='page')
@debug_log
def item_view_page(context, request):
    item_path = request.resource_path(context)
    properties = request.embed(item_path, '@@embedded', as_user=True)
    calculated = calculate_properties(context, request, properties, category='page')
    merge_calculated_into_properties(properties, calculated)
    return properties


@view_config(context=Item, permission='expand', request_method='GET',
             name='expand')
@debug_log
def item_view_expand(context, request):
    path = request.resource_path(context)
    properties = request.embed(path, '@@object', as_user=True)
    for path in request.params.getall('expand'):
        expand_path(request, properties, path)
    return properties


def expand_column(request, obj, subset, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if not remaining:
        subset[name] = value
        return
    if isinstance(value, list):
        if name not in subset:
            subset[name] = [{} for i in range(len(value))]
        for index, member in enumerate(value):
            if not isinstance(member, dict):
                member = request.embed(member, '@@object')
            expand_column(request, member, subset[name][index], remaining)
    else:
        if name not in subset:
            subset[name] = {}
        if not isinstance(value, dict):
            value = request.embed(value, '@@object')
        expand_column(request, value, subset[name], remaining)


@view_config(context=Item, permission='view', request_method='GET',
             name='columns')
@debug_log
def item_view_columns(context, request):
    path = request.resource_path(context)
    properties = request.embed(path, '@@object')
    if context.schema is None or 'columns' not in context.schema:
        return properties

    subset = {
        '@id': properties['@id'],
        '@type': properties['@type'],
    }

    for column in context.schema['columns']:
        path = column.split('.')
        if path[-1] == 'length':
            path.pop()
        if path:
            expand_column(request, properties, subset, path)

    return subset


@view_config(context=Item, permission='view_raw', request_method='GET',
             name='raw')
@debug_log
def item_view_raw(context, request):
    props = context.properties
    # only upgrade properties if explicitly requested
    if asbool(request.params.get('upgrade', True)):
        props = context.upgrade_properties()
    # add uuid to raw view
    props['uuid'] = str(context.uuid)
    return props
