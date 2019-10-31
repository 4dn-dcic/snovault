"""
Cached views used when model was pulled from elasticsearch.
In some cases, use these views to render object and embedded content
"""

from itertools import chain
from pyramid.httpexceptions import HTTPForbidden
from pyramid.view import view_config
from .interfaces import ICachedItem
from ..resource_views import (
    item_view_embedded,
    item_view_object,
    item_view_page,
    item_view_expand
)
from ..indexing_views import item_index_data


def includeme(config):
    config.scan(__name__)


def filter_embedded(embedded, effective_principals):
    """
    Filter the embedded items by principals_allowed, replacing them with
    a 'no view allowed' error message if the effective principals on the
    request are disjointed
    """
    _skip_fields = ['@type', 'principals_allowed']
    # handle dictionary
    if isinstance(embedded, dict):
        if 'principals_allowed' in embedded.keys():
            obj_princ = embedded.get('principals_allowed')
            allowed = set(obj_princ['view'])
            if allowed.isdisjoint(effective_principals):
                embedded = {'error': 'no view permissions'}
                return embedded

        for name, obj in embedded.items():
            if isinstance(obj, (dict, list)) and name not in _skip_fields:
                embedded[name] = filter_embedded(obj, effective_principals)

    # handle array
    elif isinstance(embedded, list):
        for idx, item in enumerate(embedded):
            embedded[idx] = filter_embedded(item, effective_principals)

    # default just return the sucker
    return embedded


@view_config(context=ICachedItem, permission='view', request_method='GET',
             name='embedded')
def cached_view_embedded(context, request):

    source = context.model.source
    # generate object view if using a read-only item
    if 'object' not in source and context.used_datastore == 'elasticsearch':
        embedded = item_view_embedded(context, request)
    else:
        embedded = source['embedded']

    # permission checking on embedded objects
    allowed = set(embedded['principals_allowed']['view'])
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    return filter_embedded(embedded, request.effective_principals)


@view_config(context=ICachedItem, permission='view', request_method='GET',
             name='object')
def cached_view_object(context, request):
    source = context.model.source
    # generate object view if using a read-only item
    if 'object' not in source and context.used_datastore == 'elasticsearch':
        object = item_view_object(context, request)
    else:
        object = source['object']

    # permission checking. Is this redundant with permission set on view?
    allowed = set(object['principals_allowed']['view'])
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    return object


@view_config(context=ICachedItem, permission='view_raw', request_method='GET',
             name='raw')
def cached_view_raw(context, request):
    source = context.model.source
    props = source['properties']
    # add uuid to raw view
    props['uuid'] = source['uuid']
    return props


@view_config(context=ICachedItem, permission='view', request_method='GET',
             name='page')
def cached_view_page(context, request):
    return item_view_page(context, request)


@view_config(context=ICachedItem, permission='expand', request_method='GET',
             name='expand')
def cached_view_expand(context, request):
    return item_view_expand(context, request)


@view_config(context=ICachedItem, permission='index', request_method='GET',
             name='index-data', )
def cached_index_data(context, request):
    return item_index_data(context, request)
