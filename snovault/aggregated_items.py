from pyramid.httpexceptions import HTTPForbidden
from pyramid.view import view_config

from .calculated import calculated_property
from .resources import Item
from .util import debug_log


def includeme(config):
    config.include('.calculated')
    config.scan(__name__)


@view_config(context=Item, permission='view', request_method='GET',
             name='aggregated-items')
@debug_log
def item_view_aggregated_items(context, request):
    """
    View config for aggregated_items. If the current model is not using ES,
    do not calculate the aggregated_items, as that would required the whole
    @@index-data view to be run

    Args:
        context: current Item
        request: current request

    Returns:
        A dictionary including item path and aggregated_items
    """
    # if we do not have the cached ES model, do not run aggs
    if context.model.used_datastore != 'elasticsearch':
        return {
            '@id': request.resource_path(context),
            'aggregated_items': {},
        }
    source = context.model.source
    allowed = set(source['principals_allowed']['view'])  # use view permissions
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    return {
        '@id': source['object']['@id'],
        'aggregated_items': source.get('aggregated_items', {})
    }


@calculated_property(context=Item, category='page', name='aggregated-items',
                     condition=lambda request: request.has_permission('view'))
def aggregated_items_property(context, request):
    """
    Frame=page calculated property to add aggregated_items to response.
    The request.embed calls item_view_aggregated_items
    Args:
        context: current Item
        request: current Request
    Returns:
        Dictionary result of aggregated_items
    """
    path = request.resource_path(context)
    return request.embed(path, '@@aggregated-items')['aggregated_items']
