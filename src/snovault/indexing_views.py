from __future__ import unicode_literals
from pyramid.security import (
    Authenticated,
    Everyone,
    principals_allowed_by_permission,
)
from pyramid.traversal import resource_path
from pyramid.view import view_config
from .resources import Item
from .authentication import calc_principals
from .elasticsearch.indexer_utils import find_rev_linked_uuids
from .interfaces import STORAGE

def includeme(config):
    config.scan(__name__)


# really simple exception to know when the sid check fails
class SidException(Exception):
    pass


def cache_linked_sids_from_db(context, request):
    """
    Key dict by uuid, find all linked_uuids from es if available
    """
    sid_uuids = set()
    # first, find any linked_uuids from this item if it is already in ES
    es_res = request.registry[STORAGE].read.get_by_uuid(str(context.uuid))
    if es_res:
        es_linked_uuids = es_res.source.get('linked_uuids', [])
        sid_uuids |= set(es_linked_uuids)
    # add any items that are rev_linked (may or may not already be found)
    rev_linked_uuids = find_rev_linked_uuids(request.registry, str(context.uuid))
    sid_uuids |= rev_linked_uuids

    res = request.registry[STORAGE].write.get_sids_by_uuids(list(sid_uuids))

    for uuid in sid_uuids:
        db_res = request.registry[STORAGE].write.get_by_uuid(uuid)
        if db_res:
            request._sid_cache[uuid] = db_res.sid


@view_config(context=Item, name='index-data', permission='index', request_method='GET')
def item_index_data(context, request):
    """
    Very important view which is used to calculate all the data indexed in ES
    for the given item. If an int sid is provided as a request parameter,
    will raise an sid exception if the current item context is behind the
    given sid.
    Computationally intensive. Performs the full embedding and calculates
    audits and aggregated_items, among other things.

    Args:
        context: current Item
        request: current request

    Returns:
        A dictionary document representing the full data to index for the
        given item
    """
    uuid = str(context.uuid)
    properties = context.upgrade_properties()

    # if we want to check an sid, it should be set as a query param
    sid_check = request.params.get('sid', None)
    if sid_check:
        try:
            sid_check = int(sid_check)
        except ValueError:
            raise ValueError('sid parameter must be an integer. Provided sid: %s' % sid)
        if context.sid < sid_check:
            raise SidException('sid from the query (%s) is greater than that on context (%s). Bailing.' % (sid_check, context.sid))

    # ES versions 2 and up don't allow dots in links. Update these to use ~s
    new_links = {}
    for key, val in context.links(properties).items():
        new_links['~'.join(key.split('.'))] = val
    links = new_links
    unique_keys = context.unique_keys(properties)

    principals_allowed = calc_principals(context)
    path = resource_path(context)
    paths = {path}
    collection = context.collection

    if collection.unique_key in unique_keys:
        paths.update(
            resource_path(collection, key)
            for key in unique_keys[collection.unique_key])

    for base in (collection, request.root):
        for key_name in ('accession', 'alias'):
            if key_name not in unique_keys:
                continue
            paths.add(resource_path(base, uuid))
            paths.update(
                resource_path(base, key)
                for key in unique_keys[key_name])

    path = path + '/'
    # setting _indexing_view enables the embed_cache and cause population of
    # request._linked_uuids and request._rev_linked_uuids_by_item
    request._indexing_view = True
    # reset these properties
    request._audit_uuids = set()
    request._linked_uuids = {}
    request._rev_linked_uuids_by_item = {}
    request._aggregate_for['uuid'] = uuid
    request._aggregated_items = {
        agg: {'_fields': context.aggregated_items[agg], 'items': []} for agg in context.aggregated_items
    }
    # attempt to cache database sids related to indexing this item
    cache_linked_sids_from_db(context, request)

    # since request._indexing_view is set to True in indexer.py,
    # all embeds (including subrequests) below will use the embed cache
    embedded = request.invoke_view(path, '@@embedded', index_uuid=uuid)
    # get _linked and _rev_linked uuids from the request before @@audit views add to them
    linked_uuids = request._linked_uuids.copy()
    rev_linked_by_item = request._rev_linked_uuids_by_item.copy()
    # find uuids traversed that rev link to this item
    rev_linked_to_me = set([id for id in rev_linked_by_item if uuid in rev_linked_by_item[id]])
    # get the important information from the aggregated items
    aggregated_items = {agg: res['items'] for agg, res in request._aggregated_items.items()}
    # set the uuids we want to audit on
    request._audit_uuids = linked_uuids
    audit = request.invoke_view(path, '@@audit')['audit']
    obj = request.invoke_view(path, '@@object')
    document = {
        'aggregated_items': aggregated_items,
        'audit': audit,
        'embedded': embedded,
        'item_type': context.type_info.item_type,
        'linked_uuids': sorted(linked_uuids),
        'links': links,
        'object': obj,
        'paths': sorted(paths),
        'principals_allowed': principals_allowed,
        'properties': properties,
        'propsheets': {
            name: context.propsheets[name]
            for name in context.propsheets.keys() if name != ''
        },
        'sid': context.sid,
        'unique_keys': unique_keys,
        'uuid': uuid,
        'uuids_rev_linked_to_me': sorted(rev_linked_to_me)
    }

    return document
