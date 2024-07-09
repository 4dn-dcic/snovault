from __future__ import unicode_literals

from contextlib import contextmanager
from dcicutils.misc_utils import ignored
from pyramid.traversal import resource_path
from pyramid.view import view_config
from pyramid.settings import asbool
from timeit import default_timer as timer

from .elasticsearch.indexer_utils import find_uuids_for_indexing
from .embed import make_subrequest
from .interfaces import STORAGE
from .resources import Item
from .util import debug_log
from .validation import ValidationFailure


def includeme(config):
    config.add_route('indexing-info', '/indexing-info')
    config.add_route('max-sid', '/max-sid')
    config.scan(__name__)


@contextmanager
def indexing_timer(timer_dict, time_key):
    """
    Simple contextmanager to time components of index-data

    Args:
        timer_dict (dict): dictionary to add timing results to
        time_key (str): key to use for the given timing result
    """
    start = timer()
    yield
    timer_dict[time_key] = timer() - start


def join_linked_uuids_sids(request, uuid_type_pairs):
    """
    Simply iterate through the uuid_type_pairs and return an array of dicts containing
    uuid, sid (from request._sid_cache) and the item type.

    Args:
        request: current Request object
        uuid_type_pairs: list of 2-tuples (uuid, item_type)

    Returns:
        A list of dicts containing uuid, up-to-date db sid and item type
    """
    return [
        {
            'uuid': uuid,
            'sid': request._sid_cache[uuid],
            'item_type': item_type,
        }
        for uuid, item_type in uuid_type_pairs
    ]


def get_rev_linked_items(request, uuid):
    """
    Iterate through request._rev_linked_uuids_by_item, which is populated
    during the embedding traversal process, to find the items that are reverse
    linked to the given uuid

    Args:
        request: current Request object
        uuid (str): uuid of the object in question

    Returns:
        A set of string uuids
    """
    # find uuids traversed that rev link to this item
    rev_linked_to_me = set()
    for rev_id, rev_names in request._rev_linked_uuids_by_item.items():
        if any([uuid in rev_names[name] for name in rev_names]):
            rev_linked_to_me.add(rev_id)
            continue
    return rev_linked_to_me


@view_config(context=Item, name='index-data', permission='index', request_method='GET')
@debug_log
def item_index_data(context, request):
    """
    Very important view which is used to calculate all the data indexed in ES
    for the given item. If an int sid is provided as a request parameter,
    will raise an sid exception if the maximum sid value on current_propsheets
    table is less than the given value.

    Computationally intensive. Calculates the object and embedded views
    for the given item, using ES results where possible for speed. Also handles
    calculation of aggregated-items and validation-errors for the item.
    Leverages a number of attrs on the request to get needed information

    Args:
        context: current Item
        request: current Request

    Returns:
        A dict document representing the full data to index for the given item
    """
    indexing_stats = {}  # hold timing details for this view

    uuid = str(context.uuid)

    # upgrade_properties calls necessary upgraders based on schema_version
    with indexing_timer(indexing_stats, 'upgrade_properties'):
        properties = context.upgrade_properties()

    # 2024-07-09: Make sure that the uuid gets into the frame=raw view.
    if not properties.get('uuid'):
        properties['uuid'] = uuid

    # ES versions 2 and up don't allow dots in links. Update these to use ~s
    new_links = {}
    for key, val in context.links(properties).items():
        new_links['~'.join(key.split('.'))] = val
    links = new_links

    principals_allowed = context.principals_allowed()
    path = resource_path(context)
    paths = {path}
    collection = context.collection

    with indexing_timer(indexing_stats, 'unique_keys'):
        unique_keys = context.unique_keys(properties)
        if collection.unique_key in unique_keys:
            paths.update(
                resource_path(collection, key)
                for key in unique_keys[collection.unique_key])

    with indexing_timer(indexing_stats, 'paths'):
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

    # run the object view first
    request._linked_uuids = set()
    with indexing_timer(indexing_stats, 'object_view'):
        object_view = request.invoke_view(path, '@@object')
    linked_uuids_object = request._linked_uuids.copy()
    rev_link_names = request._rev_linked_uuids_by_item.get(uuid, {}).copy()

    # reset these properties, then run embedded view
    request._linked_uuids = set()
    request._rev_linked_uuids_by_item = {}
    request._aggregate_for['uuid'] = uuid
    request._aggregated_items = {
        agg: {'_fields': context.aggregated_items[agg], 'items': []} for agg in context.aggregated_items
    }
    # since request._indexing_view is set to True in indexer.py,
    # all embeds (including subrequests) below will use the embed cache
    with indexing_timer(indexing_stats, 'embedded_view'):
        embedded_view = request.invoke_view(path, '@@embedded', index_uuid=uuid)

    # this is built since the embedded view is built on "item_with_links", see resources.py
    linked_uuids_embedded = request._linked_uuids.copy()

    # find uuids traversed that rev link to this item
    with indexing_timer(indexing_stats, 'rev_links'):
        rev_linked_to_me = get_rev_linked_items(request, uuid)

    # calculated aggregated items
    with indexing_timer(indexing_stats, 'aggregated_items'):
        aggregated_items = {agg: res['items'] for agg, res in
                            request._aggregated_items.items()}

    # run validators for the item by PATCHing with check_only=True
    # json_body provided is the upgraded properties of the item
    with indexing_timer(indexing_stats, 'validation'):
        validate_path = path + '?check_only=true'
        validate_req = make_subrequest(request, validate_path,
                                       json_body=properties)
        try:
            request.invoke_subrequest(validate_req)
        except ValidationFailure:
            # TODO: This should probably be logged. -kmp 22-Oct-2020
            pass

    document = {
        'aggregated_items': aggregated_items,
        'embedded': embedded_view,
        'indexing_stats': indexing_stats,
        'item_type': context.type_info.item_type,
        'linked_uuids_embedded': join_linked_uuids_sids(request, linked_uuids_embedded),
        'linked_uuids_object': join_linked_uuids_sids(request, linked_uuids_object),
        'links': links,
        'max_sid': context.max_sid,
        'object': object_view,
        'paths': sorted(paths),
        'principals_allowed': principals_allowed,
        'properties': properties,
        'propsheets': {
            name: context.propsheets[name]
            for name in context.propsheets.keys() if name != ''
        },
        'rev_link_names': rev_link_names,
        'rev_linked_to_me': sorted(rev_linked_to_me),
        'sid': context.sid,
        'unique_keys': unique_keys,
        'uuid': uuid,
        'validation_errors': validate_req.errors
    }

    return document


@view_config(route_name='indexing-info', permission='index', request_method='GET')
@debug_log
def indexing_info(context, request):
    """
    Endpoint to check some indexing-related properties of a given uuid, which
    is provided using the `uuid=` query parameter. This route cannot be defined
    with the context of a specific Item because that will cause the underlying
    request to use a cached view from Elasticsearch and not properly run
    the @@embedded view from the database.

    If you do not want to calculate the embedded object, use `run=False`

    Args:
        context: ignored
        request: current Request object

    Returns:
        dict response
    """
    ignored(context)
    uuid = request.params.get('uuid')
    if not uuid:
        return {'status': 'error', 'title': 'Error', 'message': 'ERROR! Provide a uuid to the query.'}

    db_sid = request.registry[STORAGE].write.get_by_uuid(uuid).sid
    # es_model will be None if the item is not yet indexed
    es_model = request.registry[STORAGE].read.get_by_uuid(uuid)
    es_sid = es_model.sid if es_model is not None else None
    response = {'sid_db': db_sid, 'sid_es': es_sid, 'title': 'Indexing Info for %s' % uuid}
    if asbool(request.params.get('run', True)):
        request._indexing_view = True
        request.datastore = 'database'
        path = '/' + uuid + '/@@index-data'
        index_view = request.invoke_view(path, index_uuid=uuid, as_user='INDEXER')
        response['indexing_stats'] = index_view['indexing_stats']
        # since there is no diff, we cannot compute invalidation scope here.
        es_assc_uuids, _ = find_uuids_for_indexing(request.registry, {uuid})
        new_rev_link_uuids = get_rev_linked_items(request, uuid)
        # invalidated: items linking to this in es + newly rev linked items
        response['uuids_invalidated'] = list(es_assc_uuids | new_rev_link_uuids)
        response['description'] = f'Using live results for embedded view of {uuid}. Query with run=False to skip this.'
    else:
        response['description'] = (f'Query with run=True to calculate live information on invalidation'
                                   f' and embedding time.')
    response['display_title'] = 'Indexing Info for %s' % uuid
    response['status'] = 'success'
    return response


@view_config(route_name='max-sid', permission='index', request_method='GET')
@debug_log
def max_sid(context, request):
    """
    Very simple endpoint to return the current maximum sid used in postgres.
    Might make more sense to define this view in storage.py, but leave it here
    with the other sid/indexing related code.

    Args:
        context: ignored
        request: current Request object

    Returns:
        dict response
    """
    ignored(context)
    response = {'display_title': 'Current maximum database sid'}
    try:
        max_sid = request.registry[STORAGE].write.get_max_sid()
        response.update({'status': 'success', 'max_sid': max_sid})
    except Exception as exc:
        response.update({'status': 'failure', 'detail': str(exc)})
    return response
