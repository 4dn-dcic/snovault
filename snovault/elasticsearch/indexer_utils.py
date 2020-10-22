from elasticsearch.helpers import scan
from ..interfaces import COLLECTIONS
from .interfaces import ELASTIC_SEARCH


def get_namespaced_index(config, index):
    """ Grabs indexer.namespace from settings and namespace the given index """
    try:
        settings = config.registry.settings
    except:  # accept either config or registry as first arg
        settings = config.settings
    namespace = settings.get('indexer.namespace') or ''
    return namespace + index


def namespace_index_from_health(health, index):
    """ Namespaces the given index based on health page data """
    if 'error' in health:
        raise RuntimeError('Mirror health unresolved: %s' % health)
    return health.get('namespace', '') + index


def find_uuids_for_indexing(registry, updated, find_index=None):
    """
    Run a search to find uuids of objects with that contain the given set of
    updated uuids in their linked_uuids.
    Uses elasticsearch.helpers.scan to iterate through ES results.
    Returns a set containing original uuids and the found uuids (INCLUDING
    uuids that were passed into this function)

    Args:
        registry: the current Registry
        updated (set): uuids to use as basis for finding associated items
        find_index (str): index to search in. Default to '_all' (all indices)

    Return:
        set: of uuids, including associated uuids found AND `updated` uuids
    """
    es = registry[ELASTIC_SEARCH]
    scan_query = {
        'query': {
            'bool': {
                'filter': {
                    'bool': {
                        'should': [
                            {
                                'terms': {
                                    'linked_uuids_embedded.uuid': list(updated)
                                }
                            }
                        ]
                    }
                }
            }
        },
        '_source': False
    }
    if not find_index:
        find_index = get_namespaced_index(registry, '*')
    results = scan(es, index=find_index, query=scan_query)
    invalidated = {res['_id'] for res in results}
    return invalidated | updated


def get_uuids_for_types(registry, types=[]):
    """
    WARNING! This makes lots of DB requests and should be used carefully.

    Generator function to return uuids for all the given types. If no
    types provided, uses all types (get all uuids). Because of inheritance
    between item classes, do not iterate over all subtypes (as is done with
    `for uuid in collection`; instead, leverage `collection.iter_no_subtypes`)

    Args:
        registry: the current Registry
        types (list): string item types to specifcally use to find collections.
            Default is empty list, which means all collections are used

    Yields:
        str: uuid of item in collections
    """
    if not isinstance(types, list) or not all(isinstance(t, str) for t in types):  # type check for safety
        raise TypeError('Expected type=list (of strings) for argument "types"')
    collections = registry[COLLECTIONS]
    # might as well sort collections alphatbetically, as this was done earlier
    for coll_name in sorted(collections.by_item_type):
        if types and coll_name not in types:
            continue
        for uuid in collections.by_item_type[coll_name].iter_no_subtypes():
            yield str(uuid)
