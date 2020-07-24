from elasticsearch.helpers import scan
from ..interfaces import COLLECTIONS
from .interfaces import ELASTIC_SEARCH
from ..util import DEFAULT_EMBEDS


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


def to_camel_case(snake_string):
    return snake_string.title().replace("_", "")


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
                                    'linked_uuids_embedded.uuid': list(updated),
                                    '_cache': False,
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
    invalidated_with_type = {(res['_id'], to_camel_case(res['_type'])) for res in results}
    invalidated = {uuid for uuid, type in invalidated_with_type}

    return updated | invalidated, invalidated_with_type


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


def extract_type_properties(registry, invalidated_item_type):
    """ Helper function, useful for mocking. """
    return registry['types'][invalidated_item_type].schema['properties']


def extract_type_embedded_list(registry, invalidated_item_type):
    """ Helper function, useful for mocking """
    return registry['types'][invalidated_item_type].embedded_list


def filter_invalidation_scope(registry, diff, invalidated_with_type, secondary_uuids):
    """ Function that given a diff in the following format:
            ItemType.base_field.terminal_field --> {ItemType: base_field.terminal_field} intermediary
        And a list of invalidated uuids with their type information as a 2-tuple:
            [(<uuid>, <item_type>), ...]
        Removes uuids of item types that were not invalidated from the set of secondary_uuids.

    :param registry: application registry, used to retrieve type information
    :param diff: a diff of the change (from SQS), see build_diff_from_request
    :param invalidated_with_type: list of 2-tuple (uuid, item_type)
    :param secondary_uuids: primary set of uuids to be invalidated
    """
    # build representation of diffs
    # item type -> modified fields mapping
    diffs = {}
    skip = False  # if a modified field is a default embed, EVERYTHING has to be invalidated
    for _d in diff:
        modified_item_type, modified_field = _d.split('.', 1)
        if ('.' + modified_field) in DEFAULT_EMBEDS + ['.status']:  # XXX: 'status' is an unnamed default embed?
            skip = True
            break
        elif modified_item_type not in diffs:
            diffs[modified_item_type] = [modified_field]
        else:
            diffs[modified_item_type].append(modified_field)

    # go through all invalidated uuids, looking at the embedded list of the item type
    item_type_is_invalidated = {}
    for invalidated_uuid, invalidated_item_type in invalidated_with_type:
        if skip is True:  # if we detected a change to a default embed, invalidate everything
            break

        # remove this uuid if its item type has been seen before and found to
        # not be invalidated
        if invalidated_item_type in item_type_is_invalidated:
            if item_type_is_invalidated[invalidated_item_type] is False:
                secondary_uuids.discard(invalidated_uuid)
                continue

        # if we get here, we are looking at an invalidated_item_type that exists in the
        # diff and we need to inspect the embedded list to see if the diff fields are
        # embedded
        properties = extract_type_properties(registry, invalidated_item_type)
        embedded_list = extract_type_embedded_list(registry, invalidated_item_type)
        for embed in embedded_list:
            base_field, terminal_field = embed.split('.', 1)
            # resolve the item type of the base field by looking at the linkTo field first
            base_field_props = properties.get(base_field, {})
            if 'linkTo' in base_field_props:
                base_field_item_type = base_field_props['linkTo']
            elif 'linkTo' in base_field_props.get('items', {}):
                base_field_item_type = base_field_props['items']['linkTo']
            else:
                raise Exception("Encountered embed that is not a linkTo or array of linkTo's! \n"
                                "embed: %s, base_field: %s, base_field_props: %s" % (embed, base_field,
                                                                                     base_field_props))

            # XXX VERY IMPORTANT: for this to work correctly, the fields used in calculated properties MUST
            # be embedded! In addition, if you embed * on a linkTo, modifications to that linkTo will ALWAYS
            # invalidate the item_type
            if base_field in properties and \
                    (any(field.endswith(terminal_field) for field in diffs.get(base_field_item_type, [])) or
                     terminal_field.endswith('*')):
                item_type_is_invalidated[invalidated_item_type] = True
                break

        # if we didnt break out of the above loop, we never found an embedded field that was
        # touched, so set this item type to False so all items of this type are NOT invalidated
        if invalidated_item_type not in item_type_is_invalidated:
            secondary_uuids.discard(invalidated_uuid)
            item_type_is_invalidated[invalidated_item_type] = False
