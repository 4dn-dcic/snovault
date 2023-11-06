import structlog
from elasticsearch.helpers import scan
from pyramid.view import view_config
from pyramid.exceptions import HTTPBadRequest
from ..interfaces import COLLECTIONS, TYPES
from .interfaces import ELASTIC_SEARCH
from ..util import DEFAULT_EMBEDS, crawl_schema, debug_log
from ..typeinfo import AbstractTypeInfo


log = structlog.getLogger(__name__)
SCAN_PAGE_SIZE = 5000


def includeme(config):
    config.add_route('compute_invalidation_scope', '/compute_invalidation_scope')
    config.scan(__name__)


def get_namespaced_index(config, index):
    """ Grabs indexer.namespace from settings and namespace the given index """
    try:
        settings = config.registry.settings
    except AttributeError:  # accept either config or registry as first arg
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
                                    'linked_uuids_embedded.uuid': list(updated)
                                }
                            }
                        ]
                    }
                }
            }
        },
        '_source': {  # in ES7 there is no _type included by default, so inspect embedded.@type
            'includes':
                'item_type'
        }
    }
    if not find_index:
        find_index = get_namespaced_index(registry, '*')
    # size param below == # of results per request, too large and can timeout, too small and will make too
    # many requests - 5000 seems to be a reasonable number. - Will 6/7/21
    results = scan(es, index=find_index, query=scan_query, size=SCAN_PAGE_SIZE)
    invalidated_with_type = {(res['_id'], to_camel_case(res['_source']['item_type'])) for res in results}
    invalidated = {uuid for uuid, _type in invalidated_with_type}

    return (updated | invalidated), invalidated_with_type


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
    # might as well sort collections alphabetically, as this was done earlier
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


def extract_type_default_diff(registry, invalidated_item_type):
    """ Helper function that extracts the default diff for this item, if one exists. """
    return getattr(registry['types'][invalidated_item_type], 'default_diff', [])


def extract_base_types(registry, item_type):
    """ Helper function, useful for mocking """
    return registry[TYPES][item_type].base_types


def determine_parent_types(registry, item_type):
    """ Determines the parent types of the given item_type """
    base_types = []
    try:
        base_types = extract_base_types(registry, item_type)
    except KeyError:  # indicative of an error if not testing
        log.info(f'Tried to determine parent type of invalid type: {item_type}')
    return [b for b in base_types if b != 'Item']


def determine_child_types(registry, parent_type):
    """ Determines the child types of the given parent type (to a depth of one). """
    child_types = []
    for potential_child_type, details in registry[TYPES].by_item_type.items():
        if parent_type in getattr(details, 'base_types', []):
            child_types.append(details.name)
    return child_types


def build_diff_metadata(registry, diff):
    """ Helper function for below that builds metadata from diff needed to filter
        invalidation scope.

    :param registry: application registry, used to retrieve type information
    :param diff: a diff of the change (from SQS), see build_diff_from_request
    :returns: 4-tuple:
                * skip bool (to invalidate everything),
                * diff intermediary
                * type that the diff modifies
                * child -> parent type mappings (if they exist, in case we are modifying a leaf type)
    """
    # build representation of diffs
    # item type -> modified fields mapping
    diffs, child_to_parent_type = {}, {}
    skip = False  # if a modified field is a default embed, EVERYTHING has to be invalidated
    modified_item_type = None  # this will get set to the last value in the diff, if any
    for _d in diff:
        modified_item_type, modified_field = _d.split('.', 1)
        if ('.' + modified_field) in DEFAULT_EMBEDS:
            skip = True
            break
        if modified_item_type not in diffs:
            diffs[modified_item_type] = [modified_field]
        else:
            diffs[modified_item_type].append(modified_field)

        default_diff = extract_type_default_diff(registry, modified_item_type)
        if default_diff:
            diffs[modified_item_type].extend(default_diff)

        modified_item_parent_types = determine_parent_types(registry, modified_item_type)
        if modified_item_parent_types:
            child_to_parent_type[modified_item_type] = modified_item_parent_types

    return skip, diffs, modified_item_type, child_to_parent_type


def filter_invalidation_scope(registry, diff, invalidated_with_type, secondary_uuids):
    """ Function that given a diff in the following format:
            ItemType.base_field.terminal_field --> {ItemType: base_field.terminal_field} intermediary
        And a list of invalidated uuids with their type information as a 2-tuple:
            [(<uuid>, <item_type>), ...]
        Removes uuids of item types that were not invalidated from the set of secondary_uuids.

        NOTE: The core logic of invalidation scope occurs in this function.
        The algorithm proceeds roughly as follows:
            for invalidated_item_type:
                for each embed in the invalidated_item_type's embedded list:
                    if this embed does not form a link to the diff target:
                        keep searching
                    else (it forms a candidate link):
                        determine all possible diffs for the valid types
                        if any part of the diff is embedded directly through a terminal field or via *:
                            mark this type as invalidated, cache type result to avoid re-computation
                        else:
                            keep searching embeds
                if we reach this point and have not invalidated, the type is cleared

    :param registry: application registry, used to retrieve type information
    :param diff: a diff of the change (from SQS), see build_diff_from_request
    :param invalidated_with_type: list of 2-tuple (uuid, item_type)
    :param secondary_uuids: primary set of uuids to be invalidated
    :returns: dictionary mapping types to a boolean on whether or not the type is invalidated
    """
    skip, diffs, diff_type, child_to_parent_type = build_diff_metadata(registry, diff)
    valid_diff_types = child_to_parent_type.get(diff_type, []) + [diff_type]
    # go through all invalidated uuids, looking at the embedded list of the item type
    item_type_is_invalidated = {}
    for invalidated_uuid, invalidated_item_type in invalidated_with_type:
        if skip is True:  # if we detected a change to a default embed, invalidate everything

            if invalidated_item_type not in item_type_is_invalidated:
                item_type_is_invalidated[invalidated_item_type] = True
            continue

        # remove this uuid if its item type has been seen before and found to
        # not be invalidated
        if invalidated_item_type in item_type_is_invalidated:
            if item_type_is_invalidated[invalidated_item_type] is False:
                secondary_uuids.discard(invalidated_uuid)
            continue  # nothing else to do here

        # if we get here, we are looking at an invalidated_item_type that exists in the
        # diff, and we need to inspect the embedded list to see if the diff fields are
        # embedded
        properties = extract_type_properties(registry, invalidated_item_type)
        embedded_list = extract_type_embedded_list(registry, invalidated_item_type)

        for embed in embedded_list:

            # check the field up to the embed as this is the path to the linkTo
            # we must determine its type and determine if the given diff could've
            # resulted in an invalidation
            split_embed = embed.split('.')
            link_depth, base_field_item_type = 0, None

            # Checks that schema_part is a linkTo and it is of type item_type
            def is_matched_linkto(schema_part, item_types):
                if 'linkTo' in schema_part and schema_part['linkTo'] in item_types:
                    return True
                return False

            # crawl schema by each increasingly shorter embed searching for where
            # the last link target ends and the terminal field begins
            for i in range(len(split_embed), 0, -1):
                embed_path = '.'.join(split_embed[0:i])
                if embed_path.endswith('*'):
                    continue
                embed_part_schema = crawl_schema(registry['types'], embed_path, properties)
                if is_matched_linkto(embed_part_schema, valid_diff_types):
                    base_field_item_type = embed_part_schema.get('linkTo', None)
                    link_depth = i
                    break
                elif 'items' in embed_part_schema:
                    array = embed_part_schema['items']
                    if is_matched_linkto(array, valid_diff_types):
                        base_field_item_type = array.get('linkTo')
                        link_depth = i
                        break

            # if we did not find a linkTo, this diff is not represented in the current embed
            # so continue looking through the rest of the embeds
            if base_field_item_type is None:
                continue

            # grab terminal field based on the actual linkTo depth
            terminal_field = '.'.join(split_embed[link_depth:])

            # Collect diffs from all possible item_types
            all_possible_diffs = diffs.get(base_field_item_type, [])

            # A linkTo target could be a child type (in that we need to look at parent type diffs as well)
            parent_types = child_to_parent_type.get(base_field_item_type, None)
            if parent_types is not None:
                for parent_type in child_to_parent_type.get(base_field_item_type, []):
                    all_possible_diffs.extend(diffs.get(parent_type, []))

            # It could also be parent type (in that we must look at all potential child types)
            child_types = determine_child_types(registry, base_field_item_type)
            if child_types is not None:
                for child_type in determine_child_types(registry, base_field_item_type) or []:
                    all_possible_diffs.extend(diffs.get(child_type, []))

            if not all_possible_diffs:  # no diffs match this embed
                continue

            # Checks that the 'field' passed is actually contained in the embed list
            def field_is_part_of_target_embed(field):
                terminal_field_as_list = terminal_field.split('.')
                for field_part in field.split('.'):
                    if field_part in terminal_field_as_list:
                        return True
                return False

            # VERY IMPORTANT: for this to work correctly, the fields used in calculated properties MUST
            # be embedded! In addition, if you embed * on a linkTo, modifications to that linkTo will ALWAYS
            # invalidate the item_type
            for field in all_possible_diffs:
                # if terminal field matches a diff exactly, invalidate
                if terminal_field == field:
                    log.info(f'Invalidating item type {invalidated_item_type} based on edit to field {field} given exact'
                             f'embed {terminal_field}')
                # if terminal field is *, invalidate
                elif terminal_field == '*' or (terminal_field.endswith('*') and field_is_part_of_target_embed(field)):
                    log.info(f'Invalidating item type {invalidated_item_type} for field {field} based on star embed {split_embed}')
                # if we edited a link itself or any field on the path, invalidate
                elif field in split_embed:
                    log.info(f'Invalidating item type {invalidated_item_type} based on edit to field {field} given embed'
                             f' path {split_embed}')
                else:
                    log.info(f'Skipping field {field} as {split_embed} does not match')
                    continue
                item_type_is_invalidated[invalidated_item_type] = True
                break

            if item_type_is_invalidated.get(invalidated_item_type):
                break  # if found we don't need to continue searching

        # if we didn't break out of the above loop, we never found an embedded field that was
        # touched, so set this item type to False so all items of this type are NOT invalidated
        if invalidated_item_type not in item_type_is_invalidated:
            secondary_uuids.discard(invalidated_uuid)
            item_type_is_invalidated[invalidated_item_type] = False

    # XXX: Enable to get debugging information on invalidation scope
    # def _sort(tp):
    #     return tp[0]
    # log.error('Diff: %s Invalidated: %s Cleared: %s' % (diffs, sorted(list((k, v) for k, v in item_type_is_invalidated.items()
    #                                                                        if v is True), key=_sort),
    #                                                            sorted(list((k, v) for k, v in item_type_is_invalidated.items()
    #                                                                        if v is False), key=_sort)))
    return item_type_is_invalidated


def _compute_invalidation_scope_base(request, result, source_type, target_type, simulated_prop):
    """ Helper for below route - implements the base case of the API
        Builds a dummy diff from on the simulated prop and determines whether the edit results
        in invalidation of the target type.
    """

    dummy_diff = ['.'.join([source_type, simulated_prop])]
    invalidated_with_type = [('dummy', target_type)]
    invalidated_metadata = filter_invalidation_scope(request.registry, dummy_diff, invalidated_with_type, set())
    if invalidated_metadata.get(target_type, False):
        result['Invalidated'].append(simulated_prop)
    else:
        result['Cleared'].append(simulated_prop)


def _compute_invalidation_scope_recursive(request, result, meta, source_type, target_type, simulated_prop):
    """ Helper for below route - implements the recursive step of the API.
        Traverses the properties computing invalidation scope for all possible patch paths.
    """
    if 'calculatedProperty' in meta:  # we cannot patch calc props, so behavior here is irrelevant
        return
    elif meta['type'] == 'object':
        if 'properties' not in meta:
            return  # sometimes can occur (see workflow.json in fourfront) - nothing we can do
        for sub_prop, sub_meta in meta['properties'].items():
            _compute_invalidation_scope_recursive(request, result, sub_meta, source_type, target_type,
                                                  '.'.join([simulated_prop, sub_prop]))
    elif meta['type'] == 'array':
        sub_type = meta['items']['type']
        if sub_type == 'object':
            if 'properties' not in meta['items']:
                return  # sometimes can occur (see workflow.json in fourfront) - nothing we can do
            for sub_prop, sub_meta in meta['items']['properties'].items():
                _compute_invalidation_scope_recursive(request, result, sub_meta, source_type, target_type,
                                                      '.'.join([simulated_prop, sub_prop]))
        else:
            _compute_invalidation_scope_base(request, result, source_type, target_type, simulated_prop)
    else:
        _compute_invalidation_scope_base(request, result, source_type, target_type, simulated_prop)


@view_config(route_name='compute_invalidation_scope', request_method='POST', permission='index')
@debug_log
def compute_invalidation_scope(context, request):
    """ Computes invalidation scope for a given source item type against a target item type.
        Arguments:
            source_type: item type whose edits we'd like to investigate
            target_type: "impacted" type ie: assume this type was invalidated
        Response:
            source/target type are given back
            Invalidated: list of fields on source_type that, if modified, trigger invalidation of target_type
            Cleared: list of fields on source_type that, if modified, do not trigger invalidation of target_type
    """
    source_type = request.json.get('source_type', None)
    target_type = request.json.get('target_type', None)
    # None-check
    if not source_type or not target_type:
        raise HTTPBadRequest('Missing required parameters: source_type, target_type')
    # Invalid Type
    if source_type not in request.registry[TYPES] or target_type not in request.registry[TYPES]:
        raise HTTPBadRequest('Invalid source/target type: %s/%s' % (source_type, target_type))
    # Abstract type
    # Note 'type' is desired here because concrete types have literal type TypeInfo
    # vs. abstract types have literal type AbstractTypeInfo
    # isinstance() will return True (wrong) since TypeInfo inherits from AbstractTypeInfo
    if type(request.registry[TYPES][source_type]) is AbstractTypeInfo or \
            type(request.registry[TYPES][target_type]) is AbstractTypeInfo:
        raise HTTPBadRequest('One or more of your types is abstract! %s/%s' % (source_type, target_type))
    source_type_schema = request.registry[TYPES][source_type].schema
    result = {
        'Source Type': source_type,
        'Target Type': target_type,
        'Invalidated': [],
        'Cleared': []
    }

    # Walk schema, simulating an edit and computing invalidation scope per field, recording result
    for prop, meta in source_type_schema['properties'].items():
        _compute_invalidation_scope_recursive(request, result, meta, source_type, target_type, prop)

    return result
