import structlog
from elasticsearch.helpers import scan
from pyramid.view import view_config
from pyramid.exceptions import HTTPBadRequest
from ..interfaces import COLLECTIONS, TYPES
from .interfaces import ELASTIC_SEARCH
from ..util import DEFAULT_EMBEDS, crawl_schema, debug_log
from ..typeinfo import AbstractTypeInfo


log = structlog.getLogger(__name__)


def includeme(config):
    config.add_route('compute_invalidation_scope', '/compute_invalidation_scope')
    config.scan(__name__)


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
    results = scan(es, index=find_index, query=scan_query, size=10000)
    invalidated_with_type = {(res['_id'], to_camel_case(res['_type'])) for res in results}
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
        log.info('Tried to determine parent type of invalid type: %s' % item_type)
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
    :returns: 3-tuple:
                * skip bool (to invalidate everything),
                * diff intermediary
                * child -> parent type mappings (if they exist, in case we are modifying a leaf type)
    """
    # build representation of diffs
    # item type -> modified fields mapping
    diffs, child_to_parent_type = {}, {}
    skip = False  # if a modified field is a default embed, EVERYTHING has to be invalidated
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

    return skip, diffs, child_to_parent_type


def filter_invalidation_scope(registry, diff, invalidated_with_type, secondary_uuids, verbose=False):
    """ Function that given a diff in the following format:
            ItemType.base_field.terminal_field --> {ItemType: base_field.terminal_field} intermediary
        And a list of invalidated uuids with their type information as a 2-tuple:
            [(<uuid>, <item_type>), ...]
        Removes uuids of item types that were not invalidated from the set of secondary_uuids.

    :param registry: application registry, used to retrieve type information
    :param diff: a diff of the change (from SQS), see build_diff_from_request
    :param invalidated_with_type: list of 2-tuple (uuid, item_type)
    :param secondary_uuids: primary set of uuids to be invalidated
    :param verbose: specifies if we would like to return debugging info
    """
    skip, diffs, child_to_parent_type = build_diff_metadata(registry, diff)
    # go through all invalidated uuids, looking at the embedded list of the item type
    item_type_is_invalidated = {}
    for invalidated_uuid, invalidated_item_type in invalidated_with_type:
        if skip is True:  # if we detected a change to a default embed, invalidate everything

            # if in debug mode, populate invalidation metadata at the expense of performance
            if verbose:
                if invalidated_item_type not in item_type_is_invalidated:
                    item_type_is_invalidated[invalidated_item_type] = True
                continue
            else:  # in production, exit immediately if we see this, as this works by side-effect
                break

        # remove this uuid if its item type has been seen before and found to
        # not be invalidated
        if invalidated_item_type in item_type_is_invalidated:
            if item_type_is_invalidated[invalidated_item_type] is False:
                secondary_uuids.discard(invalidated_uuid)
            continue  # nothing else to do here

        # if we get here, we are looking at an invalidated_item_type that exists in the
        # diff and we need to inspect the embedded list to see if the diff fields are
        # embedded
        properties = extract_type_properties(registry, invalidated_item_type)
        embedded_list = extract_type_embedded_list(registry, invalidated_item_type)
        for embed in embedded_list:

            # check the field up to the embed as this is the path to the linkTo
            # we must determine it's type and determine if the given diff could've
            # resulted in an invalidation
            split_embed = embed.split('.')
            base_field, terminal_field = '.'.join(split_embed[0:-1]), split_embed[-1]
            base_field_schema = crawl_schema(registry['types'], base_field, properties)
            base_field_item_type = base_field_schema.get('linkTo', None)

            # recursive helper function that will drill down as much as necessary
            def locate_link_to(schema_cursor):
                if 'items' in schema_cursor:  # array
                    if 'properties' in schema_cursor['items']:
                        for field_name, details in schema_cursor['items']['properties'].items():
                            if base_field.endswith(field_name):
                                if 'linkTo' in details:
                                    return details['linkTo']
                                else:
                                    return locate_link_to(details)
                    else:
                        return schema_cursor['items']['linkTo']
                elif 'properties' in schema_cursor:  # object
                    for field_name, details in schema_cursor['properties'].items():
                        if base_field.endswith(field_name):
                            if 'linkTo' in details:
                                return details['linkTo']
                            else:
                                return locate_link_to(details)
                else:
                    log.error(schema_cursor)
                    raise Exception('Unexpected')

            # if we are not a top level linkTo, drill down
            if base_field_item_type is None:
                base_field_item_type = locate_link_to(base_field_schema)

            # Collect diffs from all possible item_types
            all_possible_diffs = diffs.get(base_field_item_type, [])

            # A linkTo target could be a child type (in that we need to look at parent type diffs as well)
            # NOTE: this situation doesn't actually occur in our system as of right now
            # but theoretically could
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

            # VERY IMPORTANT: for this to work correctly, the fields used in calculated properties MUST
            # be embedded! In addition, if you embed * on a linkTo, modifications to that linkTo will ALWAYS
            # invalidate the item_type
            if (any(terminal_field == field for field in all_possible_diffs) or
                    terminal_field.endswith('*')):
                item_type_is_invalidated[invalidated_item_type] = True
                break

        # if we didnt break out of the above loop, we never found an embedded field that was
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
    if verbose:  # noQA this function is intended to be considered 'void' but will return info if asked - Will
        return item_type_is_invalidated


def _compute_invalidation_scope_base(request, result, source_type, target_type, simulated_prop):
    """ Helper for below route - implements the base case of the API
        Builds a dummy diff from on the simulated prop and determines whether the edit results
        in invalidation of the target type.
    """

    dummy_diff = ['.'.join([source_type, simulated_prop])]
    invalidated_with_type = [('dummy', target_type)]
    invalidated_metadata = filter_invalidation_scope(request.registry, dummy_diff, invalidated_with_type, set(),
                                                     verbose=True)
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
    if type(request.registry[TYPES][source_type]) == AbstractTypeInfo or \
            type(request.registry[TYPES][target_type]) == AbstractTypeInfo:
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
