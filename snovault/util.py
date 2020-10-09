import contextlib
import functools
import json
import sys
from copy import copy
from datetime import datetime, timedelta

import structlog
from past.builtins import basestring
from pyramid.httpexceptions import HTTPForbidden
from pyramid.threadlocal import manager as threadlocal_manager

from .interfaces import CONNECTION, STORAGE, TYPES
from .settings import Settings

log = structlog.getLogger(__name__)


###################
# Misc. utilities #
###################


@contextlib.contextmanager
def mappings_use_nested(value=True):
    """ Context manager that sets the MAPPINGS_USE_NESTED setting with the given value, default True """
    old_setting = Settings.MAPPINGS_USE_NESTED
    try:
        Settings.MAPPINGS_USE_NESTED = value
        yield
    finally:
        Settings.MAPPINGS_USE_NESTED = old_setting


class DictionaryKeyError(KeyError):

    def __init__(self, dictionary, key):
        super(DictionaryKeyError, self).__init__(key)
        self._dictionary = dictionary
        self._dictionary_key = key

    def __str__(self):
        if isinstance(self._dictionary, dict):
            return "%r has no %r key." % (self._dictionary, self._dictionary_key)
        else:
            return "%r is not a dictionary." % self._dictionary


def dictionary_lookup(dictionary, key):
    """
    dictionary_lookup(d, k) is the same as d[k] but with more informative error reporting.
    """
    if not isinstance(dictionary, dict) or (key not in dictionary):
        log.error('Got dictionary KeyError with %s and %s' % (dictionary, key))
        return None
        #raise DictionaryKeyError(dictionary=dictionary, key=key)  this causes MPIndexer exception - will 3/10/2020
    else:
        return dictionary[key]


_skip_fields = ['@type', 'principals_allowed']  # globally accessible if need be in the future


def filter_embedded(embedded, effective_principals):
    """
    Filter the embedded items by principals_allowed, replacing them with
    a 'no view allowed' error message if the effective principals on the
    request are disjointed
    """
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


def debug_log(func):
    """ Decorator that adds some debug output of the view to log that we got there """
    @functools.wraps(func)
    def log_decorator(*args, **kwargs):
        log_function_call(log, func.__name__)
        if not args:
            return func(**kwargs)
        elif not kwargs:
            return func(*args)
        return func(*args, **kwargs)
    return log_decorator


def log_function_call(log_ref, func_name, extra=None):
    """
    Logs that we have reached func_name in the application
    Can log 'extra' information as well if specified
    Helpful in debugging 500 errors on routes and logging entry to any particular function
    """
    log_ref.info('DEBUG_FUNC -- Entering view config: %s' % func_name)
    if extra:
        log_ref.info('DEBUG_FUNC -- Extra info: %s' % extra)


def select_distinct_values(request, value_path, *from_paths):
    if isinstance(value_path, basestring):
        value_path = value_path.split('.')

    values = from_paths
    for name in value_path:
        objs = (request.embed(member, '@@object') for member in values)
        value_lists = (ensurelist(obj.get(name, [])) for obj in objs)
        values = {value for value_list in value_lists for value in value_list}

    return list(values)


def includeme(config):
    config.add_request_method(select_distinct_values)


def get_root_request():
    if threadlocal_manager.stack:
        return threadlocal_manager.stack[0]['request']


def ensurelist(value):
    if isinstance(value, basestring):
        return [value]
    return value


def uuid_to_path(request, obj, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if remaining:
        if isinstance(value, list):
            for v in value:
                uuid_to_path(request, v, remaining)
        else:
            uuid_to_path(request, value, remaining)
        return
    conn = request.registry[CONNECTION]
    if isinstance(value, list):
        obj[name] = [
            request.resource_path(conn[v])
            for v in value
        ]
    else:
        obj[name] = request.resource_path(conn[value])


def simple_path_ids(obj, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        yield obj
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if not isinstance(value, list):
        value = [value]
    for member in value:
        for result in simple_path_ids(member, remaining):
            yield result


def expand_path(request, obj, path):
    """
    Used with ?expand=... view. See resource_views.item_view_expand
    """
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if isinstance(value, list):
        for index, member in enumerate(value):
            if not isinstance(member, dict):
                res = secure_embed(request, member, '@@object')
                member = value[index] = res
            expand_path(request, member, remaining)
    else:
        if not isinstance(value, dict):
            res = secure_embed(request, value, '@@object')
            value = obj[name] = res
        expand_path(request, value, remaining)


def find_collection_subtypes(registry, item_type, types_covered=None):
    """
    Given an item type (or item class name), find all subtypes for that type
    and return a list containing all of them. types_covered is meant to be
    used internally, but adding a item type to it will cause it to be removed
    from the returned output

    Args:
        registry: the current Registry
        item_type (str): item type (or item class name) to find subtypes for
        types_covered (list): used internally to track covered types

    Returns:
        list: all item types found when traversing substypes
    """
    types_found = []
    if types_covered is None:
        types_covered = []  # initialize
    try:
        # this works for item name (MyItem) and item type (my_name)
        registry_type = registry[TYPES][item_type]
    except KeyError:
        return [] # no types found
    # add the item_type of this collection if applicable
    if hasattr(registry_type, 'item_type'):
        if registry_type.name not in types_covered:
            types_found.append(registry_type.item_type)
        types_covered.append(registry_type.name)
    # subtypes are given by name and include the registry_type.name itself
    if hasattr(registry_type, 'subtypes'):
        subtypes = registry_type.subtypes
        for subtype in subtypes:
            if subtype not in types_covered:
                types_found.extend(
                    find_collection_subtypes(registry, subtype, types_covered)
                )
    return types_found


def crawl_schema(types, field_path, schema_cursor, split_path=None):
    """
    Given a field_path that is a sequence of fields joined by '.' and a starting
    schema, will recursively drill down into the schema to find the schema value
    of the terminal field. Will raise an Exception if the field cannot be found
    Args:
        types: Result of registry[TYPES].
        field_path: string field path, joined by '.'
        schema_cursor: dictionary schema starting point
        split_path: array of remaining fields to traverse. Used internally

    Returns:
        Dictionary schema for the terminal field in field_path
    """
    # true if we are just starting up
    if split_path is None:
        # ensure input schema is a dictionary
        if not isinstance(schema_cursor, dict):
            raise Exception('Could not find schema field for: %s. Invalid starting schema.' % field_path)

        # drill into 'properties' of initial schema
        if 'properties' in schema_cursor:
            schema_cursor = schema_cursor['properties']
        split_path = field_path.split('.')

    curr_field = split_path[0]
    schema_cursor = schema_cursor.get(curr_field)
    if not schema_cursor:
        raise Exception('Could not find schema field for: %s. Field not found. Failed at: %s' % (field_path, curr_field))

    # schema_cursor should always be a dictionary
    if not isinstance(schema_cursor, dict):
        raise Exception('Could not find schema field for: %s. Non-dictionary schema. Failed at: %s' % (field_path, curr_field))

    ## base case. We have found the desired schema
    if len(split_path) == 1:
        return schema_cursor

    # drill into 'items' or 'properties'. always check 'items' before 'properties'
    # check if an array + drill into if so
    if schema_cursor.get('type') == 'array' and 'items' in schema_cursor:
        schema_cursor = schema_cursor['items']
    # check if an object + drill into if so
    if schema_cursor.get('type') == 'object' and 'properties' in schema_cursor:
        schema_cursor = schema_cursor['properties']
    # if we hit a linkTo, pull in the new schema of the linkTo type
    if 'linkTo' in schema_cursor:
        linkTo = schema_cursor['linkTo']
        try:
            linkTo_type = types.all[linkTo]
        except KeyError:
            raise Exception('Could not find schema field for: %s. Invalid linkTo. Failed at: %s' % (field_path, curr_field))
        linkTo_schema = linkTo_type.schema
        schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema

    return crawl_schema(types, field_path, schema_cursor, split_path[1:])


##########################################
# Embedding / aggregated_items utilities #
##########################################


# Terminal fields that are added to the embedded list for every embedded item
DEFAULT_EMBEDS = ['.@id', '.@type', '.display_title', '.uuid', '.principals_allowed.*']


def secure_embed(request, item_path, addition='@@object'):
    """
    Make a call to embed() with the given item path and user status
    Handles substituting a no view permissions message if a the given
    request does not have permission to see the object
    """
    res = {'error': 'no view permissions'}
    try:
        # if empty item_path reqeust.embed returns just addition as a string
        if item_path:
            res = request.embed(str(item_path), addition, as_user=True)
        else:
            res = ''
        return res
    except HTTPForbidden:
        print("you don't have access to this object")

    return res


def expand_embedded_model(request, obj, model, parent_path='', embedded_path=None):
    """
    A similar idea to expand_path, but takes in model from build_embedded_model
    instead. Takes in the @@object view of the item (obj) and returns a
    fully embedded result.
    This is also used recursively to handle dictionaries encountered during
    this process.
    parent_path and embedded_path are passed in for aggregated_items tracking.

    Args:
        request: current Request
        obj (dict): item to expand the embedded model on
        model (dict): model for embedding from build_embedded_model
        parent_path (str): resource path of the parent linkTo item encountered
            while embedding. If no external items embedded, is an empty string
            Used for aggregated items
        embedded_path (list): field names of all embedded fields traversed so
            so far in the model. Used for aggregated_items

    Returns:
        dict: embedded result
    """
    embedded_res = {}
    if embedded_path is None:
        embedded_path = []  # initialize
    # first take care of the fields_to_use at this level; get them from obj
    fields_to_use = model.get('fields_to_use')
    if fields_to_use:
        if '*' in fields_to_use:
            embedded_res = obj
        else:
            for field in fields_to_use:
                found = obj.get(field)
                if found is not None:
                    embedded_res[field] = found
    # then handle objects at the next level
    for to_embed in model:
        if to_embed == 'fields_to_use':
            continue
        obj_val = obj.get(to_embed)
        if obj_val is None:
            continue
        # branch embedded path for each field to embed
        this_embedded_path = embedded_path.copy()
        # pass to_embed (field name) to track aggregated_items
        obj_embedded = expand_val_for_embedded_model(request, obj_val,
                                                     model[to_embed],
                                                     to_embed, parent_path,
                                                     this_embedded_path)
        if obj_embedded is not None:
            embedded_res[to_embed] = obj_embedded
    return embedded_res


def expand_val_for_embedded_model(request, obj_val, downstream_model, field_name='',
                                  parent_path='', embedded_path=None):
    """
    Take a value from an object and the relevant piece of the embedded_model
    and perform embedding.
    We have to account for lists, dictionaries, linkTos, and other values:
        - lists: process each entry separately and join them. embedded_path
            is branched for each entry
        - dicts: run expand_embedded_model on the dict using the downstream
            embedded model. Record in aggregated_items if necessary
        - linkTo: attempt to get frame=object for item, taking permissions into
            account. Record in aggregated_items if necessary
        - other values: return them
    field_name/parent_path are optional and used to track aggregated_items.
    embedded_path is used to track the levels of embedding we've traversed
    and is updated whenever a dict/linkTo is encountered

    Args:
        request: current Request
        obj_val: value of the embedded field from the previous model
        downstream_model (dict): model for downstream embedding, originally
            from build_embedded_model
        field_name (str): name of the current field being embedded. Used for
            aggregated items
        parent_path (str): resource path of the parent linkTo item encountered
            while embedding. If no external items embedded, is an empty string
            Used for aggregated items
        embedded_path (list): field names of all embedded fields traversed so
            so far in the model. Used for aggregated_items

    Returns:
        The processed embed from the given obj_val and downstream_model
    """
    agg_items = request._aggregated_items
    # if the value is a list, process each value sequentially
    # we are not actually progressing down the embedded model yet
    if isinstance(obj_val, list):
        obj_list = []
        for idx, member in enumerate(obj_val):
            # branch embedded_path for each item in list
            this_embedded_path = embedded_path.copy()
            # lists conserve field name and their order
            obj_embedded = expand_val_for_embedded_model(request, member,
                                                         downstream_model,
                                                         field_name=field_name,
                                                         parent_path=parent_path,
                                                         embedded_path=this_embedded_path)
            if obj_embedded is not None:
                obj_list.append(obj_embedded)
        return obj_list
    else:
        # for dict/linkTo/other values, we are progressing down the embed
        embedded_path.append(field_name)

    if isinstance(obj_val, dict):
        obj_embedded = expand_embedded_model(request, obj_val, downstream_model,
                                             parent_path=parent_path, embedded_path=embedded_path)
        # aggregate the item if applicable
        if field_name and parent_path and field_name in agg_items:
            agg_emb_path = '.'.join(embedded_path)
            new_agg = {'parent': parent_path, 'embedded_path': agg_emb_path, 'item': obj_embedded}
            agg_items[field_name]['items'].append(new_agg)
        return obj_embedded
    elif isinstance(obj_val, basestring):
        # get the @@object view of obj to embed
        # TODO: per-field invalidation by adding uuids to request._linked_uuids
        # ONLY if the field is used in downstream_model (i.e. in embedded_list)
        obj_val = secure_embed(request, obj_val, '@@object')
        if not obj_val or obj_val == {'error': 'no view permissions'}:
            return obj_val

        # aggregate the item if applicable
        if field_name and parent_path and field_name in agg_items:
            agg_emb_path = '.'.join(embedded_path)
            # we may need to merge the values with existing ones
            new_agg = {'parent': parent_path, 'embedded_path': agg_emb_path, 'item': obj_val}
            agg_items[field_name]['items'].append(new_agg)

        # track the new parent object if we are indexing
        new_parent_path = obj_val.get('@id') if request._indexing_view else None
        obj_embedded = expand_embedded_model(request, obj_val, downstream_model,
                                             parent_path=new_parent_path,
                                             embedded_path=embedded_path)
        return obj_embedded
    else:
        # this means the object should be returned as-is
        return obj_val



def build_embedded_model(fields_to_embed):
    """
    Takes a list of fields to embed and builds the framework used to generate
    the fully embedded result. 'fields_to_use' refer to specific fields that are to
    be embedded within an object. The base level object gets a special flag,
    '*', which means all non-object fields are embedded by default.
    Below is an example calculated from the following fields:
    INPUT:
    [modifications.modified_regions.chromosome,
    lab.uuid,
    award.*,
    biosource.name]
    OUTPUT:
    {'modifications': {'modified_regions': {'fields_to_use': ['chromosome']}},
     'lab': {'fields_to_use': ['uuid']},
     'award': {'fields_to_use': ['*']},
     'biosource': {'fields_to_use': ['name']},
     'fields_to_use': ['*']}
    """
    embedded_model = {'fields_to_use':['*']}
    for field in fields_to_embed:
        split_field = field.split('.')
        field_pointer = embedded_model
        for idx, subfield in enumerate(split_field):
            if idx == len(split_field) - 1:  # terminal field
                if 'fields_to_use' in field_pointer:
                    field_pointer['fields_to_use'].append(subfield)
                else:
                    field_pointer['fields_to_use'] = [subfield]
                continue
            elif subfield not in field_pointer:
                field_pointer[subfield] = {}
            field_pointer = field_pointer[subfield]
    return embedded_model


def add_default_embeds(item_type, types, embeds, schema={}):
    """
    Perform default processing on the embedded_list of an item_type.
    Three part process that automatically builds a list of embed paths using
    the embedded_list (embeds parameter), expanding all the top level linkTos,
    and then finally adding the default embeds to all the linkTo paths generated.
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    # remove duplicate embeds
    embeds = list(set(list(embeds)))
    embeds.sort()
    if 'properties' in schema:
        schema = schema['properties']
    processed_embeds = set(embeds[:]) if len(embeds) > 0 else set()
    # add default embeds for items in the embedded_list
    embeds_to_add, processed_embeds = expand_embedded_list(item_type, types, embeds,
                                                           schema, processed_embeds)
    # automatically embed top level linkTo's not already embedded
    # also find subobjects and embed those
    embeds_to_add.extend(find_default_embeds_for_schema('', schema))
    # finally actually add the default embeds
    return build_default_embeds(embeds_to_add, processed_embeds)


def expand_embedded_list(item_type, types, embeds, schema, processed_embeds):
    """
    Takes the embedded_list (as defined in types/ file for an item) and finds
    all items that should have the default embeds added to them
    """
    embeds_to_add = []
    # Handles the use of a terminal '*' in the embeds
    for embed_path in embeds:
        # ensure that the embed is valid
        split_path = embed_path.strip().split('.')
        error_message, path_embeds_to_add = crawl_schemas_by_embeds(item_type, types, split_path, schema)
        if error_message:
            # remove bad embeds
            # check error_message rather than is_valid because there can
            # be cases of fields that are not valid for default embeds
            # but are still themselves valid fields
            processed_embeds.remove(embed_path)
            print(error_message, file = sys.stderr)
        else:
            embeds_to_add.extend(path_embeds_to_add)
    return embeds_to_add, processed_embeds


def build_default_embeds(embeds_to_add, processed_embeds):
    """
    Actually add the embed path for default embeds using the embeds_to_add
    list generated in add_default_embeds.
    """
    for add_embed in embeds_to_add:
        if add_embed[-2:] == '.*':
            processed_embeds.add(add_embed)
        else:
            # for neatness' sake, ensure redundant embeds are not getting added
            check_wildcard = add_embed + '.*'
            if check_wildcard not in processed_embeds and check_wildcard not in embeds_to_add:
                # default embeds to add
                for default_emb in DEFAULT_EMBEDS:
                    processed_embeds.add(add_embed + default_emb)
    return list(processed_embeds)


def find_default_embeds_for_schema(path_thus_far, subschema):
    """
    For a given field and that field's subschema, return an array of paths
    to the objects in that subschema. This includes all linkTo's and any
    subobjects within the subschema. Recursive function.
    """
    linkTo_paths = []
    if subschema.get('type') == 'array' and 'items' in subschema:
        items_linkTos = find_default_embeds_for_schema(path_thus_far, subschema['items'])
        linkTo_paths += items_linkTos
    if subschema.get('type') == 'object' and 'properties' in subschema:
        # we found an object in the schema. embed all its fields
        linkTo_paths.append(path_thus_far + '.*')
        props_linkTos = find_default_embeds_for_schema(path_thus_far, subschema['properties'])
        linkTo_paths += props_linkTos
    for key, val in subschema.items():
        if key == 'items' or key == 'properties':
            continue
        elif key == 'linkTo':
            linkTo_paths.append(path_thus_far)
        elif isinstance(val, dict):
            updated_path = key if path_thus_far == '' else path_thus_far + '.' + key
            item_linkTos = find_default_embeds_for_schema(updated_path, val)
            linkTo_paths += item_linkTos
    return linkTo_paths


def crawl_schemas_by_embeds(item_type, types, split_path, schema):
    """
    Take a split embed_path from the embedded_list and confirm that each item in the
    path has a valid schema. Also return default embeds associated with embed_path.
    If embed_path only has one element, return an error. This is because it is
    a redundant embed (all top level fields and @id/display_title for
    linkTos are added automatically).
    - split_path is embed_path (e.g. biosource.biosample.*) split on '.', so
      ['biosample', 'biosource', '*'] for the example above.
    - types parameter is registry[TYPES].
    A linkTo schema is considered valid if it has @id and display_title fields.
    Return values:
    1. error_message. Either None for no errors or a string to describe the error
    2. embeds_to_add. List of embeds to add for the given embed_path. In the
    case of embed_path ending with a *, this is the default embeds for that
    object's schema. Otherwise, it may just be embed_path, once its validated.
    """
    schema_cursor = schema
    embeds_to_add = []
    error_message = None
    linkTo_path = '.'.join(split_path)
    if len(split_path) == 1:
        error_message = '{} has a bad embed: {} is a top-level field. Did you mean: "{}.*"?.'.format(item_type, split_path[0], split_path[0])
    for idx in range(len(split_path)):
        element = split_path[idx]
        # schema_cursor should always be a dictionary if we have more split_fields
        if not isinstance(schema_cursor, dict):
            error_message = '{} has a bad embed: {} does not have valid schemas throughout.'.format(item_type, linkTo_path)
            return error_message, embeds_to_add
        if element == '*':
            linkTo_path = '.'.join(split_path[:-1])
            if idx != len(split_path) - 1:
                error_message = '{} has a bad embed: * can only be at the end of an embed.'.format(item_type)
            if '@id' in schema_cursor and 'display_title' in schema_cursor:
                # add default linkTos for the '*' object
                embeds_to_add.extend(find_default_embeds_for_schema(linkTo_path, schema_cursor))
            return error_message, embeds_to_add
        elif element in schema_cursor:
            # save prev_schema_cursor in case where last split_path is a non-linkTo field
            prev_schema_cursor = copy(schema_cursor)
            schema_cursor = schema_cursor[element]
            # drill into 'items' or 'properties'. always check 'items' before 'properties'
            # check if an array + drill into if so
            if schema_cursor.get('type', None) == 'array' and 'items' in schema_cursor:
                schema_cursor = schema_cursor['items']
            # check if an object + drill into if so
            if schema_cursor.get('type', None) == 'object' and 'properties' in schema_cursor:
                schema_cursor = schema_cursor['properties']
            # if we hit a linkTo, pull in the new schema of the linkTo type
            # if this is a terminal linkTo, add display_title/@id
            if 'linkTo' in schema_cursor:
                linkTo = schema_cursor['linkTo']
                try:
                    linkTo_type = types.all[linkTo]
                except KeyError:
                    error_message = '{} has a bad embed: {} is not a valid type.'.format(item_type, linkTo)
                    return error_message, embeds_to_add
                linkTo_schema = linkTo_type.schema
                schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema
                if '@id' not in schema_cursor or 'display_title' not in schema_cursor:
                    error_message = '{} has a bad embed: {} object does not have @id/display_title.'.format(item_type, linkTo_path)
                    return error_message, embeds_to_add
                # we found a terminal linkTo embed
                if idx == len(split_path) - 1:
                    embeds_to_add.append(linkTo_path)
                    return error_message, embeds_to_add
                else:  # also add default embeds for each intermediate item in the path
                    intermediate_path = '.'.join(split_path[:idx+1])
                    embeds_to_add.append(intermediate_path)
            # not a linkTo. See if this is this is the terminal element
            else:
                # check if this is the last element in path
                if idx == len(split_path) - 1:
                    # in this case, the last element in the embed is a field
                    # remove that from linkTo_path
                    linkTo_path = '.'.join(split_path[:-1])
                    if '@id' in prev_schema_cursor and 'display_title' in prev_schema_cursor:
                        embeds_to_add.append(linkTo_path)
                    return error_message, embeds_to_add
        else:
            error_message = '{} has a bad embed: {} is not contained within the parent schema. See {}.'.format(item_type, element, linkTo_path)
            return error_message, embeds_to_add
    # really shouldn't hit this return, but leave as a back up
    return error_message, embeds_to_add


def process_aggregated_items(request):
    """
    After all aggregated items have been found, process them on the request
    to narrow down to the fields we wish to aggregated on. This reduces the
    amount of info carried on the request, which is important because it
    will have to be carried through the subrequest chain.

    Args:
        request: the current request

    Returns:
        None
    """
    for agg_on, agg_body in request._aggregated_items.items():
        covered_json_items = []  # compare agg items using json.dumps
        item_idxs_to_remove = []  # remove these items after processing
        agg_fields = agg_body['_fields']
        # automatically aggregate on uuid if no fields provided
        # if you want to change this default, also change in create_mapping
        if not agg_fields:
            agg_fields = ['uuid']
        # handle badly formatted agg_fields here (?)
        if not isinstance(agg_fields, list):
            agg_fields = [agg_fields]
        for agg_idx, agg_item in enumerate(agg_body['items']):
            # deduplicate aggregated items by comparing sorted json
            # use whole agg_item (w/ 'parent' and 'embedded_path') for dedup
            if json.dumps(agg_item, sort_keys=True) in covered_json_items:
                item_idxs_to_remove.append(agg_idx)
                continue
            covered_json_items.append(json.dumps(agg_item, sort_keys=True))
            proc_item = {}
            for field in agg_fields:
                pointer = agg_item['item']
                split_field = field.strip().split('.')
                found_value = recursively_process_field(pointer, split_field)
                # terminal dicts will create issues with the mapping. Print a warning and skip
                if isinstance(found_value, dict):
                    log.error('ERROR. Found dictionary terminal value for field %s when aggregating %s items. Context is: %s'
                              % (field, agg_on, str(request.context.uuid)))
                    continue
                proc_pointer = proc_item
                for idx, split in enumerate(split_field):
                    if idx == len(split_field) - 1:
                        proc_pointer.update({split: found_value})
                    else:
                        if split not in proc_pointer:
                            proc_pointer[split] = {}
                        proc_pointer = proc_pointer[split]
            # replace the unprocessed item with the processed one
            agg_body['items'][agg_idx]['item'] = proc_item
        # remove deduplicated items by index in reverse order
        for dedup_idx in reversed(item_idxs_to_remove):
            del agg_body['items'][dedup_idx]


def recursively_process_field(item, split_fields):
    """
    Recursive function to pull out a field, in split-on-dot format, from
    the given item. Example of split format is:
        'subobject.value' --> ['subobject', 'value']
    Args:
        item: dictionary item to pull fields from
        split_fields: list resulting from field.split('.')

    Returns:
        The found value
    """
    try:
        next_level = item.get(split_fields[0])
    except AttributeError:
        # happens if a string/int is encountered at the top level
        return item
    if next_level is None:
        return None
    if len(split_fields[1:]) == 0:
        # we are at the end of the path
        return next_level
    elif isinstance(next_level, list):
        return [recursively_process_field(entry, split_fields[1:]) for entry in next_level]
    elif isinstance(next_level, dict):
        # can't drill down anymore
        return recursively_process_field(next_level, split_fields[1:])
    else:
        # can't drill down if not a list or dict. just return
        return next_level


###########################
# Resource view utilities #
###########################


def check_es_and_cache_linked_sids(context, request, view='embedded'):
    """
    For the given context and request, see if the desired item is present in
    Elasticsearch and, if so, retrieve it cache all sids of the linked objects
    that correspond to the given view. Store these in request._sid_cacheself.

    Args:
        context: current Item
        request: current Request
        view (str): 'embedded' or 'object', depending on the desired view

    Returns:
        The _source of the Elasticsearch result, if found. None otherwise
    """
    es_model = request.registry[STORAGE].get_by_uuid_direct(str(context.uuid), context.item_type)
    if es_model is None:
        return None
    es_res = es_model.get('_source')
    es_links_field = 'linked_uuids_object' if view == 'object' else 'linked_uuids_embedded'
    if es_res and es_res.get(es_links_field):
        linked_uuids = [link['uuid'] for link in es_res[es_links_field]
                        if link['uuid'] not in request._sid_cache]
        to_cache = request.registry[STORAGE].write.get_sids_by_uuids(linked_uuids)
        request._sid_cache.update(to_cache)
        return es_res
    return None


def validate_es_content(context, request, es_res, view='embedded'):
    """
    For the given context, request, and found Elasticsearch result, determine
    whether that result is valid. This depends on the view (either 'embedded' or
    'object'). This is based off of the following:
        1. All sids from the ES result must match those in request._sid_cache
        2. All rev_links from the ES result must be up-to-date
    This function will automatically add sids to _sid_cache from the DB if
    they are not already present.

    Args:
        context: current Item
        request: current Request
        es_res (dict): dictionary Elasticsearch result
        view (str): 'embedded' or 'object', depending on the desired view

    Returns:
        bool: True if es_res is valid, otherwise False
    """
    if view not in ['object', 'embedded']:
        return False
    es_links_field = 'linked_uuids_object' if view == 'object' else 'linked_uuids_embedded'
    linked_es_sids = es_res[es_links_field]
    if not linked_es_sids:  # there should always be context.uuid here. abort
        return False
    use_es_result = True
    # check to see if there are any new rev links from the item
    for rev_name in context.rev:
        # the call below updates request._rev_linked_uuids_by_item.
        db_rev_uuids = context.get_filtered_rev_links(request, rev_name)
        es_rev_uuids = es_res['rev_link_names'].get(rev_name, [])
        if set(db_rev_uuids) != set(es_rev_uuids):
            return False
    for linked in linked_es_sids:
        # infrequently, may need to add sids from the db to the _sid_cache
        if linked['uuid'] not in request._sid_cache:
            db_res = request.registry[STORAGE].write.get_by_uuid(linked['uuid'])
            if db_res:
                request._sid_cache[linked['uuid']] = db_res.sid
        found_sid = request._sid_cache.get(linked['uuid'])
        if found_sid is None or linked['sid'] < found_sid:
            use_es_result = False
            break
    return use_es_result


class CalculatedOverrideOfBasePropertiesNotPermitted(ValueError):
    """ Helper exception for below method """
    def __init__(self, calculated_props, base_props):
        self.calculated_props = calculated_props
        self.base_props = base_props
        super().__init__('Calculated properties are not permitted to override'
                         ' base properties of a sub-embedded object:'
                         '\n calculated: %s'
                         '\n base props: %s' % (calculated_props, base_props))


def merge_calculated_into_properties(properties: dict, calculated: dict):
    """ Performs a depth 2 dictionary merge into properties.

    :param properties: base item properties
    :param calculated: calculated properties
    """
    for key, value in calculated.items():
        if key not in properties:
            properties[key] = value
        else:
            calculated_sub_values = calculated[key]
            properties_sub_values = properties[key]
            if isinstance(calculated_sub_values, dict) and isinstance(properties_sub_values, dict):
                for k, v in calculated_sub_values.items():
                    if k in properties_sub_values:
                        raise CalculatedOverrideOfBasePropertiesNotPermitted(calculated_sub_values,
                                                                             properties_sub_values)
                    properties_sub_values[k] = v
            elif isinstance(calculated_sub_values, list) and isinstance(properties_sub_values, list):
                for calculated_entry, props_entry in zip(calculated_sub_values, properties_sub_values):
                    for k, v in calculated_entry.items():
                        if k in props_entry:
                            raise CalculatedOverrideOfBasePropertiesNotPermitted(calculated_sub_values,
                                                                                 properties_sub_values)
                        props_entry[k] = v
            else:
                raise ValueError('Got unexpected types for calculated/properties sub-values: '
                                 'calculated: %s \n properties: %s' % (calculated_sub_values, properties_sub_values))


class CachedField:
    def __init__(self, name, update_function, timeout=600):
        """ Provides a named field that is cached for a certain period of time. The value is computed
            on calls to __init__, after which the get() method should be used.

        :param name: name of property
        :param update_function: lambda to be invoked to update the value
        :param timeout: TTL of this field, in seconds
        """
        self.name = name
        self._update_function = update_function
        self.timeout = timeout
        self.value = update_function()
        self.time_of_next_update = datetime.utcnow() + timedelta(seconds=timeout)

    def _update_timestamp(self):
        self.time_of_next_update = datetime.utcnow() + timedelta(seconds=self.timeout)

    def _update_value(self):
        self.value = self._update_function()
        self._update_timestamp()

    def get(self):
        """ Intended for normal use - to get the value subject to the given TTL on creation. """
        now = datetime.utcnow()
        if now > self.time_of_next_update:
            self._update_value()
        return self.value

    def get_updated(self, push_ttl=False):
        """ Intended to force an update to the value and potentially push back the timeout from now. """
        self.value = self._update_function()
        if push_ttl:
            self.time_of_next_update = datetime.utcnow() + timedelta(seconds=self.timeout)
        return self.value

    def set_timeout(self, new_timeout):
        """ Sets a new value for timeout and restarts the timeout counter."""
        self.timeout = new_timeout
        self._update_timestamp()

    def __repr__(self):
        return 'CachedField %s with update function %s on timeout %s' % (
            self.name, self._update_function, self.timeout
        )

