from past.builtins import basestring
from pyramid.threadlocal import manager as threadlocal_manager
from pyramid.httpexceptions import HTTPForbidden
from .interfaces import CONNECTION, STORAGE
import json
import structlog


log = structlog.getLogger(__name__)


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


def expand_path(request, obj, path):
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


def expand_embedded_model(request, obj, model, parent_path='', embedded_path=[]):
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
                                  parent_path='', embedded_path=[]):
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
        # ONLY if the field is used in downstream_model (i.e. actually in the
        # context embedded_list)
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
     'bisource': {'fields_to_use': ['name']},
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
            if json.dumps(agg_item['item'], sort_keys=True) in covered_json_items:
                item_idxs_to_remove.append(agg_idx)
                continue
            covered_json_items.append(json.dumps(agg_item['item'], sort_keys=True))
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
    es_model = request.registry[STORAGE].read.get_by_uuid_direct(str(context.uuid), context.item_type)
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


def select_distinct_values(request, value_path, *from_paths):
    if isinstance(value_path, basestring):
        value_path = value_path.split('.')

    values = from_paths
    for name in value_path:
        objs = (request.embed(member, '@@object') for member in values)
        value_lists = (ensurelist(obj.get(name, [])) for obj in objs)
        values = {value for value_list in value_lists for value in value_list}

    return list(values)
