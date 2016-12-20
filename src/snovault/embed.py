from copy import deepcopy
from .cache import ManagerLRUCache
from past.builtins import basestring
from posixpath import join
from pyramid.compat import (
    native_,
    unquote_bytes_to_wsgi,
)
from pyramid.httpexceptions import HTTPNotFound
import logging
log = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)
    config.add_renderer('null_renderer', NullRenderer)
    config.add_request_method(embed, 'embed')
    config.add_request_method(lambda request: set(), '_embedded_uuids', reify=True)
    config.add_request_method(lambda request: set(), '_linked_uuids', reify=True)
    config.add_request_method(lambda request: None, '__parent__', reify=True)


def make_subrequest(request, path):
    """ Make a subrequest

    Copies request environ data for authentication.

    May be better to just pull out the resource through traversal and manually
    perform security checks.
    """
    env = request.environ.copy()
    if path and '?' in path:
        path_info, query_string = path.split('?', 1)
        path_info = path_info
    else:
        path_info = path
        query_string = ''
    env['PATH_INFO'] = path_info
    env['QUERY_STRING'] = query_string
    subreq = request.__class__(env, method='GET', content_type=None,
                               body=b'')
    subreq.remove_conditional_headers()
    # XXX "This does not remove headers like If-Match"
    subreq.__parent__ = request
    return subreq


embed_cache = ManagerLRUCache('embed_cache')


def embed(request, *elements, **kw):
    """ as_user=True for current user
    Pass in fields_to_embed as a keyword arg
    """
    # Should really be more careful about what gets included instead.
    # Cache cut response time from ~800ms to ~420ms.
    fields_to_embed = kw.get('fields_to_embed')
    schema = kw.get('schema')
    as_user = kw.get('as_user')
    item_type = kw.get('item_type')
    path = join(*elements)
    path = unquote_bytes_to_wsgi(native_(path))
    # check to see if this embed is a non-object field
    if len(path.split('/')) != 4 and path[0] != '/':
        return path.split('/')[0]  # grab field from <field>/@@embed
    log.debug('embed: %s', path)
    if as_user is not None:
        result, embedded, linked = _embed(request, path, as_user)
    else:
        cached = embed_cache.get(path, None)
        if cached is None:
            cached = _embed(request, path)
            embed_cache[path] = cached
        result, embedded, linked = cached
        result = deepcopy(result)

    request._embedded_uuids.update(embedded)
    request._linked_uuids.update(linked)
    # parse result to conform to selective embedding
    if item_type is not None and fields_to_embed is not None:
        p_result = parse_result(result, fields_to_embed, schema)
        return p_result
    return result


def _embed(request, path, as_user='EMBED'):
    subreq = make_subrequest(request, path)
    subreq.override_renderer = 'null_renderer'
    if as_user is not True:
        if 'HTTP_COOKIE' in subreq.environ:
            del subreq.environ['HTTP_COOKIE']
        subreq.remote_user = as_user
    try:
        result = request.invoke_subrequest(subreq)
    except HTTPNotFound:
        raise KeyError(path)
    return result, subreq._embedded_uuids, subreq._linked_uuids


def parse_result(result, fields_to_embed, schema):
    """
    Code for ES upgrade.
    Takes a fully-embedded json result and parses it based off of the
    appropriate schema and fields to be embedded, which are defined in the
    /types/ file for that object (outside of snovault).
    Recursive function, so this can handle subobjects defined in schemas that
    are themselves not schemas.
    Must handle cases where linkTo fields are objects or arrays of objects.
    """
    parsed_result = {}
    linkTo_fields = []
    # First add all non-link to fields if top-level
    for key, val in result.items():
        if key in schema['properties'].keys():
            # single obj
            if 'linkTo' in schema['properties'][key]:
                linkTo_fields.append(key)
            # array of objs
            elif 'items' in schema['properties'][key] and 'linkTo' in schema['properties'][key]['items']:
                linkTo_fields.append(key)
            elif 'subobject' in schema['properties'][key] and schema['properties'][key]['subobject'] == True:
                sub_fields_to_embed = ['.'.join(emb.split('.')[1:]) for emb in fields_to_embed if (key == emb.split('.')[0] and len(emb.split('.')) > 1)]
                if schema['properties'][key]['type'] == 'array':
                    subobject = []
                    for subitem in val:
                        subobject.append(parse_result(subitem, sub_fields_to_embed, schema['properties'][key]['items']))
                else:  # not an array of subobjects, just a subobject
                    subobject = parse_result(val, sub_fields_to_embed, schema['properties'][key])
                parsed_result[key] = subobject
            else:
                parsed_result[key] = val
        else:
            parsed_result[key] = val
    if not isinstance(fields_to_embed, list): # no embedding here
        return parsed_result
    for field in linkTo_fields:
        # the embed will be used if it has the matching base-level linkTo
        matching_embeds = [emb for emb in fields_to_embed if field == emb.split('.')[0]]
        if len(matching_embeds) == 0: # do not embed this object
            continue
        matching_embeds = ['.'.join(emb.split('.')[1:]) if len(emb.split('.')) > 1 else '*' for emb in matching_embeds]
        for emb in matching_embeds:
            inner_result = inner_parse(deepcopy(result[field]), emb)
            parsed_result = update_embedded_obj(parsed_result, field, inner_result)
    return parsed_result


def inner_parse(result, field):
    """
    Code for ES upgrade.
    Takes one embedded field (such as biosource.individual.organism) and the
    corresponding branch of the fully expanded json from parse_result().
    Trims the json down to get the desired data. The final part of the field
    (when split by '.') may be an object or a single field within a schema
    (of any type). Handles 3 main cases:
        1. All fields are to embedded, but no linkTos.
        2. There is a linkTo, and the embedded objects are in an array
        3. There is a linkTo and field is the embedded object (no array)
    This function is used recursively if the current level isn't the deepest
    level of desired embedding.

    Will raise an error if an embedded field is trying to be found that doesn't
    exist (this would occur if a field was incorrectly embedded in the /types/
    file).
    """
    split_field = field.split('.')
    if field == '*':
        if isinstance(result, dict) and 'uuid' in result.keys():
            for key, val in result.items():
                if isinstance(val, dict) and 'uuid' in val.keys():
                    result[key] = val['@id'] if '@id' in val.keys() else val['uuid']
        return result
    elif isinstance(result, list):
        ret_arr = []
        for result_entry in result:
            if len(split_field) > 1:
                # ensure desired field exists
                if isinstance(result_entry, dict) and split_field[0] not in result_entry.keys():
                    # field not found for this entry; do not include it
                    ret_arr.append({'uuid': result_entry['uuid']})
                    continue
                inner_val = inner_parse(result_entry[split_field[0]], '.'.join(split_field[1:]))
                if split_field == 'uuid':
                    ret_arr.append({split_field[0]: inner_val})
                else:  # must use uuid to to allow for annotation of array items
                    ret_arr.append({split_field[0]: inner_val, 'uuid': result_entry['uuid']})
            else:
                found_val = result_entry[field] if field in result_entry.keys() else 'NOT_FOUND'
                # ensure that deeper objects are not automatically embedded if we're already at the deepest desired embed
                if isinstance(found_val, dict) and 'uuid' in found_val.keys():
                    for key, val in found_val.items():
                        if isinstance(val, dict) and 'uuid' in val.keys():
                            found_val[key] = val['@id'] if '@id' in val.keys() else val['uuid']
                if field == 'uuid':
                    ret_arr.append({field: found_val})
                elif found_val != 'NOT_FOUND':
                    ret_arr.append({field: found_val, 'uuid': result_entry['uuid']})
                else:
                    ret_arr.append({'uuid': result_entry['uuid']})
        return ret_arr
    else:
        ret_obj = {}
        if len(split_field) > 1:
            if split_field != 'uuid':
                # add uuid here to keep consistent with the way arrays are done
                ret_obj['uuid'] = result['uuid']
            ret_obj[split_field[0]] = inner_parse(result[split_field[0]], '.'.join(split_field[1:]))
        else:
            if not isinstance(result, dict):
                return result
            if field != 'uuid':
                # add uuid here to keep consistent with the way arrays are done
                ret_obj['uuid'] = result['uuid']
            try:
                ret_obj[field] = result[field]
            except:
                print('Embedded key ', field, ' was not found inside object: ', result)
                raise
        return ret_obj


def update_embedded_obj(emb_obj, field, update_val):
    """
    Used to combine the resulting json from one embedded field from
    inner_parse() with the aggregate embedded result.
    Used recursively if in object is found within the embedded object.
    emb_obj is the aggregate json result so far, field is the field that
    will be embedded on, and update_val is the result from inner_parse().
    """
    curr_field_val = emb_obj[field] if field in emb_obj.keys() else {}
    if isinstance(update_val, list):
        # address updating deeper objects
        for k in range(len(update_val)):
            for key, val in update_val[k].items():
                if isinstance(val, dict):
                    update_val[k] = update_embedded_obj(update_val[k], key, val)
        curr_field_val = recursive_merge_field(curr_field_val, update_val)
    elif isinstance(update_val, str):
        if curr_field_val == {}:
            curr_field_val = update_val
        else:
            # less information is returned by updating, so keep as-is
            return emb_obj
    else:
        for key, val in update_val.items():
            if isinstance(val, dict):
                update_val = update_embedded_obj(update_val, key, val)
        curr_field_val = recursive_merge_field(curr_field_val, update_val)
    emb_obj[field] = curr_field_val
    return emb_obj


# merges by uuid
def recursive_merge_field(prev_field, update_field):
    """
    Takes two branches of a json object that have the same field origin and
    recursively merge their values (prev_field and update_field args).
    For example, this would be used to merge the embed results for:
        biosource.organism.individual.name
        and
        biosource.organims.individual.scientific_name
    (These fields are simply an example of object names; used in encode and
    fourfront)
    For arrays, uuids are needed to make sure the correct array entries are
    being merged together.
    """
    if prev_field == {} or prev_field == update_field:
        return update_field
    if isinstance(update_field, list):
        for i in range(len(update_field)):
            for j in range(len(prev_field)):
                if prev_field[j]['uuid'] == update_field[i]['uuid']:
                    prev_field[j] = recursive_merge_field(prev_field[j], update_field[i])
        return prev_field
    to_return = deepcopy(prev_field)
    for prev_key, prev_val in prev_field.items():
        for up_key, up_val in update_field.items():
            if prev_key == up_key:
                if isinstance(prev_val, dict) and isinstance(up_val, dict):
                    if prev_val['uuid'] == up_val['uuid']:  # safety check; shouldn't fail
                        to_return[prev_key] = recursive_merge_field(prev_val, up_val)
                else:
                    to_return[prev_key] = up_val
            elif up_key not in prev_field.keys():
                to_return[up_key] = up_val
    return to_return


class NullRenderer:
    '''Sets result value directly as response.
    '''
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        if request is None:
            return value
        request.response = value
        return None
