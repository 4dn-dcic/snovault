import boto3
import contextlib
import datetime as datetime_module
import functools
import gzip
import io
import json
import os
import re
import structlog
import sys
import tempfile
import time
from typing import Optional

from botocore.client import Config
from copy import copy
from datetime import datetime, timedelta
from io import BytesIO
from pyramid.httpexceptions import HTTPUnprocessableEntity, HTTPForbidden
from pyramid.threadlocal import manager as threadlocal_manager
from dcicutils.ecs_utils import ECSUtils
from dcicutils.misc_utils import ignored, PRINT, VirtualApp, count_if, identity
from dcicutils.secrets_utils import assume_identity

from .interfaces import CONNECTION, STORAGE, TYPES
from .settings import Settings


log = structlog.getLogger(__name__)


###################
# Misc. utilities #
###################


# These used to be in create_mapping.py
# number of shards and replica, currently used for all indices
NUM_SHARDS = 1
NUM_REPLICAS = 1
# memory safeguard; how many documents can be covered by from + size search req
SEARCH_MAX = 100000
# ignore above this number of kb when mapping keyword fields
KW_IGNORE_ABOVE = 512
# used to customize ngram filter behavior
MIN_NGRAM = 2
MAX_NGRAM = 10
# used to enable nested mapping on array of object fields
NESTED_ENABLED = 'enable_nested'
# global index.refresh_interval - currently the default of 1s
REFRESH_INTERVAL = '1s'

# Schema keys to ignore when finding embeds
SCHEMA_KEYS_TO_IGNORE_FOR_EMBEDS = {
    "items", "properties", "additionalProperties", "patternProperties"
}


class IndexSettings:
    """ Object wrapping important ElasticSearch index settings. Previously these
        settings were determined by the constants above, but depending on data model
        structure and size it can be necessary to tune these.

        number_of_shards: determines how many shards to split each replica into
                          large collections can benefit from increasing this value
        number_of_replicas: number of times to replicate the index across the cluster
                            large collections also benefit from increasing this value,
                            but note that it has a multiplicative effect on the number
                            of shards ie: 2 replicas divided into 3 shards results in 6
                            total shards across the cluster
        min_ngram: shortest string length to map for string matching or q= searching
        max_ngram: longest string length to map for string matching or q= searching
                   the relationship between min and max ngram dictates both the amount of
                   free text data that is generated and the length of strings that are
                   indexed - for example min=2 and max=3 would produce ngrams for the word
                   "sentence" = [se, en, nt, te, en, nc, ce, sen, ent, nte, ten, enc, nce]
                   note the implications of this structure - if max is too low, terms may
                   not be long enough to match your important terms, which will affect the
                   accuracy of search results and may even be misleading to the user
        refresh_interval: determines how often index refreshes occur - an index refresh is
                          when a blocking forced update is triggered by the cluster for all
                          replicas and shards of the index - note that this operation blocks
                          even if no changes are present in the index, so tuning this value
                          up from the default may be necessary if we can tolerate indexing
                          delay for search results, which in many cases we can
    """
    def __init__(self, *, shard_count=NUM_SHARDS, replica_count=NUM_REPLICAS,
                 min_ngram=MIN_NGRAM, max_ngram=MAX_NGRAM, refresh_interval=REFRESH_INTERVAL):
        self.settings = {
            'index': {
                'number_of_shards': shard_count,
                'number_of_replicas': replica_count,
                'refresh_interval': refresh_interval,
                'analysis': {
                    'filter': {
                        'ngram_filter': {
                            'type': 'edgeNGram',
                            'min_gram': min_ngram,
                            'max_gram': max_ngram
                        },
                        # truncate tokens to size MAX_NGRAM
                        'truncate_to_ngram': {
                            'type': 'truncate',
                            'length': max_ngram
                        }
                    }
                }
            }
        }


def create_empty_s3_file(s3_client, bucket: str, key: str, s3_encrypt_key_id: Optional[str] = None):
    """
    Args:
        s3_client: a client object that results from a boto3.client('s3', ...) call.
        bucket: an S3 bucket name
        key: the name of a key within the given S3 bucket
        s3_encrypt_key_id: the name of a KMS encrypt key id, or None
    """
    empty_file = "/dev/null"

    extra_kwargs = extra_kwargs_for_s3_encrypt_key_id(s3_encrypt_key_id=s3_encrypt_key_id,
                                                      client_name='create_empty_s3_file')

    s3_client.upload_file(empty_file, Bucket=bucket, Key=key, **extra_kwargs)


def get_trusted_email(request, context=None, raise_errors=True):
    """
    Get an email address on behalf of which we can issue other requests.

    If auth0 has authenticated user info to offer, return that.
    Otherwise, look for a userid.xxx among request.effective_principals and get the email from that.

    This will raise HTTPUnprocessableEntity if there's a problem obtaining the mail.
    """
    try:
        context = context or "Requirement"
        email = getattr(request, '_auth0_authenticated', None)
        if not email:
            user_uuid = None
            for principal in request.effective_principals:
                if principal.startswith('userid.'):
                    user_uuid = principal[7:]
                    break
            if not user_uuid:
                raise HTTPUnprocessableEntity('%s: Must provide authentication' % context)
            user_props = get_item_or_none(request, user_uuid)
            if not user_props:
                raise HTTPUnprocessableEntity('%s: User profile missing' % context)
            if 'email' not in user_props:
                raise HTTPUnprocessableEntity('%s: Entry for "email" missing in user profile.' % context)
            email = user_props['email']
        return email
    except Exception:
        if raise_errors:
            raise
        return None


def beanstalk_env_from_request(request):
    return beanstalk_env_from_registry(request.registry)


def beanstalk_env_from_registry(registry):
    return registry.settings.get('env.name')


def customized_delay_rerun(sleep_seconds=1):
    def parameterized_delay_rerun(*args):
        """ Rerun function for flaky """
        ignored(args)
        time.sleep(sleep_seconds)
        return True
    return parameterized_delay_rerun


delay_rerun = customized_delay_rerun(sleep_seconds=1)

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
        # raise DictionaryKeyError(dictionary=dictionary, key=key)  this causes MPIndexer exception - will 3/10/2020
    else:
        return dictionary[key]


def deduplicate_list(lst):
    """ De-duplicates the given list by converting it to a set then back to a list.

    NOTES:
    * The list must contain 'hashable' type elements that can be used in sets.
    * The result list might not be ordered the same as the input list.
    * This will also take tuples as input, though the result will be a list.

    :param lst: list to de-duplicate
    :return: de-duplicated list
    """
    return list(set(lst))


def gunzip_content(content):
    """ Helper that will gunzip content (into memory) """
    f_in = BytesIO()
    f_in.write(content)
    f_in.seek(0)
    with gzip.GzipFile(fileobj=f_in, mode='rb') as f:
        gunzipped_content = f.read()
    return gunzipped_content.decode('utf-8')


DEBUGLOG = os.environ.get('DEBUGLOG', "")


def debuglog(*args):
    """
    As the name implies, this is a low-tech logging facility for temporary debugging info.
    Prints info to a file in user's home directory.

    The debuglog facility allows simple debugging for temporary debugging of disparate parts of the system.
    It takes arguments like print or one of the logging operations and outputs to ~/DEBUGLOG-yyyymmdd.txt.
    Each line in the log is timestamped.
    """
    if DEBUGLOG:
        try:
            nowstr = str(datetime.datetime.now())
            dateid = nowstr[:10].replace('-', '')
            with io.open(os.path.expanduser(os.path.join(DEBUGLOG, "DEBUGLOG-%s.txt" % dateid)), "a+") as fp:
                PRINT(nowstr, *args, file=fp)
        except Exception:
            # There are many things that could go wrong, but none of them are important enough to fuss over.
            # Maybe it was a bad pathname? Out of disk space? Network error?
            # It doesn't really matter. Just continue...
            pass


_skip_fields = ['@type', 'principals_allowed']  # globally accessible if need be in the future


# TODO: This is a priority candidate for unit testing. -kmp 27-Jul-2020
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
    if isinstance(value_path, str):
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
    if isinstance(value, str):
        return [value]
    return value


def uuid_to_path(request, obj, path):
    if isinstance(path, str):
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
    if isinstance(path, str):
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
    if isinstance(path, str):
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
        return []  # no types found
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
        raise Exception('Could not find schema field for: %s. Field not found. Failed at: %s'
                        % (field_path, curr_field))

    # schema_cursor should always be a dictionary
    if not isinstance(schema_cursor, dict):
        raise Exception('Could not find schema field for: %s. Non-dictionary schema. Failed at: %s'
                        % (field_path, curr_field))

    # base case. We have found the desired schema
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
            raise Exception('Could not find schema field for: %s. Invalid linkTo. Failed at: %s'
                            % (field_path, curr_field))
        linkTo_schema = linkTo_type.schema
        schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema

    return crawl_schema(types, field_path, schema_cursor, split_path[1:])


##########################################
# Embedding / aggregated_items utilities #
##########################################


# Terminal fields that are added to the embedded list for every embedded item
# Status must now be a default embed, since it can effect principals_allowed
DEFAULT_EMBEDS = ['.@id', '.@type', '.display_title', '.uuid', '.status', '.principals_allowed.*']


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
        PRINT("you don't have access to this object")

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
    elif isinstance(obj_val, str):
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
    FIELDS_TO_USE = 'fields_to_use'
    embedded_model = {FIELDS_TO_USE: ['*']}
    for field in fields_to_embed:
        split_field = field.split('.')
        max_idx = len(split_field) - 1
        cursor = embedded_model
        for idx, subfield in enumerate(split_field):
            if idx == max_idx:  # terminal field
                cursor[FIELDS_TO_USE] = fields_to_use = cursor.get(FIELDS_TO_USE, [])
                fields_to_use.append(subfield)
            else:
                if subfield not in cursor:
                    cursor[subfield] = {}
                cursor = cursor[subfield]
    return embedded_model


def add_default_embeds(item_type, types, embeds, schema=None):
    """
    Perform default processing on the embedded_list of an item_type.
    Three part process that automatically builds a list of embed paths using
    the embedded_list (embeds parameter), expanding all the top level linkTos,
    and then finally adding the default embeds to all the linkTo paths generated.
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    if schema is None:
        schema = {}
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
            PRINT(error_message, file=sys.stderr)
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
        if key in SCHEMA_KEYS_TO_IGNORE_FOR_EMBEDS:
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
        error_message = ('{} has a bad embed: {} is a top-level field. Did you mean: "{}.*"?.'
                         .format(item_type, split_path[0], split_path[0]))
    for idx in range(len(split_path)):
        element = split_path[idx]
        # schema_cursor should always be a dictionary if we have more split_fields
        if not isinstance(schema_cursor, dict):
            error_message = f'{item_type} has a bad embed: {linkTo_path} does not have valid schemas throughout.'
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
                    error_message = ('{} has a bad embed: {} object does not have @id/display_title.'
                                     .format(item_type, linkTo_path))
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
            error_message = ('{} has a bad embed: {} is not contained within the parent schema. See {}.'
                             .format(item_type, element, linkTo_path))
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
                    log.error('ERROR. Found dictionary terminal value for field %s when aggregating %s items.'
                              ' Context is: %s'
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


def _sid_cache(request):
    return request._sid_cache  # noQA. Centrally ignore that it's an access to a protected member.


def _sid_cache_update(request, new_value):
    request._sid_cache.update(new_value)  # noQA. Centrally ignore that it's an access to a protected member.


def check_es_and_cache_linked_sids(context, request, view='embedded'):
    """
    For the given context and request, see if the desired item is present in
    Elasticsearch and, if so, retrieve it cache all sids of the linked objects
    that correspond to the given view. Store these in request's sid cache.

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
                        if link['uuid'] not in _sid_cache(request)]
        to_cache = request.registry[STORAGE].write.get_sids_by_uuids(linked_uuids)
        _sid_cache_update(request, to_cache)
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
        cached = _sid_cache(request)
        found_sid = cached.get(linked['uuid'])
        if not found_sid:
            db_res = request.registry[STORAGE].write.get_by_uuid(linked['uuid'])
            if db_res:
                cached[linked['uuid']] = found_sid = db_res.sid
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
            # 2024-07-09: This check for uuid is fallout from the fix
            # in indexing_views.item_index_data for uuid in frame=raw view.
            elif key != "uuid":
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


def generate_indexer_namespace_for_testing(prefix='sno'):
    test_job_id = os.environ.get('TEST_JOB_ID') or os.environ.get('TRAVIS_JOB_ID')
    if test_job_id:
        if '-test-' in test_job_id:
            # We need to manage some set of ids unchanged at the command line,
            # so if the caller has segmented things, trust it to have added a repo prefix
            # and just return it unaltered. -kmp 9-Mar-2021
            return test_job_id
        # Nowadays, this might be a GitHub run id, which isn't globally unique.
        # Each repo is monotonic but at different pace and they can collide. Repo prefix is essential.
        return "%s-test-%s-" % (prefix, test_job_id)
    else:
        # We've experimentally determined that it works pretty well to just use the timestamp.
        return "%s-test-%s-" % (prefix, int(datetime_module.datetime.now().timestamp() * 1000000))


INDEXER_NAMESPACE_FOR_TESTING = generate_indexer_namespace_for_testing()


def is_admin_request(request):
    """ Checks for 'group.admin' in effective_principals on request - if present we know this
        request was submitted by an admin
    """
    return 'group.admin' in request.effective_principals


def get_item_or_none(request, value, itype=None, frame='object'):
    """
    Return the view of an item with given frame. Can specify different types
    of `value` for item lookup

    Args:
        request: the current Request
        value (str): String item identifier or a dict containing @id/uuid
        itype (str): Optional string collection name for the item (e.g. /file-formats/)
        frame (str): Optional frame to return. Defaults to 'object'

    Returns:
        dict: given view of the item or None on failure
    """
    item = None

    if isinstance(value, dict):
        if 'uuid' in value:
            value = value['uuid']
        elif '@id' in value:
            value = value['@id']

    svalue = str(value)

    # Below case is for UUIDs & unique_keys such as accessions, but not @ids
    if not svalue.startswith('/') and not svalue.endswith('/'):
        svalue = '/' + svalue + '/'
        if itype is not None:
            svalue = '/' + itype + svalue

    # Request.embed will attempt to get from ES for frame=object/embedded
    # If that fails, get from DB. Use '@@' syntax instead of 'frame=' because
    # these paths are cached in indexing
    try:
        item = request.embed(svalue, '@@' + frame)
    except Exception:
        pass

    # could lead to unexpected errors if == None
    return item


CONTENT_TYPE_SPECIAL_CASES = {
    'application/x-www-form-urlencoded': [
        # Single legacy special case to allow us to POST to metadata TSV requests via form submission.
        # All other special case values should be added using register_path_content_type.
        '/metadata/',
        '/variant-sample-search-spreadsheet/',
        re.compile(r'/variant-sample-lists/[\da-z-]+/@@spreadsheet/'),
    ]
}


def register_path_content_type(*, path, content_type):
    """
    Registers that endpoints that begin with the specified path use the indicated content_type.

    This is part of an inelegant workaround for an issue in renderers.py that maybe we can make go away in the future.
    See the 'implementation note' in ingestion/common.py for more details.
    """
    exceptions = CONTENT_TYPE_SPECIAL_CASES.get(content_type, None)
    if exceptions is None:
        CONTENT_TYPE_SPECIAL_CASES[content_type] = exceptions = []
    if path not in exceptions:
        exceptions.append(path)


compiled_regexp_class = type(re.compile("foo.bar"))  # Hides that it's _sre.SRE_Pattern in 3.6, but re.Pattern in 3.7


def content_type_allowed(request):
    """
    Returns True if the current request allows the requested content type.

    This is part of an inelegant workaround for an issue in renderers.py that maybe we can make go away in the future.
    See the 'implementation note' in ingestion/common.py for more details.
    """
    if request.content_type == "application/json":
        # For better or worse, we always allow this.
        return True

    exceptions = CONTENT_TYPE_SPECIAL_CASES.get(request.content_type)

    if exceptions:
        for path_condition in exceptions:
            if isinstance(path_condition, str):
                if path_condition in request.path:
                    return True
            elif isinstance(path_condition, compiled_regexp_class):
                if path_condition.match(request.path):
                    return True
            else:
                raise NotImplementedError(f"Unrecognized path_condition: {path_condition}")

    return False


def check_user_is_logged_in(request):
    """ Raises HTTPForbidden if the request did not come from a logged in user. """
    for principal in request.effective_principals:
        if principal.startswith('userid.') or principal == 'group.admin':  # allow if logged in OR has admin
            break
    else:
        raise HTTPForbidden(title="Not logged in.")



EMAIL_PATTERN = re.compile(r'[^@]+[@][^@]+')


def make_vapp_for_email(*, email, app=None, registry=None, context=None):
    app = _app_from_clues(app=app, registry=registry, context=context)
    if not isinstance(email, str) or not EMAIL_PATTERN.match(email):
        # It's critical to check that the pattern has an '@' so we know it's not a system account (injection).
        raise RuntimeError("Expected email to be a string of the form 'user@host'.")
    user_environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': email,
    }
    vapp = VirtualApp(app, user_environ)
    return vapp


@contextlib.contextmanager
def vapp_for_email(email, app=None, registry=None, context=None):
    yield make_vapp_for_email(email=email, app=app, registry=registry, context=context)


def make_vapp_for_ingestion(*, app=None, registry=None, context=None):
    app = _app_from_clues(app=app, registry=registry, context=context)
    user_environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'INGESTION',
    }
    vapp = VirtualApp(app, user_environ)
    return vapp


@contextlib.contextmanager
def vapp_for_ingestion(app=None, registry=None, context=None):
    yield make_vapp_for_ingestion(app=app, registry=registry, context=context)


def _app_from_clues(app=None, registry=None, context=None):
    if count_if(identity, [app, registry, context]) != 1:
        raise RuntimeError("Expected exactly one of app, registry, or context.")
    if not app:
        app = (registry or context).app
    return app


def make_s3_client():
    s3_client_extra_args = {}
    if 'IDENTITY' in os.environ:
        identity = assume_identity()
        s3_client_extra_args['aws_access_key_id'] = key_id = identity.get('S3_AWS_ACCESS_KEY_ID')
        s3_client_extra_args['aws_secret_access_key'] = identity.get('S3_AWS_SECRET_ACCESS_KEY')
        s3_client_extra_args['region_name'] = ECSUtils.REGION
        log.warning(f"make_s3_client using S3 entity ID {key_id[:10]} arguments in `boto3 client creation call.")
        if 'ENCODED_S3_ENCRYPT_KEY_ID' in identity:
            # This setting is required when testing locally and encrypted buckets need to be accessed.
            s3_client_extra_args['config'] = Config(signature_version='s3v4')
    else:
        log.warning(f'make_s3_client called with no identity')

    s3_client = boto3.client('s3', **s3_client_extra_args)
    return s3_client


def build_s3_presigned_get_url(*, params):
    """ Helper function that builds a presigned URL. """
    s3_client = make_s3_client()
    return s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params=params,
        ExpiresIn=36 * 60 * 60
    )


def convert_integer_to_comma_string(value):
    """Convert integer to comma-formatted string for displaying SV
    position.

    :param value: Value to format.
    :type value: int
    :returns: Comma-formatted integer or None
    :rtype: str or None
    """
    result = None
    if isinstance(value, int):
        result = format(value, ",d")
    return result


ENCODED_ROOT_DIR = os.path.dirname(__file__)


def resolve_file_path(path, file_loc=None, root_dir=ENCODED_ROOT_DIR):
    """ Takes a relative path from this file location and returns an absolute path to
        the desired file, needed for WSGI to resolve embed files.

    :param path: relative path to be converted
    :param file_loc: absolute path to location path is relative to, by default path/to/encoded/src/
    :return: absolute path to location specified by path
    """
    if path.startswith("~"):
        # Really this shouldn't happen, so we could instead raise an error, but at least this is semantically correct.
        path = os.path.expanduser(path)
    if file_loc:
        if file_loc.startswith("~"):
            file_loc = os.path.expanduser(file_loc)
        path_to_this_file = os.path.abspath(os.path.dirname(file_loc))
    else:
        path_to_this_file = os.path.abspath(root_dir)
    return os.path.join(path_to_this_file, path)


# These next few could be in dcicutils.s3_utils as part of s3Utils, but details of interfaces would have to change.
# For now, for expedience, they can live here and we can refactor later. -kmp 25-Jul-2020

@contextlib.contextmanager
def s3_output_stream(s3_client, bucket: str, key: str, s3_encrypt_key_id: Optional[str] = None):
    """
    This context manager allows one to write:

        with s3_output_stream(s3_client, bucket, key) as fp:
            ... fp.write("foo") ...

    to do output to an s3 bucket.

    In fact, an intermediate local file is involved, so this function yields a file pointer (fp) to a
    temporary local file that is open for write. That fp should be used to supply content to the file
    during the dynamic scope of the context manager. Once the context manager's body executes, the
    file will be closed, its contents will be copied to s3, and finally the temporary local file will
    be deleted.

    Args:
        s3_client: a client object that results from a boto3.client('s3', ...) call.
        bucket: an S3 bucket name
        key: the name of a key within the given S3 bucket
        s3_encrypt_key_id: a KMS encryption key id or None
    """

    tempfile_name = tempfile.mktemp()
    try:
        with io.open(tempfile_name, 'w') as fp:
            yield fp
        extra_kwargs = extra_kwargs_for_s3_encrypt_key_id(s3_encrypt_key_id=s3_encrypt_key_id,
                                                          client_name='s3_output_stream')
        s3_client.upload_file(Filename=tempfile_name, Bucket=bucket, Key=key, **extra_kwargs)
    finally:
        try:
            os.remove(tempfile_name)
        except Exception:
            pass


@contextlib.contextmanager
def s3_local_file(s3_client, bucket: str, key: str, local_filename: str = None):
    """
    This context manager allows one to write:

        with s3_local_file(s3_client, bucket, key) as file:
            with io.open(local_file, 'r') as fp:
                dictionary = json.load(fp)

    to do input from an s3 bucket.

    Args:
        s3_client: a client object that results from a boto3.client('s3', ...) call.
        bucket: an S3 bucket name
        key: the name of a key within the given S3 bucket
    """
    if local_filename:
        tempdir_name = tempfile.mkdtemp()
        tempfile_name = os.path.join(tempdir_name, os.path.basename(local_filename))
    else:
        ext = os.path.splitext(key)[-1]
        tempfile_name = tempfile.mktemp() + ext
    try:
        s3_client.download_file(Bucket=bucket, Key=key, Filename=tempfile_name)
        yield tempfile_name
    finally:
        try:
            os.remove(tempfile_name)
        except Exception:
            pass


@contextlib.contextmanager
def s3_input_stream(s3_client, bucket: str, key: str, mode: str = 'r'):
    """
    This context manager allows one to write:

        with s3_input_stream(s3_client, bucket, key) as fp:
            dictionary = json.load(fp)

    to do input from an s3 bucket.

    In fact, an intermediate local file is created, copied, and deleted.

    Args:
        s3_client: a client object that results from a boto3.client('s3', ...) call.
        bucket: an S3 bucket name
        key: the name of a key within the given S3 bucket
        mode: an input mode acceptable to io.open
    """

    with s3_local_file(s3_client, bucket, key) as file:
        with io.open(file, mode=mode) as fp:
            yield fp


class SettingsKey:
    APPLICATION_BUCKET_PREFIX = 'application_bucket_prefix'
    BLOB_BUCKET = 'blob_bucket'
    EB_APP_VERSION = 'eb_app_version'
    ELASTICSEARCH_SERVER = 'elasticsearch.server'
    ENCODED_VERSION = 'encoded_version'
    FILE_UPLOAD_BUCKET = 'file_upload_bucket'
    FILE_WFOUT_BUCKET = 'file_wfout_bucket'
    FOURSIGHT_BUCKET_PREFIX = 'foursight_bucket_prefix'
    IDENTITY = 'identity'
    INDEXER = 'indexer'
    INDEXER_NAMESPACE = 'indexer.namespace'
    INDEX_SERVER = 'index_server'
    LOAD_TEST_DATA = 'load_test_data'
    METADATA_BUNDLES_BUCKET = 'metadata_bundles_bucket'
    S3_ENCRYPT_KEY_ID = 's3_encrypt_key_id'
    SNOVAULT_VERSION = 'snovault_version'
    SQLALCHEMY_URL = 'sqlalchemy.url'
    SYSTEM_BUCKET = 'system_bucket'
    TIBANNA_CWLS_BUCKET = 'tibanna_cwls_bucket'
    TIBANNA_OUTPUT_BUCKET = 'tibanna_output_bucket'
    UTILS_VERSION = 'utils_version'


class ExtraArgs:
    SERVER_SIDE_ENCRYPTION = "ServerSideEncryption"
    SSE_KMS_KEY_ID = "SSEKMSKeyId"


def extra_kwargs_for_s3_encrypt_key_id(s3_encrypt_key_id, client_name):

    extra_kwargs = {}
    if s3_encrypt_key_id:
        log.error(f"{client_name} adding SSEKMSKeyId ({s3_encrypt_key_id}) arguments in upload_fileobj call.")
        extra_kwargs["ExtraArgs"] = {
            ExtraArgs.SERVER_SIDE_ENCRYPTION: "aws:kms",
            ExtraArgs.SSE_KMS_KEY_ID: s3_encrypt_key_id,
        }
    else:
        log.error(f"{client_name} found no s3 encrypt key id ({SettingsKey.S3_ENCRYPT_KEY_ID})"
                  f" in request.registry.settings.")

    return extra_kwargs
