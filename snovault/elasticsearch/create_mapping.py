"""\
Example.

To load the initial data:

    %(prog)s production.ini

"""

import argparse
import datetime
import json
import logging
import structlog
import time

from collections import OrderedDict
from elasticsearch.exceptions import (
    TransportError,
    RequestError,
    ConnectionTimeout
)
from elasticsearch_dsl import Search
from functools import reduce
from itertools import chain
from pyramid.paster import get_app
from timeit import default_timer as timer
from ..interfaces import COLLECTIONS, TYPES
from dcicutils.log_utils import set_logging
from ..commands.es_index_data import run as run_index_data
from ..schema_utils import combine_schemas
from ..util import add_default_embeds, find_collection_subtypes
from .indexer_utils import get_namespaced_index, find_uuids_for_indexing, get_uuids_for_types
from .interfaces import ELASTIC_SEARCH, INDEXER_QUEUE
from ..settings import Settings


EPILOG = __doc__

log = structlog.getLogger(__name__)

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
# used to disable nested mapping on array of object fields
NESTED_ENABLED = 'enable_nested'
# global index.refresh_interval - currently the default of 1s
REFRESH_INTERVAL = '1s'


def determine_if_is_date_field(field, schema):
    """
    Helper funciton to determine whether a given `schema` for a field is a date.
    TODO: remove unused `field` parameter. Requires search.py change
    """
    is_date_field = False
    if schema.get('format') is not None:
        if schema['format'] == 'date' or schema['format'] == 'date-time':
            is_date_field = True
    elif schema.get('anyOf') is not None and len(schema['anyOf']) > 1:
        is_date_field = True # Will revert to false unless all anyOfs are format date/datetime.
        for schema_option in schema['anyOf']:
            if schema_option.get('format') not in ['date', 'date-time']:
                is_date_field = False
                break
    return is_date_field


def schema_mapping(field, schema, top_level=False, from_array=False):
    """
    Create the mapping for a given schema. Can handle using all fields for
    objects (*), but can handle specific fields using the field parameter.
    This allows for the mapping to match the selective embedding.

    Ultimately responsible for creating the field-level ES mappings for each
    schema property. A typical mapping for a text field would be:

        {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }

    This field has two subfields, 'raw' and 'lower_case_sort', which are both
    keywords that are leveraged in searches. 'lower_case_sort' uses a custom
    normalize to lowercase the keyword value.
    TODO: rename 'lower_case_sort' to 'lowercase' and adjust search code
    """
    type_ = schema['type']

    # Elasticsearch handles multiple values for a field
    if type_ == 'array' and schema['items']:
        return schema_mapping(field, schema['items'], from_array=True)

    if type_ == 'object':
        properties = {}
        for k, v in schema.get('properties', {}).items():
            mapping = schema_mapping(k, v)
            if mapping is not None:
                if field == '*' or k == field:
                    properties[k] = mapping

        # only do this if we said so, allow it to be explicitly disabled as well
        if from_array and Settings.MAPPINGS_USE_NESTED and schema.get(NESTED_ENABLED, False):
            return {
                'type': 'nested',
                'properties': properties
            }
        else:
            return {
                'properties': properties,
            }

    if determine_if_is_date_field(field, schema):
        return {
            'type': 'date',
            'format': "date_optional_time",
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }

    if type_ == ["number", "string"]:
        return {
            'type': 'text',
            'fields': {
                'value': {
                    'type': 'float',
                    'ignore_malformed': True,
                },
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }

    if type_ == 'boolean':
        return {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }

    if type_ == 'string':
        # don't make a mapping for linked objects not within the embedded list
        if 'linkTo' in schema:
            return None

        sub_mapping = {
            'type': 'text',
            'copy_to': ['full_text'],
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }

        return sub_mapping

    if type_ == 'number':
        return {
            'type': 'float',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }

    if type_ == 'integer':
        return {
            'type': 'long',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                },
                'lower_case_sort': {
                    'type': 'keyword',
                    'normalizer': 'case_insensitive',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }


def index_settings():
    """
    Return a dictionary of index settings, which dictate things such as
    shard/replica config per index, as well as filters, analyzers, and
    normalizers. Several settings are configured using global values
    """
    return {
        'index': {
            'number_of_shards': NUM_SHARDS,
            'number_of_replicas': NUM_REPLICAS,
            'max_result_window': SEARCH_MAX,
            'refresh_interval': REFRESH_INTERVAL,  # although we are using the default, let's be explicit about it
            'mapping': {
                'nested_fields': {
                    'limit': 100
                },
                'total_fields': {
                    'limit': 5000
                },
                'depth': {
                    'limit': 30
                }
            },
            'analysis': {
                'filter': {
                    # create tokens between size MIN_NGRAM and MAX_NGRAM
                    'ngram_filter': {
                        'type': 'edgeNGram',
                         'min_gram': MIN_NGRAM,
                         'max_gram': MAX_NGRAM
                    },
                    # truncate tokens to size MAX_NGRAM
                    'truncate_to_ngram': {
                         'type': 'truncate',
                         'length': MAX_NGRAM
                    }
                },
                'analyzer': {
                    # used to analyze `_all` at index time
                    'snovault_index_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'lowercase',
                            'asciifolding',
                            'ngram_filter'
                        ]
                    },
                    # used to analyze `_all` at query time
                    'snovault_search_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'filter': [
                            'lowercase',
                            'asciifolding',
                            'truncate_to_ngram'
                        ]
                    }
                },
                'normalizer': {
                    # keyword fields can use to lowercase on indexing and search
                    'case_insensitive': {
                        'type': 'custom',
                        'filter': ['lowercase']
                    }
                }
            }
        }
    }


def validation_error_mapping():
    """
    Static mapping defined for validation errors built in @@index-data view
    """
    return {
        'location': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        },
        'name': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        },
        'description': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'ignore_above': KW_IGNORE_ABOVE
                }
            }
        }
    }


# generate an index record, which contains a mapping and settings
def build_index_record(mapping, in_type):
    """
    Generate an index record, which is the entire mapping + settings for the
    given index (in_type)

    NOTE: you could disable dynamic mappings globally here, but doing so will break ES
    Item because it relies on the dynamic mappings used for unique keys.
    """
    #mapping['dynamic'] = 'false'  # disable dynamic mappings GLOBALLY, ES demands use of 'false' here
    return {
        'mappings': {in_type: mapping},
        'settings': index_settings()
    }


def es_mapping(mapping, agg_items_mapping):
    """
    Entire Elasticsearch mapping for one item type, including dynamic templates
    and all properties made in the @@index-data view. Takes the item mapping
    and aggregated item mapping as parameters, since those vary by item type.

    Dynamic mappings are disabled within the embedded mapping here
    """
    mapping['dynamic'] = 'false'  # disable dynamic mappings WITHIN embedded, ES demands use of 'false' here
    return {
        'dynamic_templates': [
            {
                'template_principals_allowed': {
                    'path_match': "principals_allowed.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                },
            },
            {
                'template_unique_keys': {
                    'path_match': "unique_keys.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                },
            },
            {
                'template_links': {
                    'path_match': "links.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                },
            },
        ],
        'properties': {
            'full_text': {  # in ES >5 this can be manually created like so
                'type': 'text',
                'analyzer': 'snovault_index_analyzer',  # TODO: a custom analyzer here would be awesome
                'search_analyzer': 'snovault_search_analyzer'
            },
            'uuid': {
                'type': 'keyword',
                'ignore_above': KW_IGNORE_ABOVE
            },
            'sid': {
                'type': 'keyword',
                'ignore_above': KW_IGNORE_ABOVE
            },
            'max_sid': {
                'type': 'keyword',
                'ignore_above': KW_IGNORE_ABOVE
            },
            'item_type': {
                'type': 'keyword',
                'copy_to': ['full_text'],
                'ignore_above': KW_IGNORE_ABOVE
            },
            'embedded': mapping,
            'object': {
                'type': 'object',
                'enabled': False
            },
            'properties': {
                'type': 'object',
                'enabled': False
            },
            'propsheets': {
                'type': 'object',
                'enabled': False
            },
            'principals_allowed': {
                'properties': {
                    'view': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                    'edit': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    }
                }
            },
            'aggregated_items': agg_items_mapping,
            'linked_uuids_embedded': {
                'properties': {
                    'uuid': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                    'sid': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                    'item_type': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    }
                }
            },
            'linked_uuids_object': {
                'properties': {
                    'uuid': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                    'sid': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                    'item_type': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    }
                }
            },
            'rev_link_names': {
                'properties': {
                    'name': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    },
                    'uuids': {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE
                    }
                }
            },
            'rev_linked_to_me': {
                'type': 'keyword',
                'ignore_above': KW_IGNORE_ABOVE
            },
            'validation_errors': {
                'properties': validation_error_mapping()
            },
            'unique_keys': {
                'type': 'object'
            },
            'links': {
                'type': 'object'
            },
            'paths': {
                'type': 'keyword',
                'ignore_above': KW_IGNORE_ABOVE
            },
            'indexing_stats': {  # explicitly map instead of relying on dynamic mappings
                'properties': {
                    'aggregated_items': {
                        'type': 'float'
                    },
                    'embedded_view': {
                        'type': 'float'
                    },
                    'object_view': {
                        'type': 'float'
                    },
                    'paths': {
                        'type': 'float'
                    },
                    'rev_links': {
                        'type': 'float'
                    },
                    'total_indexing_view': {
                        'type': 'float'
                    },
                    'unique_keys': {
                        'type': 'float'
                    },
                    'upgrade_properties': {
                        'type': 'float'
                    },
                    'validation': {
                        'type': 'float'
                    }
                }
            }
        }
    }


def aggregated_items_mapping(types, item_type):
    """
    Create the mapping for the aggregated items of the given type.
    This is a simple mapping, since all values can be set as keywords
    (only used for exact match search and not sorted).
    Since the fields for each aggregated item are split by dots, we organize
    these as the hierarchical objects for Elasticsearch

    Args:
        types: result of request.registry[TYPES]
        item_type: string item type that we are creating the mapping for
    Returns:
        Dictionary mapping for the aggrated_items of the given item type
    """
    type_info = types[item_type]
    aggregated_items = type_info.aggregated_items
    mapping = {'type': 'object'}
    if not aggregated_items:
        return mapping
    del mapping['type']
    mapping['properties'] = aggs_mapping = {}
    for agg_item, agg_fields in aggregated_items.items():
        # include raw field name by convention, though both are keywords
        aggs_mapping[agg_item] = {
            'properties': {
                'parent': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword',
                            'ignore_above': KW_IGNORE_ABOVE
                        }
                    }
                },
                'embedded_path': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword',
                            'ignore_above': KW_IGNORE_ABOVE
                        }
                    }
                },
                'item': {
                    'properties': {}
                }
            }
        }
        # if no agg fields are provided, default to uuid
        if not agg_fields:
            agg_fields = ['uuid']
        aggs_mapping[agg_item]['properties']['item']['properties'] = agg_fields_mapping = {}
        for agg_field in agg_fields:
            # elasticsearch models fields with dots as hierarchical objects
            # must compose our mapping like that
            split_field = agg_field.split('.')
            ptr = agg_fields_mapping
            for idx, split_part in enumerate(split_field):
                if idx == len(split_field) - 1:
                    mapping_val = {
                        'type': 'keyword',
                        'ignore_above': KW_IGNORE_ABOVE,
                        'fields': {
                            'raw': {
                                'type': 'keyword',
                                'ignore_above': KW_IGNORE_ABOVE
                            }
                        }
                    }
                else:
                    mapping_val = {'properties': {}}
                if (split_part not in ptr or
                    ('properties' in mapping_val and 'properties' not in ptr[split_part])):
                    ptr[split_part] = mapping_val
                if 'properties' in ptr[split_part]:
                    ptr = ptr[split_part]['properties']
                else:
                    break
    return mapping


def type_mapping(types, item_type, embed=True):
    """
    Create mapping for each type. This is relatively simple if embed=False.
    When embed=True, the embedded fields (defined in /types/ directory) will
    be used to generate custom embedding of objects. Embedding paths are
    separated by dots. If the last field is an object, all fields in that
    object will be embedded (e.g. biosource.individual). To embed a specific
    field only, do add it at the end of the path: biosource.individual.title

    No field checking has been added yet (TODO?), so make sure fields are
    spelled correctly.

    Any fields that are not objects will NOT be embedded UNLESS they are in the
    embedded list, again defined in the types .py file for the object.
    """
    type_info = types[item_type]
    schema = type_info.schema
    # use top_level parameter here for schema_mapping
    mapping = schema_mapping('*', schema, from_array=False)
    if not embed:
        return mapping

    # process the `embedded_list` to add default fields for embedded objects
    embeds = add_default_embeds(item_type, types, type_info.embedded_list, schema)
    embeds.sort()
    for prop in embeds:
        single_embed = {}
        curr_s = schema
        curr_m = mapping
        split_embed_path = prop.split('.')
        for curr_e in split_embed_path:
            # if we want to map all fields (*), do not drill into schema
            if curr_e != '*':
                # drill into the schemas. if no the embed is not found, break
                subschema = curr_s.get('properties', {}).get(curr_e, None)
                curr_s = merge_schemas(subschema, types)

            if not curr_s:
                break
            curr_m = update_mapping_by_embed(curr_m, curr_e, curr_s)

            # If this is a list of linkTos and has properties to be embedded,
            # make it 'nested' for more aggregations.
            map_with_nested = (Settings.MAPPINGS_USE_NESTED and  # nested must be globally enabled
                               curr_m.get('properties') and
                               curr_e != 'update_items' and
                               curr_e in schema['properties'] and
                               curr_e in mapping['properties'] and
                               schema['properties'][curr_e]['type'] == 'array' and
                               curr_s.get(NESTED_ENABLED, False))  # nested must also be enabled on individual fields
            if map_with_nested:
                curr_m['type'] = 'nested'

    # Copy text fields to 'full_text' so we can still do _all searches
    # TODO: At some point we should filter based on field_name, maybe we add a PHI tag
    #       to relevant fields so that they are not mapped into full_text, for example.
    properties = schema['properties']
    for _, sub_mapping in properties.items():
        if sub_mapping['type'] == 'text':
            sub_mapping['copy_to'] = ['full_text']
    return mapping


def merge_schemas(subschema, types):
    """
    Merge any linked schemas into the current one. Return None if none present
    """
    if not subschema:
        return None
    # handle arrays by simply jumping into them
    # we don't care that they're flattened during mapping
    ref_types = None
    subschema = subschema.get('items', subschema)
    if 'linkTo' in subschema:
        ref_types = subschema['linkTo']
        if not isinstance(ref_types, list):
            ref_types = [ref_types]
    if ref_types is None:
        curr_s = subschema
    else:
        embedded_types = [types[t].schema for t in ref_types
                          if t in types.all]
        if not embedded_types:
            return None
        curr_s = reduce(combine_schemas, embedded_types)
    return curr_s


def update_mapping_by_embed(curr_m, curr_e, curr_s):
    """
    Update the mapping based on the current mapping (curr_m), the current embed
    element (curr_e), and the processed schemas (curr_s).
    when curr_e = '*', it is a special case where all properties are added
    to the object that was previously mapped.
    """
    # see if there's already a mapping associated with this embed:
    # multiple subobjects may be embedded, so be careful here
    mapped = schema_mapping(curr_e, curr_s)
    if curr_e == '*':
        if 'properties' in mapped:
            curr_m['properties'].update(mapped['properties'])
        else:
            curr_m['properties'] = mapped
    elif curr_e in curr_m['properties'] and 'properties' in curr_m['properties'][curr_e]:
        if 'properties' in mapped:
            curr_m['properties'][curr_e]['properties'].update(mapped['properties'])
        else:
            curr_m['properties'][curr_e] = mapped
        curr_m = curr_m['properties'][curr_e]
    else:
        curr_m['properties'][curr_e] = mapped
        curr_m = curr_m['properties'][curr_e]
    return curr_m


def create_mapping_by_type(in_type, registry):
    """
    Return a full mapping for a given doc_type of in_type
    """
    # build a schema-based hierarchical mapping for embedded view
    collection = registry[COLLECTIONS].by_item_type[in_type]
    embed_mapping = type_mapping(registry[TYPES], collection.type_info.item_type)
    agg_items_mapping = aggregated_items_mapping(registry[TYPES], collection.type_info.item_type)
    # finish up the mapping
    return es_mapping(embed_mapping, agg_items_mapping)


def build_index(app, es, index_name, in_type, mapping, uuids_to_index, dry_run,
                check_first=False,index_diff=False, print_count_only=False):
    """
    Creates an es index for the given `in_type` with the given mapping and
    settings defined by item_settings(). Delete existing index first.
    Adds uuids from the given collection to `uuids_to_index`.
    Some options:
    - If `check_first` is True, will compare the given mapping with the found
      mapping and item counts for the index, and skip creating it if possible.
    - If `index_diff` is True, do not remove the existing index and instead
      only add any missing items to `uuids_to_index`
    """
    uuids_to_index[in_type] = set()
    if print_count_only:
        log.info('___PRINTING COUNTS___')
        check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff, True)
        return

    # combines mapping and settings
    this_index_record = build_index_record(mapping, in_type)

    if dry_run:
        log.info('___DRY RUN___')
        log.info('MAPPING: would use the attached mapping/settings for index %s' % (in_type),
                 collection=in_type, mapping=this_index_record)
        return

    # determine if index already exists for this type
    this_index_exists = check_if_index_exists(es, index_name)

    # if the index exists, we might not need to delete it
    # otherwise, run if we are using the check-first or index_diff args
    if ((check_first or index_diff) and this_index_exists
        and compare_against_existing_mapping(es, index_name, in_type, this_index_record, True)):
        check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff)
        log.info('MAPPING: using existing index for collection %s' % (in_type), collection=in_type)
        return

    # if index_diff and we've made it here, the mapping must be off
    if index_diff:
        log.error('MAPPING: cannot index-diff for index %s due to differing mappings'
                  % (in_type), collection=in_type)
        return

    # delete the index. Ignore 404 because new item types will not be present
    if this_index_exists:
        res = es_safe_execute(es.indices.delete, index=index_name, ignore=[404])
        if res is not None:
            if res.get('status') == 404:
                log.info('MAPPING: index %s not found and cannot be deleted' % in_type,
                         collection=in_type)
            else:
                assert res.get('acknowledged') is True
                log.info('MAPPING: index successfully deleted for %s' % in_type,
                         collection=in_type)
        else:
            log.error('MAPPING: error on delete index for %s' % in_type, collection=in_type)

    # first, create the mapping. adds settings and mappings in the body
    res = es_safe_execute(es.indices.create, index=index_name, body=this_index_record)
    if res is not None:
        assert res.get('acknowledged') is True
        log.info('MAPPING: new index created for %s' % (in_type), collection=in_type)
    else:
        log.error('MAPPING: new index failed for %s' % (in_type), collection=in_type)

    # check to debug create-mapping issues and ensure correct mappings
    confirm_mapping(es, index_name, in_type, this_index_record)

    # we need to queue items in the index for indexing
    # if check_first and we've made it here, nothing has been queued yet
    # for this collection
    start = timer()
    coll_uuids = set(get_uuids_for_types(app.registry, types=[in_type]))
    end = timer()
    log.info('Time to get collection uuids: %s' % str(end-start), cat='fetch time',
             duration=str(end-start), collection=in_type)
    uuids_to_index[in_type] = coll_uuids
    log.info('MAPPING: will queue all %s items in the new index %s for reindexing' %
             (len(coll_uuids), in_type), cat='items to queue', count=len(coll_uuids), collection=in_type)


def check_if_index_exists(es, in_type):
    return es_safe_execute(es.indices.exists, index=in_type)


def check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff=False, print_counts=False):
    """
    lastly, check to make sure the item count for the existing
    index matches the database document count. If not, queue the uuids_to_index
    in the index for reindexing.
    If index_diff, store uuids for reindexing that are in DB but not ES
    """
    db_count, es_count, db_uuids, diff_uuids = get_db_es_counts_and_db_uuids(app, es, in_type, index_diff)
    log.info("DB count is %s and ES count is %s for index: %s" %
                (str(db_count), str(es_count), in_type), collection=in_type,
                 db_count=str(db_count), cat='collection_counts', es_count=str(es_count))
    if print_counts:  # just display things, don't actually queue the uuids
        if index_diff and diff_uuids:
            log.info("The following UUIDs are found in the DB but not the ES index: %s\n%s"
                        % (in_type, diff_uuids), collection=in_type)
        return
    if es_count is None or es_count != db_count:
        if index_diff:
            log.info('MAPPING: queueing %s items found in DB but not ES in the index %s for reindexing'
                        % (str(len(diff_uuids)), in_type), items_queued=str(len(diff_uuids)), collection=in_type)
            uuids_to_index[in_type] = diff_uuids
        else:
            log.info('MAPPING: queueing %s items found in the existing index %s for reindexing'
                        % (str(len(db_uuids)), in_type), items_queued=str(len(db_uuids)), collection=in_type)
            uuids_to_index[in_type] = db_uuids


def get_db_es_counts_and_db_uuids(app, es, in_type, index_diff=False):
    """
    Return the database count and elasticsearch count for a given item type,
    the list of collection uuids from the database, and the list of uuids
    found in the DB but not in the ES store.
    """
    namespaced_index = get_namespaced_index(app, in_type)
    if check_if_index_exists(es, namespaced_index):
        if index_diff:
            search = Search(using=es, index=namespaced_index, doc_type=in_type)
            search_source = search.source([])
            es_uuids = set([h.meta.id for h in search_source.scan()])
            es_count = len(es_uuids)
        else:
            count_res = es.count(index=namespaced_index, doc_type=in_type)
            es_count = count_res.get('count')
            es_uuids = set()
    else:
        es_count = 0
        es_uuids = set()
    db_uuids = set(get_uuids_for_types(app.registry, types=[in_type]))
    db_count = len(db_uuids)
    # find uuids in the DB but not ES (set operations)
    if index_diff:
        diff_uuids = db_uuids - es_uuids
    else:
        diff_uuids = set()
    return db_count, es_count, db_uuids, diff_uuids


def find_and_replace_dynamic_mappings(new_mapping, found_mapping):
    """
    Needed to compare a newly created mapping and a mapping found in ES,
    since unmapped objects will be automatically mapped by elasticsearch.
    An example is `links` object, which we don't explictly map.

    Recursively move through the new mapping to find such objects and then
    replace them in the found mapping. Modifies both mappings in place
    """
    # identify dynamic mappings created by additionalProperties and remove
    possible_add_properties = set(found_mapping) - set(new_mapping)
    for add_key in possible_add_properties:
        # know it's a dynamic mapping if 'raw' field is not present...
        # ... or if type is not keyword/object and no fields/properties are defined
        if (
            ('fields' in found_mapping[add_key]
             and 'raw' not in found_mapping[add_key]['fields']) or
            ('fields' not in found_mapping[add_key]
             and 'properties' not in found_mapping[add_key]
             and found_mapping[add_key].get('type') not in ['keyword', 'object'])
        ):
            del found_mapping[add_key]

    for key, new_val in new_mapping.items():
        if key not in found_mapping:
            continue
        found_val = found_mapping[key]
        if ((new_val.get('type') == 'object' and 'properties' not in new_val)
            or (new_val.get('properties') == {} and 'type' not in new_val)):
            if found_val.get('properties') is not None and 'type' not in found_val:
                # this was an dynamically created mapping. Reset it
                del found_val['properties']
                found_val['type'] = 'object'

        # drill down into further properties
        if new_val.get('properties'):
            find_and_replace_dynamic_mappings(new_val['properties'], found_val.get('properties', {}))


def compare_against_existing_mapping(es, index_name, in_type, this_index_record, live_mapping=False):
    """
    Compare the given index mapping and compare it to the existing mapping
    in an index. Return True if they are the same, False otherwise.
    Use live_mapping=True when the existing mapping from the index may have been
    automatically changed through ES dynamic mapping when documents were added.
    In this case, attempt to revert the obtained mapping to its original state
    using `find_and_replace_dynamic_mappings` so that it can be compared with
    the new mapping.

    Args:
        es: current Elasticsearch client
        in_type (str): item type of current index
        this_index_record (dict): record of current index, with mapping and settings
        live_mapping (bool): if True, compare new mapping to live one and remove
            dynamically-created mappings

    Returns:
        bool: True if new mapping is the same as the live mapping
    """
    found_mapping = es.indices.get_mapping(index=index_name).get(index_name).get('mappings', {})
    new_mapping = this_index_record['mappings']
    if live_mapping:
        find_and_replace_dynamic_mappings(new_mapping[in_type]['properties'],
                                          found_mapping[in_type]['properties'])
    # dump to JSON to compare the mappings
    found_map_json = json.dumps(found_mapping, sort_keys=True)
    new_map_json = json.dumps(new_mapping, sort_keys=True)
    # es converts {'properties': {}} --> {'type': 'object'}
    new_map_json = new_map_json.replace('{"properties": {}}', '{"type": "object"}')
    return found_map_json == new_map_json


def confirm_mapping(es, index_name, in_type, this_index_record):
    """
    The mapping put to ES can be incorrect, most likely due to residual
    items getting indexed at the time of index creation. This loop serves
    to find those problems and correct them, as well as provide more info
    for debugging the underlying issue.
    Returns number of iterations this took (0 means initial mapping was right)
    """
    mapping_check = False
    tries = 0
    while not mapping_check and tries < 5:
        if compare_against_existing_mapping(es, index_name, in_type, this_index_record):
            mapping_check = True
        else:
            count = es.count(index=index_name, doc_type=in_type).get('count', 0)
            log.info('___BAD MAPPING FOUND FOR %s. RETRYING___\nDocument count in that index is %s.'
                        % (in_type, count), collection=in_type, count=count, cat='bad mapping')
            es_safe_execute(es.indices.delete, index=index_name)
            # do not increment tries if an error arises from creating the index
            try:
                es_safe_execute(es.indices.create, index=index_name, body=this_index_record)
            except (TransportError, RequestError) as e:
                log.info('___COULD NOT CREATE INDEX FOR %s AS IT ALREADY EXISTS.\nError: %s\nRETRYING___'
                            % (in_type, str(e)), collection=in_type, cat='index already exists')
            else:
                tries += 1
            time.sleep(2)
    if not mapping_check:
        log.info('___MAPPING CORRECTION FAILED FOR %s___' % in_type, cat='correction', collection=in_type)
    return tries


def es_safe_execute(function, **kwargs):
    """
    Tries to execute the function 3 times, handling ES ConnectionTimeout
    Returns the response or None if could not execute
    """
    exec_count = 0
    res = None
    while exec_count < 3:
        try:
            res = function(**kwargs)
        except ConnectionTimeout:
            exec_count += 1
            log.info('ES connection issue! Retrying.')
        else:
            break
    return res


def flatten_and_sort_uuids(registry, uuids_to_index, item_order):
    """
    Flatten the input dict of sets (uuids_to_index) into a list that is ordered
    based off of item type, which is provided through item_order.
    item_order may be a list of item types (e.g. my_type) or item names
    (e.g. MyType)

    Args:
        reigstry: current Pyramid Registry
        uuids_to_index (set): keys are item_type and values are set of uuids
        item_order (list): string item types / item names to order by

    Returns:
        list: ordered uuids to index synchronously or queue for indexing
    """
    # arg default of [] can be dangerous
    if item_order is None:
        item_order = []
    # process item_order to turn item names to item types
    proc_item_order = []
    for name_or_type in item_order:
        try:
            i_type = registry[COLLECTIONS][name_or_type].type_info.item_type
        except KeyError:
            # not an item name or type. Log error and exclude
            log.error('___Entry %s is not valid in mapping item_order. Skipping___' % name_or_type)
        else:
            proc_item_order.append(i_type)
    to_index_list = []

    def type_sort_key(i_type):
        """
        Simple helper fxn to sort collections by their index in item_order.
        If not in item_order, preserve order as-is
        """
        try:
            res = proc_item_order.index(i_type)
        except ValueError:
            res = 999
        return res

    # use type_sort_key fxn to sort + flatten uuids_to_index
    for itype in sorted(uuids_to_index.keys(), key=type_sort_key):
        to_index_list.extend(uuids_to_index[itype])
    return to_index_list


def run_indexing(app, indexing_uuids):
    """
    indexing_uuids is a set of uuids that should be reindexed. If global args
    are available, then this will spawn a new process to run indexing with.
    Otherwise, run with the current INDEXER
    """
    run_index_data(app, uuids=indexing_uuids)


def run(app, collections=None, dry_run=False, check_first=False, skip_indexing=False,
        index_diff=False, strict=False, sync_index=False, print_count_only=False,
        purge_queue=False, item_order=None):
    """
    Run create_mapping. Has the following options:
    collections: run create mapping for the given list of item types only.
    dry_run: if True, do not delete/create indices
    skip_indexing: if True, do not index ANYTHING with this run.
    check_first: if True, attempt to keep indices that have not changed mapping.
        If the document counts in the index and db do not match, delete index
        and queue all items in the index for reindexing.
    index_diff: if True, do NOT create/delete indices but identify any items
        that exist in db but not in es and reindex those.
        Takes precedence over check_first
    strict: if True, do not include associated items when considering what
        items to reindex. Only takes affect with index_diff or when specific
        item_types are specified, since otherwise a complete reindex will
        occur anyways.
    sync_index: if True, synchronously run reindexing rather than queueing.
    print_count_only: if True, print counts for existing indices instead of
        queueing items for reindexing. Must to be used with check_first.
    purge_queue: if True, purge the contents of all relevant indexing queues.
        Is automatically done on a full indexing (no index_diff, check_first,
        or collections).
    item_order: provide a list of item types (e.g. my_type) or item names
        (e.g. MyType). Indexing/queueing order will be dictated by index in the
        list, such that the items at the front are indexed first.
    """
    overall_start = timer()
    registry = app.registry
    es = registry[ELASTIC_SEARCH]
    indexer_queue = registry[INDEXER_QUEUE]
    cat = 'start create mapping'

    # always overwrite telemetry id
    global log
    telemetry_id='cm_run_' + datetime.datetime.now().isoformat()
    log = log.bind(telemetry_id=telemetry_id)
    log.info('\n___CREATE-MAPPING___:\ncollections: %s\ncheck_first %s\n index_diff %s\n' %
                (collections, check_first, index_diff), cat=cat)
    log.info('\n___ES___:\n %s\n' % (str(es.cat.client)), cat=cat)
    log.info('\n___ES NODES___:\n %s\n' % (str(es.cat.nodes())), cat=cat)
    log.info('\n___ES HEALTH___:\n %s\n' % (str(es.cat.health())), cat=cat)
    log.info('\n___ES INDICES (PRE-MAPPING)___:\n %s\n' % str(es.cat.indices()), cat=cat)
    # keep track of uuids to be indexed after mapping is done.
    # Set of uuids for each item type; keyed by item type. Order for python < 3.6
    uuids_to_index = OrderedDict()
    total_reindex = (collections is None and not dry_run and not check_first
                     and not index_diff and not print_count_only)

    if not collections:
        collections = list(registry[COLLECTIONS].by_item_type)

    # clear the indexer queue on a total reindex
    namespaced_index = get_namespaced_index(app, 'indexing')
    if total_reindex or purge_queue:
        log.info('___PURGING THE QUEUE AND CLEARING INDEXING RECORDS BEFORE MAPPING___\n', cat=cat)
        indexer_queue.purge_queue()
        # we also want to remove the 'indexing' index, which stores old records
        # it's not guaranteed to be there, though
        es_safe_execute(es.indices.delete, index=namespaced_index, ignore=[404])

    # if 'indexing' index doesn't exist, initialize it with some basic settings
    # but no mapping. this is where indexing_records go
    if not check_if_index_exists(es, namespaced_index):
        idx_settings = {'settings': index_settings()}
        es_safe_execute(es.indices.create, index=namespaced_index, body=idx_settings)

    greatest_mapping_time = {'collection': '', 'duration': 0}
    greatest_index_creation_time = {'collection': '', 'duration': 0}
    timings = {}
    log.info('\n___FOUND COLLECTIONS___:\n %s\n' % (str(collections)), cat=cat)
    for collection_name in collections:
        # do NOT redo indices for collections that use ES as a primary datastore,
        # since this will cause loss of data. Only run in such cases if the index is empty
        if registry[COLLECTIONS][collection_name].properties_datastore == 'elasticsearch':
            namespaced_index = get_namespaced_index(app, collection_name)
            if check_if_index_exists(es, namespaced_index):
                count_res = es.count(index=namespaced_index, doc_type=collection_name)
                if count_res.get('count', 0) > 0:
                    log.info('Skipping %s mapping since it is an ES-based '
                             'collection with items in it' % collection_name)
                    continue
        start = timer()
        mapping = create_mapping_by_type(collection_name, registry)
        mapping_time = timer() - start
        start = timer()
        namespaced_index = get_namespaced_index(app, collection_name)
        build_index(app, es, namespaced_index, collection_name, mapping, uuids_to_index,
                    dry_run, check_first, index_diff, print_count_only)
        index_time = timer() - start
        log.info('___FINISHED %s___\n' % (collection_name))
        log.info('___Mapping Time: %s  Index time %s ___\n' % (mapping_time, index_time),
                    cat='index mapping time', collection=collection_name, map_time=mapping_time,
                    index_time=index_time)
        if mapping_time > greatest_mapping_time['duration']:
            greatest_mapping_time['collection'] = collection_name
            greatest_mapping_time['duration'] = mapping_time
        if index_time > greatest_index_creation_time['duration']:
            greatest_index_creation_time['collection'] = collection_name
            greatest_index_creation_time['duration'] = index_time
        timings[collection_name] = {'mapping': mapping_time, 'index': index_time}

    overall_end = timer()
    cat = 'finished mapping'
    log.info('\n___ES INDICES (POST-MAPPING)___:\n %s\n' % (str(es.cat.indices())), cat=cat)
    log.info('\n___FINISHED CREATE-MAPPING___\n', cat=cat)


    log.info('\n___GREATEST MAPPING TIME: %s\n' % greatest_mapping_time,
                cat='max mapping time', **greatest_mapping_time)
    log.info('\n___GREATEST INDEX CREATION TIME: %s\n' % greatest_index_creation_time,
                cat='max index create time', **greatest_index_creation_time)
    log.info('\n___TIME FOR ALL COLLECTIONS: %s\n' % (overall_end - overall_start),
                cat='overall mapping time', duration=str(overall_end - overall_start))
    if skip_indexing or print_count_only:
        return timings

    # now, queue items for indexing in the secondary queue
    # get a total list of all uuids to index among types for invalidation checking
    len_all_uuids = sum([len(uuids_to_index[i_type]) for i_type in uuids_to_index])
    if uuids_to_index:
        # only index (synchronously) if --sync-index option is used
        if sync_index:
            # using sync_index and NOT strict could cause issues with picking
            # up newly rev linked items. Print out an error and deal with it
            # for now
            if not strict:
                # XXX: this used to check if len(uuids) > 50000 and if so trigger a full reindex
                #      no idea why such a thing was needed/desired -will 4-16-2020
                if total_reindex:
                    log.warning('___MAPPING ALL ITEMS WITH STRICT=TRUE TO SAVE TIME___')
                    # get all the uuids from EVERY item type
                    for i_type in registry[COLLECTIONS].by_item_type:
                        uuids_to_index[i_type] = set(get_uuids_for_types(registry, types=[i_type]))
                else:
                    # find invalidated uuids for each index. Must concat all
                    # uuids over all types in uuids_to_index to do this
                    all_uuids_to_index = set(chain.from_iterable(uuids_to_index.values()))
                    for i_type in registry[COLLECTIONS].by_item_type:
                        if not check_if_index_exists(es, i_type):
                            continue
                        # must subtract the input uuids that are not of the given type
                        to_subtract = set(chain.from_iterable(
                            [v for k, v in uuids_to_index.items() if k != i_type]
                        ))
                        # NOTE: invalidation scope computation not possible here since there is no set of diffs
                        all_assc_uuids, _ = find_uuids_for_indexing(registry, all_uuids_to_index, i_type)
                        uuids_to_index[i_type] = all_assc_uuids - to_subtract
                log.error('___SYNC INDEXING WITH STRICT=FALSE MAY CAUSE REV_LINK INCONSISTENCY___')
            # sort by-type uuids into one list and index synchronously
            to_index_list = flatten_and_sort_uuids(app.registry, uuids_to_index, item_order)
            log.info('\n___UUIDS TO INDEX (SYNC)___: %s\n' % len(to_index_list),
                        cat='uuids to index', count=len(to_index_list))
            run_indexing(app, to_index_list)
        else:
            # if non-strict and attempting to reindex a ton, it is faster
            # just to strictly reindex all items
            use_strict = strict or total_reindex
            if len_all_uuids > 50000 and not use_strict:
                log.warning('___MAPPING ALL ITEMS WITH STRICT=TRUE TO SAVE TIME___')
                # get all the uuids from EVERY item type
                for i_type in registry[COLLECTIONS].by_item_type:
                    uuids_to_index[i_type] = set(get_uuids_for_types(registry, types=[i_type]))
                use_strict = True
            # sort by-type uuids into one list and queue for indexing
            to_index_list = flatten_and_sort_uuids(app.registry, uuids_to_index, item_order)
            log.info('\n___UUIDS TO INDEX (QUEUED)___: %s\n' % len(to_index_list),
                        cat='uuids to index', count=len(to_index_list))
            indexer_queue.add_uuids(app.registry, to_index_list, strict=use_strict,
                                    target_queue='secondary', telemetry_id=telemetry_id)
    return timings


def main():
    parser = argparse.ArgumentParser(
        description="Create Elasticsearch mapping", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('--item-type', action='append', help="Item type")
    parser.add_argument('--dry-run', action='store_true',
                        help="Don't post to ES, just print")
    parser.add_argument('--check-first', action='store_true',
                        help="check if index exists first before attempting creation")
    parser.add_argument('--skip-indexing', action='store_true',
                        help="skip all indexing if set")
    parser.add_argument('--index-diff', action='store_true',
                        help="reindex any items in the db but not es store for all/given collections")
    parser.add_argument('--strict', action='store_true',
                        help="used with check_first in combination with item-type. Only index the given types (ignore associated items). Advanced users only")
    parser.add_argument('--sync-index', action='store_true',
                        help="add to trigger synchronous indexing instead of queued")
    parser.add_argument('--print-count-only', action='store_true',
                        help="use with check_first to only print counts")
    parser.add_argument('--purge-queue', action='store_true',
                        help="purge the contents of all queues, regardless of run mode")

    args = parser.parse_args()

    app = get_app(args.config_uri, args.app_name)

    # Loading app will have configured from config file. Reconfigure here:
    # Use `es_server=app.registry.settings.get('elasticsearch.server')` when ES logging is working
    set_logging(in_prod=app.registry.settings.get('production'), level=logging.INFO)

    uuids = run(app, collections=args.item_type, dry_run=args.dry_run, check_first=args.check_first,
                skip_indexing=args.skip_indexing, index_diff=args.index_diff, strict=args.strict,
                sync_index=args.sync_index, print_count_only=args.print_count_only, purge_queue=args.purge_queue)
    return


if __name__ == '__main__':
    main()
