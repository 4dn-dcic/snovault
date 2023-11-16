from collections import OrderedDict
from itertools import chain
from urllib.parse import urlparse
from pathlib import Path

from pyramid.httpexceptions import HTTPNotFound
from pyramid.view import view_config

from .etag import etag_app_version_effective_principals
from .interfaces import (
    COLLECTIONS,
    TYPES,
)
from .util import debug_log
from .schema_utils import load_schema, favor_app_specific_schema
from .project_app import app_project

def includeme(config):
    config.add_route('schemas', '/profiles/')
    config.add_route('schema', '/profiles/{type_name}.json')
    config.add_route('submittables', '/can-submit/')
    config.add_route('submittable', '/can-submit/{type_name}.json')
    config.scan(__name__)


def _annotated_schema(type_info, request):
    """
    Add some extra annotiation to a schema obtained through given TypeInfo.
    Specifically, adds links to the /terms/ page and inheritance information
    through the `children` and `rdfs:subClassOf` properties. Also flags fields
    by write permission with the `readonly` field, if applicable.
    TODO: add flagging for user restricted fields once role-based field viewing
    is implemented.

    Args:
        type_info (TypeInfo): for an item type. See snovault.type_info.py
        request (Request): the current Request

    Returns:
        dict: the annotated schema
    """
    schema = type_info.schema.copy()
    schema['@type'] = ['JSONSchema']
    jsonld_base = request.registry.settings['snovault.jsonld.terms_namespace']
    schema['rdfs:seeAlso'] = urlparse(jsonld_base).path + type_info.name
    # add links to profiles of children schemas
    schema['children'] = [
        '/profiles/' + t_name + '.json' for t_name in type_info.child_types
    ]

    if type_info.factory is None:
        return schema

    # use first base_type that is not this type itself to handle abstract
    found_subtype = None
    for subtype in type_info.base_types:
        if subtype != type_info.name:
            found_subtype = subtype
            break
    if found_subtype:
        schema['rdfs:subClassOf'] = '/profiles/' + found_subtype + '.json'
    # add abstract flag to know if the profile represents abstract item
    schema['isAbstract'] = type_info.is_abstract

    collection = request.registry[COLLECTIONS][type_info.name]
    properties = OrderedDict()
    # add a 'readonly' flag to fields that the current user cannot write
    for k, v in schema['properties'].items():
        if 'permission' in v:
            if not request.has_permission(v['permission'], collection):
                v = v.copy()
                v['readonly'] = True
        properties[k] = v
    schema['properties'] = properties
    return schema


@view_config(route_name='schema', request_method='GET',
             decorator=etag_app_version_effective_principals)
@debug_log
def schema(context, request):
    """
    /profiles/{type_name}.json -- view for the profile of a specific item type
    A bit inefficient, but need to use the TypeInfo (not AbstractTypeInfo)
    to get the correct schema. To do this, iterate through all registered
    types until we find the one with matching item_type (given by type_name).
    This allows this endpoint to work with item name (e.g. MyItem) or item_type
    (e.g. my_item)
    """
    type_name = request.matchdict['type_name']
    types = request.registry[TYPES]
    found_type_info = None
    all_item_types = chain(types.by_item_type.values(),
                           types.by_abstract_type.values())
    for type_info in all_item_types:
        # handle both item name and item type inputs to the route (both valid)
        if type_info.name == type_name or type_info.item_type == type_name:
            found_type_info = type_info
            break
    if found_type_info is None:
        raise HTTPNotFound(type_name)
    return _annotated_schema(type_info, request)



@view_config(route_name='schemas', request_method='GET',
             decorator=etag_app_version_effective_principals)
@debug_log
def schemas(context, request):
    """
    /profiles/ view for viewing all schemas. Leverages the TypeInfo objects
    for regular classes using registry[TYPES].by_item_type and for abstract
    classes by using registry[TYPES].by_abstract_type
    """
    types = request.registry[TYPES]
    schemas = {}
    all_item_types = chain(types.by_item_type.values(),
                           types.by_abstract_type.values())
    for type_info in all_item_types:
        name = type_info.name
        schemas[name] = _annotated_schema(type_info, request)
    return schemas

def _get_propnames_from_oneof(oneof_info):
    propnames = []
    for oneof in oneof_info:
        # is a list but examples so far have only one member?
        propnames.extend(oneof.get('required', []))
    return propnames


def _has_attr(propinfo, include_attrs):
    for aname, avalues in include_attrs.items():
        if aname in propinfo:
            if propinfo.get(aname) in avalues:
                return True
    return False
            

def _schema_submittable_fields(schema, request, is_embedded_obj=False):
    """
    helper function to take a schema and determine whether it is submittable
    and if so parse info so only submittable fields are included along with hints
    and doc on those fields
    """
    # an explicit list of submittable schemas can also be provided
    schema_list = app_project().get_submittable_schema_names()
    # if defined a field in a schema that indicates this schema is submittable
    # eg. submmitter_id
    key_prop = app_project().get_prop_for_submittable_items()

    # explicit list of propnames to exclude eg. 'last_modified'
    excluded_props = app_project().get_properties_for_exclusion()
    # explicit list of propnames to include no current examples
    included_props = app_project().get_properties_for_inclusion()
    # if a property has an attribute present exclude that property from submit props
    # because there could be different values of an attribute this a dictionary
    # keyed by attribute name with a list of values
    # eg. {'permission':['restricted_field']}
    exclude_attrs = app_project().get_attributes_for_exclusion()
    # explicit attrs to allow inclusion in submit props
    # currently not used but will have same structure as above
    include_attrs = app_project().get_attributes_for_inclusion()

    schema_id = schema.get('$id')
    schema_props = schema.get('properties')

    # first determine if the schema is submittable
    if is_embedded_obj:  # infrequent case from recursive call
        pass
    elif schema_list:
        # an explicit schema list takes preference over a identifying field to determine submittableness    
        if not schema_id:
            return {}
        schema_name = schema_id.replace('/profiles/', '').replace('.json', '')
        if schema_name not in schema_list:
            return {}
    elif key_prop:
        # a property that if in the schema identifies the schema for a submittable item
        if not schema_props:
            return {}
        prop_names = schema_props.keys()
        if key_prop not in prop_names:
            return {}
    else:
        # if neither present in first case say it's not submittable - do we want a different default behavior?
        return{}
    
    # begin to filter and annotate the schema
    required_props = schema.get('required', [])
    oneof_info = schema.get('OneOf', [])
    oneof_props = _get_propnames_from_oneof(oneof_info)
    req_deps = schema.get('dependent_required', {})

    submittable_schema = {}
    if schema_id:
        submittable_schema['$id'] = schema_id
    submittable_schema['title'] = schema.get('title')
    submittable_schema['properties'] = {}

    for propname, propinfo in schema_props.items():
        # determine if prop should be submittable - inclusion trumps exclusion
        if propname in included_props:  # independent of any other attribute
            pass
        elif propname in excluded_props:  # explicity excluded by name
            continue
        elif propinfo.get('type') == 'object':  # infrequent case of embedded object
            emb_obj = _schema_submittable_fields(propinfo, request, is_embedded_obj=True)
        elif _has_attr(propinfo, include_attrs):  # check for explicit attr that indicate inclusion
            pass
        elif _has_attr(propinfo, exclude_attrs):
            continue

        # if we get here annotate and add prop to result
        if propname in required_props:
            propinfo['is_required'] = True
        if propname in oneof_props:  # this a bit wonky (assumes only 2 items in oneof_props)
            propinfo['required_if_not'] = [p for p in oneof_props if p != propname][0]
        if propname in req_deps:
            propinfo['also_requires'] = req_deps[propname]

        submittable_schema['properties'][propname] = propinfo
    
    return submittable_schema


@view_config(route_name='submittable', request_method='GET',
             decorator=etag_app_version_effective_principals)
@debug_log
def submittable(context, request):
    type_name = request.matchdict['type_name']
    schema = load_schema(f"schemas/{type_name}.json")
    return _schema_submittable_fields(schema, request)



@view_config(route_name='submittables', request_method='GET',
             decorator=etag_app_version_effective_principals)
@debug_log
def submittables(context, request):
    types = request.registry[TYPES]
    schemas = {}
    all_item_types = chain(types.by_item_type.values())
    for type_info in all_item_types:
        import pdb; pdb.set_trace()
        filename = favor_app_specific_schema(f"{type_info.item_type}.json")
        if Path(filename).is_file():
            schema = load_schema(f"schemas/{type_info.item_type}.json")
            schemas[type_info.name] = _schema_submittable_fields(schema, request)
    return schemas



