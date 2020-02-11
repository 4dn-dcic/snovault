from collections import OrderedDict
from itertools import chain
from urllib.parse import urlparse

from pyramid.httpexceptions import HTTPNotFound
from pyramid.view import view_config

from .etag import etag_app_version_effective_principals
from .interfaces import (
    COLLECTIONS,
    TYPES,
)
from .util import debug_log


def includeme(config):
    config.add_route('schemas', '/profiles/')
    config.add_route('schema', '/profiles/{type_name}.json')
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
        '/profiles/' +  t_name + '.json' for t_name in type_info.child_types
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
