from collections import OrderedDict
from pyramid.httpexceptions import HTTPNotFound
from pyramid.view import view_config
from .etag import etag_app_version_effective_principals
from .interfaces import (
    COLLECTIONS,
    TYPES,
)
from urllib.parse import urlparse


def includeme(config):
    config.add_route('schemas', '/profiles/')
    config.add_route('schema', '/profiles/{type_name}.json')
    config.scan(__name__)


def _annotated_schema(type_info, request):
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
def schema(context, request):
    type_name = request.matchdict['type_name']
    types = request.registry[TYPES]
    try:
        type_info = types[type_name]
    except KeyError:
        raise HTTPNotFound(type_name)

    return _annotated_schema(type_info, request)


@view_config(route_name='schemas', request_method='GET',
             decorator=etag_app_version_effective_principals)
def schemas(context, request):
    types = request.registry[TYPES]
    schemas = {}
    for type_info in types.by_item_type.values():
        name = type_info.name
        schemas[name] = _annotated_schema(type_info, request)
        schemas[name]['child_types'] = type_info.child_types
    for type_info in types.by_abstract_type.values():
        name = type_info.name
        schemas[name] = _annotated_schema(type_info, request)
        schemas[name]['child_types'] = type_info.child_types
    return schemas
