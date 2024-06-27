from collections import OrderedDict
from itertools import chain
from typing import Any, Dict, List
from urllib.parse import urlparse

from pyramid.httpexceptions import HTTPNotFound
from pyramid.view import view_config

from .etag import etag_app_version_effective_principals
from .interfaces import (
    COLLECTIONS,
    TYPES,
)
from .util import debug_log
from dcicutils.schema_utils import (
    SchemaConstants as sc,
    get_dependent_required,
    get_properties,
    get_required,
    get_items,
    is_array_schema,
    is_object_schema,
    is_submitter_required,
)
from .project_app import app_project


class SubmissionSchemaConstants:

    ENDPOINT = "/submission-schemas/"

    ALSO_REQUIRES = "also_requires"
    IS_REQUIRED = "is_required"
    PROHIBITED_IF_ONE_OF = "prohibited_if_one_of"
    REQUIRED_IF_NOT_ONE_OF = "required_if_not_one_of"


def includeme(config):
    config.add_route('schemas', '/profiles/')
    config.add_route('schema', '/profiles/{type_name}.json')
    config.add_route('submittables', SubmissionSchemaConstants.ENDPOINT)
    config.add_route(
        'submittable', SubmissionSchemaConstants.ENDPOINT + '{type_name}.json'
    )
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


def _get_conditionally_required_propnames(schema, keyword):
    """
    helper to look for the oneOf/anyOf declarations in the schema -
    for required properties and return a list of those conditionally
    required properties
    """
    propnames = []
    keyword_info = schema.get(keyword, [])
    for info in keyword_info:
        # is a list but examples so far have only one member?
        propnames.extend(info.get(sc.REQUIRED, []))
    return propnames


def _has_property_attr_with_val(propinfo, attrs_to_chk):
    """
    given a property with it's attributes will check against
    a dictionary of attribute names and values and returns true
    if the property has any of the attributes name/values in the dict
    Note: also works if the attr value is a list
    """
    for aname, avalues in attrs_to_chk.items():
        if aname in propinfo:
            propval = propinfo.get(aname)
            if isinstance(propval, list):
                if [pval for pval in propval if pval in avalues]:
                    return True
            elif propval in avalues:
                return True
    return False


def _get_item_name_from_schema_id(schema_id):
    """ this assumes a validly formatted $id value from a schema"""
    return schema_id.replace('/profiles/', '').replace('.json', '')


def _is_submittable_schema(schema_id, schema):
    """
    helper to determine if the schema has potentially submittable fields
    exclude abstract schemas by default
    """
    if schema.get('isAbstract'):
        return False
    # an explicit list of submittable items may be provided
    item_list = app_project().get_submittable_item_names()
    # a property in a schema that indicates this schema is submittable
    # eg. submmitter_id
    key_prop = app_project().get_prop_for_submittable_items()

    if item_list:
        if schema_id:
            item_name = _get_item_name_from_schema_id(schema_id)
            if item_name in item_list:
                return True  # explicitly named item found
    schema_props = get_properties(schema)
    if key_prop:
        prop_names = schema_props.keys()
        if key_prop in prop_names:
            return True  # the property that designates a schema as submittable was found

    return False


def _annotate_submittable_props(schema, props):
    """
    add annotations for requirements and dependencies to
    submittable props based on the schema info
    """
    required_props = get_required(schema)
    oneof_props = _get_conditionally_required_propnames(schema, sc.ONE_OF)
    anyof_props = _get_conditionally_required_propnames(schema, sc.ANY_OF)
    req_deps = get_dependent_required(schema)

    for propname, propinfo in props.items():
        _update_required_annotation(propname, propinfo, required_props)
        if propname in oneof_props:
            lprops = [p for p in oneof_props if p != propname]
            propinfo.setdefault(
                SubmissionSchemaConstants.REQUIRED_IF_NOT_ONE_OF, []
            ).extend(lprops)
            propinfo[SubmissionSchemaConstants.PROHIBITED_IF_ONE_OF] = lprops
        if propname in anyof_props:
            lprops = [p for p in anyof_props if p != propname]
            propinfo.setdefault(
                SubmissionSchemaConstants.REQUIRED_IF_NOT_ONE_OF, []
            ).extend(lprops)
        if propname in req_deps:
            propinfo[SubmissionSchemaConstants.ALSO_REQUIRES] = req_deps[propname]
    return props


def _update_required_annotation(
    property_: str,
    property_schema: Dict[str, Any],
    required_properties: List[str],
) -> None:
    """Add required annotation to property schema if appropriate."""
    if property_ in required_properties or is_submitter_required(property_schema):
        property_schema[SubmissionSchemaConstants.IS_REQUIRED] = True


def _build_embedded_obj(schema, embedded_obj):
    obj_info = {}
    obj_title = embedded_obj.get('title')
    obj_props = _get_submittable_props(schema, get_properties(embedded_obj))
    obj_props = _annotate_submittable_props(schema, obj_props)
    if obj_props:
        obj_info[sc.PROPERTIES] = obj_props
        if obj_title:
            obj_info['title'] = obj_title
    return obj_info


def _get_submittable_props(schema, props):
    """
    Use appproject provided info on properties and properties with certain attributes
    to exclude from submittable properties.  Excluded properties is a list of prop names
    and excluded attibutes is a dictionary of attribute names and values
    """
    excluded_props = app_project().get_properties_for_exclusion()
    exclude_attrs = app_project().get_attributes_for_exclusion()

    submittable_props = {}

    for propname, propinfo in props.items():
        emb_obj = None
        # determine if prop should be submittable
        if propname in excluded_props:  # explicity excluded by name
            continue
        elif _has_property_attr_with_val(propinfo, exclude_attrs):
            continue
        elif is_array_schema(propinfo):  # need to check the attributes of the items
            list_item = get_items(propinfo)
            if is_object_schema(list_item):  # very rare case of list of embedded objects
                emb_obj = _build_embedded_obj(schema, list_item)
                if not emb_obj:
                    continue
                else:
                    propinfo[sc.ITEMS] = emb_obj
                    submittable_props[propname] = propinfo
                    emb_obj = None
            elif _has_property_attr_with_val(list_item, exclude_attrs):
                continue
            else:
                submittable_props[propname] = propinfo
        elif is_object_schema(propinfo):  # infrequent case of embedded object
            emb_obj = _build_embedded_obj(schema, propinfo)
            if emb_obj:
                submittable_props[propname] = emb_obj
        else:
            submittable_props[propname] = propinfo
    return submittable_props


def _get_submittable_schema(schema):
    """
    helper function to take a schema and determine whether it is submittable
    and if so parse info so only submittable fields are included along with hints
    and doc on those fields
    """
    schema_id = schema.get('$id')
    schema_props = get_properties(schema)
    submittable_schema = {}
    if not schema_props:
        return {}

    if not _is_submittable_schema(schema_id, schema):
        return {}

    submittable_props = _get_submittable_props(schema, schema_props)

    if not submittable_props:
        return {}

    submittable_props = _annotate_submittable_props(schema, submittable_props)

    if schema_id:
        submittable_schema['$id'] = schema_id
    submittable_schema['title'] = schema.get('title')
    submittable_schema[sc.PROPERTIES] = submittable_props

    return submittable_schema


@view_config(route_name='submittable', request_method='GET',
             decorator=etag_app_version_effective_principals)
@debug_log
def submittable(context, request):
    schema2chk = schema(context, request)
    submittable_schema = _get_submittable_schema(schema2chk)
    if not submittable_schema:
        raise HTTPNotFound(f'The schema you requested with {request.url} is not submittable or has no submittable fields')
    return submittable_schema


@view_config(route_name='submittables', request_method='GET',
             decorator=etag_app_version_effective_principals)
@debug_log
def submittables(context, request):
    submittable_schemas = {}
    all_schemas = schemas(context, request)
    for name, schema in all_schemas.items():
        submittable_schema = _get_submittable_schema(schema)
        if submittable_schema:
            submittable_schemas[name] = submittable_schema
    if not submittable_schemas:
        raise HTTPNotFound("No submittable schemas found")
    return submittable_schemas
