import os
import codecs
import collections
import io
import json
import uuid
import re
import pkg_resources

from datetime import datetime
from dcicutils.misc_utils import ignored
from dcicutils.bundle_utils import SchemaManager
from snovault.schema_validation import SerializingSchemaValidator
from jsonschema import FormatChecker
from jsonschema import RefResolver
from jsonschema.exceptions import ValidationError, RefResolutionError
from pyramid.path import AssetResolver, caller_package
from pyramid.settings import asbool
from pyramid.threadlocal import get_current_request
from pyramid.traversal import find_resource
from uuid import UUID
from .project_app import app_project
from .util import ensurelist

# This was originally an internal import from "." (__init__.py), but I have replaced that reference
# to avoid a circularity. No files should refer upward to "." for imports. -kmp 9-Jun-2020
from .resources import Item, COLLECTIONS


SERVER_DEFAULTS = {}


# TODO: Shouldn't this return func? Otherwise this:
#           @server_default
#           def foo(instance, subschema):
#               return ...something...
#       does (approximately):
#           SERVER_DEFAULTS['foo'] = lambda(instance, subschema): ...something...
#           server_default = None
#       It feels like the function should still get defined. -kmp 17-Feb-2023
def server_default(func):
    SERVER_DEFAULTS[func.__name__] = func


class NoRemoteResolver(RefResolver):
    def resolve_remote(self, uri):
        """ Resolves remote uri for files so we can cross reference across our own
            repos, which now contain base schemas we may want to use
        """
        if any(s in uri for s in ['http', 'https', 'ftp', 'sftp']):
            raise ValueError(f'Resolution disallowed for: {uri}')
        else:
            return load_schema(uri)


def favor_app_specific_schema(schema: str) -> str:
    """
    If the given schema refers to a schema (file) which exists in the app-specific schemas
    package/directory then favor that version of the file over the local version by returning
    a reference to that schema; otherwise just returns the given schema.

    For example, IF the given schema is snovault:access_key.json AND the current app is fourfront AND
    if the file encoded/schemas/access_key.json exists THEN returns: encoded:schemas/access_key.json

    This uses the dcicutils.project_utils mechanism to get the app-specific file/path name.
    """
    if isinstance(schema, str):
        schema_parts = schema.split(":")
        schema_project = schema_parts[0] if len(schema_parts) > 1 else None
        if schema_project != app_project().PACKAGE_NAME:
            schema_filename = schema_parts[1] if len(schema_parts) > 1 else schema_parts[0]
            app_specific_schema_filename = app_project().project_filename(f"/{schema_filename}")
            if os.path.exists(app_specific_schema_filename):
                schema = f"{app_project().PACKAGE_NAME}:{schema_filename}"
    return schema


def favor_app_specific_schema_ref(schema_ref: str) -> str:
    """
    If the given schema_ref refers to a schema (file) which exists in the app-specific schemas
    directory, AND it contains the specified element, then favor that version of the file over the
    local version by returning a reference to that schema; otherwise just returns the given schema_ref.

    For example, IF the given schema is mixins.json#/modified AND the current app is fourfront
    AND if the file encoded/schemas/mixins.json exists AND if that file contains the modified
    element THEN returns: file:///full-path-to/encoded/schemas/mixins.json#/modified

    This uses the dcicutils.project_utils mechanism to get the app-specific file/path name.
    """
    def json_file_contains_element(json_filename: str, json_element: str) -> bool:
        """
        If the given JSON file exists and contains the given JSON element name then
        returns True; otherwise returns False. The given JSON element may or may
        not begin with a slash. Currently only looks at one single top-level element.
        """
        if json_filename and json_element:
            try:
                with io.open(json_filename, "r") as json_f:
                    json_content = json.load(json_f)
                    json_element = json_element.strip("/")
                    if json_element:
                        if json_content.get(json_element):
                            return True
            except Exception:
                pass
        return False

    if isinstance(schema_ref, str):
        schema_parts = schema_ref.split("#")
        schema_filename = schema_parts[0]
        app_specific_schema_filename = app_project().project_filename(f"/schemas/{schema_filename}")
        if os.path.exists(app_specific_schema_filename):
            schema_element = schema_parts[1] if len(schema_parts) > 1 else None
            if schema_element:
                if json_file_contains_element(app_specific_schema_filename, schema_element):
                    schema_ref = f"file://{app_specific_schema_filename}#{schema_element}"
            else:
                schema_ref = f"file://{app_specific_schema_filename}"
    return schema_ref


def fetch_schema_by_package_name(package_name: str, schema_name: str) -> dict:
    """ Uses the pkg_resources library (good through 3.11) to resolve schemas in other
        packages """
    try:
        schema_data = pkg_resources.resource_string(package_name, schema_name)
        schema = json.loads(schema_data)
        return schema
    except Exception as e:
        raise RefResolutionError(f"Failed to fetch schema by package name: {str(e)}")


def fetch_field_from_schema(schema: dict, ref_identifer: str) -> dict:
    """ Fetches a field from a schema given the format in our $merge definitions ie:
        /properties/access_key_id -> schema.get('properties', {}).get('access_key_id', {})
    """
    split_path = ref_identifer.split('/')[1:]
    resolved = None
    for subpart in split_path:
        resolved = schema.get(subpart, {})
        schema = resolved
    if not resolved:
        raise RefResolutionError(f'Could not locate $merge ref {ref_identifer} in schema')
    if not isinstance(resolved, dict):
        raise ValueError(
            f'Schema ref {ref_identifer} must resolve dict, not {type(resolved)}'
        )
    return resolved


MERGE_PATTERN = r'^([^:]+):(.+[.]json)#/(.+)$'


def match_merge_syntax(merge):
    """ Function that matches possible syntax for merge structure """
    return re.match(MERGE_PATTERN, merge)


def extract_schema_from_ref(ref: str) -> dict:
    """ Implements some special logic for extracting the package_name and path
        of a given $merge ref value
    """
    if not match_merge_syntax(ref):
        raise RefResolutionError(f'Ref {ref} does match regex {MERGE_PATTERN}')
    [package_name, path] = ref.split(':')
    [path, ref] = path.split('#')
    schema = fetch_schema_by_package_name(package_name, path)
    return fetch_field_from_schema(schema, ref)


def resolve_merge_ref(ref, resolver):
    """ Resolves fields that have a $merge value - must be formatted like the below:
        snovault:schemas/access_key.json#/properties/access_key_id

        The ':' character denotes package (always required)
        The '#' character denotes field path ie: how to traverse once schema is resolved
    """
    try:
        with resolver.resolving(ref) as resolved:
            if not isinstance(resolved, dict):
                raise ValueError(
                    f'Schema ref {ref} must resolve dict, not {type(resolved)}'
                )
            return resolved
    except RefResolutionError:  # try again under a different method
        return extract_schema_from_ref(ref)


def _update_resolved_data(resolved_data, value, resolver):
    # Assumes resolved value is dictionary.
    resolved_data.update(
        # Recurse here in case the resolved value has refs.
        resolve_merge_refs(
            # Actually get the ref value.
            resolve_merge_ref(value, resolver),
            resolver
        )
    )


def _handle_list_or_string_value(resolved_data, value, resolver):
    if isinstance(value, list):
        for v in value:
            _update_resolved_data(resolved_data, v, resolver)
    else:
        _update_resolved_data(resolved_data, value, resolver)


def resolve_merge_refs(data, resolver):
    if isinstance(data, dict):
        # Return copy.
        resolved_data = {}
        for k, v in data.items():
            if k == '$merge':
                _handle_list_or_string_value(resolved_data, v, resolver)
            else:
                resolved_data[k] = resolve_merge_refs(v, resolver)
    elif isinstance(data, list):
        # Return copy.
        resolved_data = [
            resolve_merge_refs(v, resolver)
            for v in data
        ]
    else:
        # Assumes we're only dealing with other JSON types
        # like string, number, boolean, null, not other
        # types like tuples, sets, functions, classes, etc.,
        # which would require a deep copy.
        resolved_data = data
    return resolved_data


def fill_in_schema_merge_refs(schema, resolver):
    """ Resolves $merge properties, custom $ref implementation from IGVF SNO2-6 """
    return resolve_merge_refs(schema, resolver)


def mixinSchemas(schema, resolver, key_name='properties'):
    mixinKeyName = 'mixin' + key_name.capitalize()
    mixins = schema.get(mixinKeyName)
    if mixins is None:
        return schema
    properties = collections.OrderedDict()
    bases = []
    for mixin in reversed(mixins):
        ref = mixin.get('$ref')
        if ref is not None:
            # For mixins check if there is an associated app-specific
            # schema file and favor that over the local one if any.
            # TODO: This may be controversial and up for discussion. 2023-05-27
            ref = favor_app_specific_schema_ref(ref)
            with resolver.resolving(ref) as resolved:
                mixin = resolved
        bases.append(mixin)
    for base in bases:
        for name, base_prop in base.items():
            prop = properties.setdefault(name, {})
            for k, v in base_prop.items():
                if k not in prop:
                    prop[k] = v
                    continue
                if prop[k] == v:
                    continue
                if key_name == 'facets':
                    continue  # Allow schema facets to override, as well.
                raise ValueError('Schema mixin conflict for %s/%s' % (name, k))
    # Allow schema properties to override
    base = schema.get(key_name, {})
    for name, base_prop in base.items():
        prop = properties.setdefault(name, {})
        for k, v in base_prop.items():
            prop[k] = v
    schema[key_name] = properties
    return schema


def linkTo(validator, linkTo, instance, schema):
    # 2024-02-21/dmichaels:
    # New skip_links functionality for smaht-submitr since it does link integrity checking.
    skip_links = (request := get_current_request()) and asbool(request.params.get('skip_links', False))
    if skip_links:
        return
    if not validator.is_type(instance, "string"):
        return

    request = get_current_request()
    collections = request.registry[COLLECTIONS]
    if validator.is_type(linkTo, "string"):
        base = collections.get(linkTo, request.root)
        linkTo = [linkTo] if linkTo else []
    elif validator.is_type(linkTo, "array"):
        base = request.root
    else:
        raise Exception("Bad schema")  # raise some sort of schema error

    try:
        item = find_resource(base, instance.replace(':', '%3A'))
    except KeyError:
        check_only = (request := get_current_request()) and asbool(request.params.get('check_only', False))
        if not check_only:
            error = "%r not found" % instance
            yield ValidationError(error)
        return

    if not isinstance(item, Item):
        error = "%r is not a linkable resource" % instance
        yield ValidationError(error)
        return

    if linkTo and not set([item.type_info.name] + item.base_types).intersection(set(linkTo)):
        reprs = (repr(it) for it in linkTo)
        error = "%r is not of type %s" % (instance, ", ".join(reprs))
        yield ValidationError(error)
        return

    linkEnum = schema.get('linkEnum')
    if linkEnum is not None:
        if not validator.is_type(linkEnum, "array"):
            raise Exception("Bad schema")

        if not any(UUID(enum_uuid) == item.uuid for enum_uuid in linkEnum):
            reprs = ', '.join(repr(it) for it in linkTo)
            error = "%r is not one of %s" % (instance, reprs)
            yield ValidationError(error)
            return

    if schema.get('linkSubmitsFor'):
        userid = None
        for principal in request.effective_principals:
            if principal.startswith('userid.'):
                userid = principal[len('userid.'):]
                break

        if userid is not None:
            user = request.root[userid]
            submits_for = user.upgrade_properties().get('submits_for')
            if (submits_for is not None and
                    not any(UUID(uuid) == item.uuid for uuid in submits_for) and
                    not request.has_permission('submit_for_any')):
                error = "%r is not in user submits_for" % instance
                yield ValidationError(error)
                return


class IgnoreUnchanged(ValidationError):
    pass


def requestMethod(validator, requestMethod, instance, schema):
    ignored(instance, schema)
    if validator.is_type(requestMethod, "string"):
        requestMethod = [requestMethod]
    elif not validator.is_type(requestMethod, "array"):
        raise Exception("Bad schema")  # raise some sort of schema error
    request = get_current_request()
    if request.method not in requestMethod:
        reprs = ', '.join(repr(it) for it in requestMethod)
        error = "request method %r is not one of %s" % (request.method, reprs)
        yield IgnoreUnchanged(error)


def permission(validator, permission, instance, schema):
    ignored(instance, schema)
    if not validator.is_type(permission, "string"):
        raise Exception("Bad schema")  # raise some sort of schema error

    request = get_current_request()
    context = request.context
    if not request.has_permission(permission, context):
        error = "permission %r required" % permission
        yield IgnoreUnchanged(error)


VALIDATOR_REGISTRY = {}


def validators(validator, validators, instance, schema):
    if not validator.is_type(validators, "array"):
        raise Exception("Bad schema")  # raise some sort of schema error

    for validator_name in validators:
        validate = VALIDATOR_REGISTRY.get(validator_name)
        if validate is None:
            raise Exception('Validator %s not found' % validator_name)
        error = validate(instance, schema)
        if error:
            yield ValidationError(error)


def calculatedProperty(validator, linkTo, instance, schema):
    """ This is the validator for calculatedProperty - if we see this field on a submitted item
        we (normally) emit ValidationError since calculated properties cannot be submitted.

        However, if sub-embedded = True is set on the calculated property, allow submission
        if the schema is in one of two valid formats:
            1. It is an object field ie: calculated property on sub-embedded object
            2. It is an array of objects ie: calculated property applied across an array of
               sub-embedded objects.
    """
    ignored(instance)

    def schema_is_sub_embedded(schema):
        return schema.get('sub-embedded', False)

    def schema_is_object(schema):
        return schema.get('type') == 'object'

    def schema_is_array_of_objects(schema):
        return schema.get('type') == 'array' and schema_is_object(schema.get('items', {}))

    if (not schema_is_sub_embedded(schema)
            or (not schema_is_array_of_objects(schema) and not schema_is_object(schema))):
        yield ValidationError('submission of calculatedProperty disallowed')


class SchemaValidator(SerializingSchemaValidator):
    VALIDATORS = SerializingSchemaValidator.VALIDATORS.copy()
    VALIDATORS['calculatedProperty'] = calculatedProperty
    VALIDATORS['linkTo'] = linkTo
    VALIDATORS['permission'] = permission
    VALIDATORS['requestMethod'] = requestMethod
    VALIDATORS['validators'] = validators
    SERVER_DEFAULTS = SERVER_DEFAULTS


def load_schema(filename):
    if isinstance(filename, dict):
        schema = filename
        resolver = NoRemoteResolver.from_schema(schema)
    else:
        if ':' not in filename:  # no repo in filename path, favor the app then fallthrough
            filename = favor_app_specific_schema(filename)
        utf8 = codecs.getreader("utf-8")
        asset = AssetResolver(caller_package()).resolve(filename)
        schema = json.load(utf8(asset.stream()),
                           object_pairs_hook=collections.OrderedDict)
        resolver = RefResolver('file://' + asset.abspath(), schema)
    # use mixinProperties, mixinFacets, mixinAggregations, and mixinColumns (if provided)
    schema = mixinSchemas(
        mixinSchemas(
            mixinSchemas(mixinSchemas(schema, resolver, 'properties'), resolver, 'facets'),
            resolver,
            'aggregations'
        ),
        resolver, 'columns'
    )
    schema = fill_in_schema_merge_refs(schema, resolver)

    # SchemaValidator is not thread safe for now
    SchemaValidator(schema, resolver=resolver)
    return schema


def extract_schema_default(schema, path):
    """ Extracts a schema default given a schema and an array path """
    if 'properties' in schema:
        schema = schema['properties']
    try:
        for key in path:
            obj = schema[key]
        return obj['default']
    except Exception:
        return None


format_checker = FormatChecker()


def validate(schema, data, current=None, validate_current=False):
    """
    Validate the given data using a schema. Optionally provide current data
    to allow IgnoreUnchanged validation errors. If validate_current is set,
    will attempt to validate against current as well as data.
    current and validate_current should be used together if data is expected
    to have deleted fields that should be validated against.

    Args:
        schema (dict): item schema
        data (dict): new item contents
        current (dict): existing item contents
        validate_current (bool): whether to validate against current

    Returns:
        dict validated contents, list of errors
    """
    resolver = NoRemoteResolver.from_schema(schema)
    sv = SchemaValidator(schema, resolver=resolver, format_checker=format_checker)
    validated, errors = sv.serialize(data)
    # validate against current contents if validate_current is set
    if current and validate_current:
        validated_curr, errors_curr = sv.serialize(current)
        data_errors_detail = {str(err.path): err.message for err in errors}
        # add errors from validation of current if they are not in existing
        # error paths or already exist but have a different message
        for err in errors_curr:
            err_path = str(err.path)
            if err_path not in data_errors_detail:
                errors.append(err)
            elif data_errors_detail[err_path] != err.message:
                errors.append(err)
    filtered_errors = []
    for error in errors:
        # Possibly ignore validation if it results in no change to data
        if current is not None and isinstance(error, IgnoreUnchanged):
            # TODO: Should this next assignment be outside the 'for' and should this in loop be testing current_value?
            #       -kmp 13-Sep-2022
            current_value = current
            try:
                for key in error.path:
                    current_value = current_value[key]
            except Exception:
                current_value = None  # not found
            else:
                validated_value = validated
                try:
                    for key in error.path:
                        validated_value = validated_value[key]
                except Exception:
                    validated_value = None  # not found
                # TODO: Should this test be indented left by 4 spaces so that the other arms of the 'if' affect it?
                #       Right now those other arms set seemingly-unused variables. -kmp 7-Aug-2022
                if validated_value == current_value:
                    continue  # value is unchanged between data/current; ignore
        # Also ignore requestMethod and permission errors from defaults.
        if isinstance(error, IgnoreUnchanged):
            new_value = data
            try:
                for key in error.path:
                    # If it's in original data then either user passed it in
                    # or it's from PATCH object with unchanged data. If it's
                    # unchanged then it's already been skipped above.
                    new_value = new_value[key]
                    # XXX: the below I think creates a vulnerability where a user can revert
                    # a protected field to it's default?
                    # default = extract_schema_default(schema, error.path)
                    # if new_value == default:
                    #     continue
            except KeyError:
                # always True for us
                if current and validate_current:
                    for key in error.path:
                        val = current.get(key)
                    default = extract_schema_default(schema, error.path)
                    if val == default:
                        continue
                else:
                    continue
        filtered_errors.append(error)

    return validated, filtered_errors


def validate_request(schema, request, data=None, current=None):
    if data is None:
        data = request.json

    validated, errors = validate(schema, data, current)
    for error in errors:
        error_path = 'Schema: ' + '.'.join([str(p) for p in error.path])
        request.errors.add('body', error_path, error.message)

    if not errors:
        request.validated.update(validated)


def schema_validator(filename):
    schema = load_schema(filename)

    def validator(request):
        return validate_request(schema, request)

    return validator


def combine_schemas(a, b):
    if a == b:
        return a
    if not a:
        return b
    if not b:
        return a
    combined = collections.OrderedDict()
    for name in set(a.keys()).intersection(b.keys()):
        if a[name] == b[name]:
            combined[name] = a[name]
        elif name == 'type':
            combined[name] = sorted(set(ensurelist(a[name]) + ensurelist(b[name])))
        elif name == 'properties':
            combined[name] = collections.OrderedDict()
            for k in set(a[name].keys()).intersection(b[name].keys()):
                combined[name][k] = combine_schemas(a[name][k], b[name][k])
            for k in set(a[name].keys()).difference(b[name].keys()):
                combined[name][k] = a[name][k]
            for k in set(b[name].keys()).difference(a[name].keys()):
                combined[name][k] = b[name][k]
        elif name == 'items':
            combined[name] = combine_schemas(a[name], b[name])
        elif name in ('boost_values', 'facets'):
            combined[name] = collections.OrderedDict()
            combined[name].update(a[name])
            combined[name].update(b[name])
        elif name == 'columns':
            allValues = collections.OrderedDict()
            allValues.update(a[name])
            allValues.update(b[name])
            intersectedKeys = set(a[name].keys()).intersection(set(b[name].keys()))
            combined[name] = {k: v
                              for k, v in allValues.items()
                              if k in intersectedKeys}
    for name in set(a.keys()).difference(b.keys()):
        combined[name] = a[name]
    for name in set(b.keys()).difference(a.keys()):
        combined[name] = b[name]
    return combined


# for integrated tests
def utc_now_str():
    return datetime.utcnow().isoformat() + '+00:00'


@server_default
def userid(instance, subschema):  # args required by jsonschema-serialize-fork
    ignored(instance, subschema)
    return str(uuid.uuid4())


@server_default
def now(instance, subschema):  # args required by jsonschema-serialize-fork
    ignored(instance, subschema)
    return utc_now_str()


def get_identifying_and_required_properties(schema: dict) -> (list, list):
    """
    Returns a tuple containing (first) the list of identifying properties
    and (second) the list of any required properties specified by the given schema.

    This DOES handle a limited version of the "anyOf" construct; namely where it only contains
    a simple list of objects each specifying a "required" property name or a list of property
    names; in this call ALL such "required" property names are included; an EXCEPTION is
    raised if an unsupported usage of this "anyOf" construct is found. 

    This may be slightly confusing in that ALL of the properties specified within an "anyOf"
    construct are returned from this function as required, which is not technically semantically
    not correct; only ONE of those would be required; but this function is NOT used for validation,
    but instead to extract from the actual object the values which must be included on the initial
    insert into the database, when it is FIRST created, via POST in loadxl.
    """
    def get_all_required_properties_from_any_of(schema: dict) -> list:
        """
        Returns a list of ALL property names which are specified as "required" within any "anyOf"
        construct within the given JSON schema. We support ONLY a LIMITED version of "anyOf" construct,
        in which it must be either ONLY a LIST of OBJECTs each specifying a "required" property which
        is a property name or a LIST of property names; if the "anyOf" construct looks like it is
        anything OTHER than this limited usaage, then an EXCEPTION will be raised.
        """
        def raise_unsupported_usage_exception():
            raise Exception("Unsupported use of anyOf in schema.")
        required_properties = set()
        any_of_list = schema.get("anyOf")
        if not any_of_list:
            return required_properties
        if not isinstance(any_of_list, list):
            raise_unsupported_usage_exception()
        for any_of in any_of_list:
            if not any_of:
                continue
            if not isinstance(any_of, dict):
                raise_unsupported_usage_exception()
            if "required" in any_of:
                if not (any_of_value := any_of["required"]):
                    continue
                if isinstance(any_of_value, list):
                    required_properties.update(any_of_value)
                elif isinstance(any_of_value, str):
                    required_properties.add(any_of_value)
                else:
                    raise_unsupported_usage_exception()
        return list(required_properties)

    required_properties = set()
    required_properties.update(schema.get("required", []))
    required_properties.update(get_all_required_properties_from_any_of(schema))
    return SchemaManager.get_identifying_properties(schema), list(required_properties)
