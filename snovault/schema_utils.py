import codecs
import collections
import uuid
import json

from datetime import datetime
from dcicutils.misc_utils import ignored
from jsonschema_serialize_fork import (
    Draft4Validator,
    FormatChecker,
    RefResolver,
)
from jsonschema_serialize_fork.exceptions import ValidationError
from pyramid.path import AssetResolver, caller_package
from pyramid.threadlocal import get_current_request
from pyramid.traversal import find_resource
from uuid import UUID
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
        raise ValueError('Resolution disallowed for: %s' % uri)


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

    # And normalize the value to a uuid
    if validator._serialize:
        validator._validated[-1] = str(item.uuid)


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


class SchemaValidator(Draft4Validator):
    VALIDATORS = Draft4Validator.VALIDATORS.copy()
    VALIDATORS['calculatedProperty'] = calculatedProperty
    VALIDATORS['linkTo'] = linkTo
    VALIDATORS['permission'] = permission
    VALIDATORS['requestMethod'] = requestMethod
    VALIDATORS['validators'] = validators
    SERVER_DEFAULTS = SERVER_DEFAULTS


format_checker = FormatChecker()


def load_schema(filename):
    if isinstance(filename, dict):
        schema = filename
        resolver = NoRemoteResolver.from_schema(schema)
    else:
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

    # SchemaValidator is not thread safe for now
    SchemaValidator(schema, resolver=resolver, serialize=True)
    return schema


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
    sv = SchemaValidator(schema, resolver=resolver, serialize=True, format_checker=format_checker)
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
    # from jsonschema_serialize_fork date-time format requires a timezone
    return datetime.utcnow().isoformat() + '+00:00'


@server_default
def userid(instance, subschema):  # args required by jsonschema-serialize-fork
    ignored(instance, subschema)
    return str(uuid.uuid4())


@server_default
def now(instance, subschema):  # args required by jsonschema-serialize-fork
    ignored(instance, subschema)
    return utc_now_str()
