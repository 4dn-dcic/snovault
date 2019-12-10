from uuid import UUID
from .schema_utils import validate_request, validate, IgnoreUnchanged
from .validation import ValidationFailure
from pyramid.security import ACLDenied
from .elasticsearch.create_mapping import determine_if_is_date_field


# No-validation validators


def no_validate_item_content_post(context, request):
    data = request.json
    request.validated.update(data)


def no_validate_item_content_put(context, request):
    data = request.json
    if 'uuid' in data:
        if UUID(data['uuid']) != context.uuid:
            msg = 'uuid may not be changed'
            raise ValidationFailure('body', ['uuid'], msg)
    request.validated.update(data)


def no_validate_item_content_patch(context, request):
    data = context.properties.copy()
    data.update(request.json)
    schema = context.type_info.schema
    # this will raise a validation error if delete_fields param provided
    data = delete_fields(request, data, schema)
    if 'uuid' in data:
        if UUID(data['uuid']) != context.uuid:
            msg = 'uuid may not be changed'
            raise ValidationFailure('body', ['uuid'], msg)
    request.validated.update(data)


def delete_fields(request, data, schema):
    """
    Delete fields from data in the delete_fields param of the request.
    Validate to catch any permission errors and return the validated data

    Args:
        request: current Request object
        data: dict item contents
        schema: dict item schema

    Returns:
        dict validated contents

    Raises:
        ValidationFailure if validate=false in request params
    """
    if not request.params.get('delete_fields'):
        return data

    # do not allow validate=false with delete_fields, since it skips schema
    # validation, which does things like adding defaults
    if request.params.get('validate') == 'false':
        err_msg = 'Cannot delete fields on request with with validate=false'
        raise ValidationFailure('body', 'delete_fields', err_msg)

    # make a copy of data after removing fields and validate
    to_validate = data.copy()
    for dfield in request.params['delete_fields'].split(','):
        dfield = dfield.strip()
        if dfield in data:
            del to_validate[dfield]
    # permission validation, comparing data with deleted values to the previous
    # data. If validated, return that validated value
    # Note: a deleted field with a default will replaced with that value;
    #       if same value was already in data, allow regardless of permission
    validated, errors = validate(schema, to_validate, current=data, validate_current=True)
    if errors:
        for error in errors:
            if isinstance(error, IgnoreUnchanged):
                if error.validator != 'permission':
                    continue
            error_name = 'Schema: ' + '.'.join([str(p) for p in error.path])
            request.errors.add('body', error_name, error.message)
    if request.errors:
        raise ValidationFailure('body', 'delete_fields', 'Error deleting fields')
    # validate() may add fields, such as uuid and schema_version
    for orig_field in [k for k in validated]:
        if orig_field not in to_validate:
            del validated[orig_field]
    # finally, return the validated contents with deleted fields
    return validated


# Schema checking validators
def validate_item_content_post(context, request):
    data = request.json
    validate_request(context.type_info.schema, request, data)


def validate_item_content_put(context, request):
    data = request.json
    schema = context.type_info.schema
    if 'uuid' in data and UUID(data['uuid']) != context.uuid:
        msg = 'uuid may not be changed'
        raise ValidationFailure('body', ['uuid'], msg)
    current = context.upgrade_properties().copy()
    current['uuid'] = str(context.uuid)
    validate_request(schema, request, data, current)


def validate_item_content_patch(context, request):
    data = context.upgrade_properties().copy()
    if 'schema_version' in data:
        del data['schema_version']
    data.update(request.json)
    schema = context.type_info.schema
    data = delete_fields(request, data, schema)
    if 'uuid' in data and UUID(data['uuid']) != context.uuid:
        msg = 'uuid may not be changed'
        raise ValidationFailure('body', ['uuid'], msg)
    current = context.upgrade_properties().copy()
    current['uuid'] = str(context.uuid)
    # this will add defaults values back to deleted fields with schema defaults
    validate_request(schema, request, data, current)


def validate_item_content_in_place(context, request):
    """
    Used with indexer. Validate the request against the schema using only the
    data in the request, which will be upgraded context properties, to save a
    call to the DB with `context.upgrade_properties`

    Args:
        context: current Item context
        request: current Request

    Returns:
        None

    Raises:
        ValidationFailure: on uuid mismatch or schema validation failure
    """
    data = request.json
    schema = context.type_info.schema
    if 'uuid' in data and UUID(data['uuid']) != context.uuid:
        msg = 'uuid may not be changed'
        raise ValidationFailure('body', ['uuid'], msg)
    # set current to data, since this validator is not meant to be used for
    # changing properties. Used with call to `schema_utils.validate`
    current = data.copy()
    current['uuid'] = str(context.uuid)
    validate_request(schema, request, data, current)
