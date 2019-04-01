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
    delete_fields(request, data)
    if 'uuid' in data:
        if UUID(data['uuid']) != context.uuid:
            msg = 'uuid may not be changed'
            raise ValidationFailure('body', ['uuid'], msg)
    request.validated.update(data)


def delete_fields(request, data):
    """
    Delete fields from data in the delete_fields param of the request.
    Do not do any special validation here, since that is handled in the
    validate function that runs this. Modified the input data in place

    Args:
        request: current Request object
        data: dict item metadata

    Returns:
        None

    Raises:
        ValidationFailure if validate=false in request params
    """
    if not request.params.get('delete_fields'):
        return

    # do not allow validate=false with delete_fields, since it skips schema
    # validation, which does things like adding defaults
    if request.params.get('validate') == 'false':
        err_msg = 'Cannot delete fields on request with with validate=false'
        raise ValidationFailure('body', ['?delete_fields'], err_msg)

    for dfield in request.params['delete_fields'].split(','):
        dfield = dfield.strip()
        if dfield in data:
            del data[dfield]


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
    delete_fields(request, data)
    if 'uuid' in data and UUID(data['uuid']) != context.uuid:
        msg = 'uuid may not be changed'
        raise ValidationFailure('body', ['uuid'], msg)
    current = context.upgrade_properties().copy()
    current['uuid'] = str(context.uuid)
    # this will add defaults values back to deleted fields with schema defaults
    validate_request(schema, request, data, current)
