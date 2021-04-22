import json
from uuid import (
    UUID,
    uuid4,
)

import transaction
from pyramid.exceptions import HTTPForbidden
from pyramid.settings import asbool
from pyramid.view import view_config
from structlog import get_logger
from dcicutils.diff_utils import DiffManager

from .calculated import calculated_property
from .interfaces import (
    STORAGE,
    Created,
    BeforeModified,
    AfterModified,
)
from .invalidation import add_to_indexing_queue
from .resources import (
    Collection,
    Item,
)
from .util import debug_log
from .validation import ValidationFailure
from .validators import (
    no_validate_item_content_patch,
    no_validate_item_content_post,
    no_validate_item_content_put,
    validate_item_content_patch,
    validate_item_content_post,
    validate_item_content_put,
    validate_item_content_in_place
)

log = get_logger(__name__)


def includeme(config):
    config.include('.calculated')
    config.scan(__name__)


def build_diff_from_request(context, request):
    """ Unpacks request.body as JSON and computes a diff """
    try:
        item_type = context.type_info.name
        body = json.loads(request.body)
        dm = DiffManager(label=item_type)
    except json.decoder.JSONDecodeError:
        log.info('Request body is not valid JSON!')  # can happen from indirect patch, such as with access key
        return None
    except Exception as e:
        log.error('Unknown error encountered building diff from request: %s' % e)
        return None
    return dm.patch_diffs(body)


def create_item(type_info, request, properties, sheets=None):
    '''
    Validates or generates new UUID, instantiates & saves an Item in
    database using provided `type_info` & `properties`, sends 'Created'
    notification, which can be subscribed to using the @subscriber(Created) decorator

    Queues the created item for indexing using a hook on the current transaction
    '''
    registry = request.registry
    item_properties = properties.copy()
    txn = transaction.get()

    if 'uuid' in item_properties:
        try:
            nonvalidated_uuid = item_properties.pop('uuid')
            uuid = UUID(nonvalidated_uuid)
        except ValueError as e:
            raise ValueError(str(e) + ': ' + str(nonvalidated_uuid))
    else:
        uuid = uuid4()

    item = type_info.factory.create(registry, uuid, item_properties, sheets)
    # add the item to the queue
    to_queue = {'uuid': str(item.uuid), 'sid': item.sid}
    telemetry_id = request.params.get('telemetry_id', None)
    if telemetry_id:
        to_queue['telemetry_id'] = telemetry_id
    log.info(event='add_to_indexing_queue', **to_queue)
    txn.addAfterCommitHook(add_to_indexing_queue, args=(request, to_queue, 'add',))
    registry.notify(Created(item, request))

    return item


def update_item(context, request, properties, sheets=None):
    '''
    Updates retrieved-from-database `context` (Item class instance) with
    `properties` (dict) in database, sends 'BeforeModified' & 'AfterModified'
    notifications, which can be subscribed to using the
    @subscriber(BeforeModified) or @subscriber(AfterModified) decorators

    Queues the updated item for indexing using a hook on the current transaction
    '''
    txn = transaction.get()
    registry = request.registry
    item_properties = properties.copy()
    registry.notify(BeforeModified(context, request))
    context.update(item_properties, sheets)
    # set up hook for queueing indexing
    diff = build_diff_from_request(context, request)
    to_queue = {'uuid': str(context.uuid), 'sid': context.sid}
    if diff is not None:
        to_queue['diff'] = diff
    telemetry_id = request.params.get('telemetry_id', None)
    if telemetry_id:
        to_queue['telemetry_id'] = telemetry_id
    txn.addAfterCommitHook(add_to_indexing_queue, args=(request, to_queue, 'edit',))
    registry.notify(AfterModified(context, request))


def delete_item(context, request):
    """
    Sets the status of an item to deleted and triggers indexing
    """
    properties = context.properties.copy()
    properties['status'] = 'deleted'
    update_item(context, request, properties)
    return True


def purge_item(context, request):
    """
    Fully delete an item from the DB and Elasticsearch if all links to that
    have been removed. Requires that the status of the item == 'deleted',
    otherwise will throw a validation failure
    """
    item_type = context.collection.type_info.item_type
    item_uuid = str(context.uuid)
    if context.properties.get('status') != 'deleted':
        msg = (u'Item status must equal deleted before purging from DB.' +
               ' It currently is %s' % context.properties.get('status'))
        raise ValidationFailure('body', ['status'], msg)
    request.registry[STORAGE].purge_uuid(rid=item_uuid, item_type=item_type)
    return True


def render_item(request, context, render, return_uri_also=False):
    if render == 'uuid':
        item_uri = '/%s' % context.uuid
    else:
        item_uri = request.resource_path(context)

    if asbool(render) is True:
        rendered = request.embed(item_uri, '@@object', as_user=True)
    else:
        rendered = item_uri
    return (rendered, item_uri) if return_uri_also else rendered


@view_config(context=Collection, permission='add', request_method='POST',
             validators=[validate_item_content_post])
@view_config(context=Collection, permission='add_unvalidated', request_method='POST',
             validators=[no_validate_item_content_post],
             request_param=['validate=false'])
@debug_log
def collection_add(context, request, render=None):
    '''Endpoint for adding a new Item.'''
    check_only = asbool(request.params.get('check_only', False))
    if check_only:
        return {
            'status': "success",
            '@type': ['result'],
        }

    if render is None:
        render = request.params.get('render', True)

    item = create_item(context.type_info, request, request.validated)

    rendered, item_uri = render_item(request, item, render, True)
    request.response.status = 201
    request.response.location = item_uri
    result = {
        'status': 'success',
        '@type': ['result'],
        '@graph': [rendered],
    }
    return result


@view_config(context=Item, permission='edit', request_method='PUT',
             validators=[validate_item_content_put])
@view_config(context=Item, permission='edit', request_method='PATCH',
             validators=[validate_item_content_patch])
@view_config(context=Item, permission='edit_unvalidated', request_method='PUT',
             validators=[no_validate_item_content_put],
             request_param=['validate=false'])
@view_config(context=Item, permission='edit_unvalidated', request_method='PATCH',
             validators=[no_validate_item_content_patch],
             request_param=['validate=false'])
@view_config(context=Item, permission='index', request_method='GET',
             validators=[validate_item_content_in_place],
             request_param=['check_only=true'])
@debug_log
def item_edit(context, request, render=None):
    '''
    Endpoint for editing an existing Item.
    This handles both PUT and PATCH, difference is the validator

    PUT - replaces the current properties with the new body
    PATCH - updates the current properties with those supplied.
    Note validators will handle the PATH ?delete_fields parameter if you want
    field to be deleted

    Also allow a GET request to just run the edit validators when using
    `check_only=true` query param when indexing. Whether using this or POST/
    PATCH, always return a dummy response with `check_only=true` and do not
    actually modify the DB with `update_item`
    '''
    check_only = asbool(request.params.get('check_only', False))
    if check_only:
        return {
            'status': "success",
            '@type': ['result'],
        }

    if render is None:
        render = request.params.get('render', True)

    # This *sets* the property sheet and adds the item to the indexing queue
    update_item(context, request, request.validated)

    rendered = render_item(request, context, render)
    request.response.status = 200
    result = {
        'status': 'success',
        '@type': ['result'],
        '@graph': [rendered],
    }
    return result


@view_config(context=Item, permission='view', request_method='GET',
             name='links')
@debug_log
def get_linking_items(context, request, render=None):
    """
    Utilize find_uuids_linked_to_item function in PickStorage to find
    any items that link to the given item context
    Split the answer into linkTos and rev_links
    """
    item_uuid = str(context.uuid)
    links = request.registry[STORAGE].find_uuids_linked_to_item(item_uuid)
    request.response.status = 200
    result = {
        'status': 'success',
        '@type': ['result'],
        'display_title': 'Links to %s' % item_uuid,
        'notification' : '%s has %s items linking to it. This may include rev_links if status != deleted' % (item_uuid, len(links)),
        'uuids_linking_to': links
    }
    return result


@view_config(context=Item, permission='edit', request_method='DELETE')
@debug_log
def item_delete_full(context, request, render=None):
    """
    DELETE method that either sets the status of an item to deleted (base
    functionality) or fully purges the item from the DB and ES if all links
    to that item have been removed. Default behavior is delete

    To purge, use ?purge=true query string
    For example: DELETE `/<item-type>/<uuid>?purge=true`
    """
    # possibly temporary fix to check if user is admin
    if hasattr(request, 'user_info'):
        user_details = request.user_info.get('details', {})
    else:
        if 'group.admin' in request.effective_principals:
            user_details = {'groups': 'admin'}  # you can do it
        else:
            user_details = {}  # you cannot
    if 'admin' not in user_details.get('groups', []):
        msg = u'Must be admin to fully delete items.'
        raise ValidationFailure('body', ['userid'], msg)

    purge_from_database = asbool(request.GET and request.GET.get('purge'))
    uuid = str(context.uuid)
    if purge_from_database:
        # Delete entirely - WARNING USE WITH CAUTION - DELETES PERMANENTLY
        # checking of item status and links is done within purge_item()
        result = purge_item(context, request)
        if result:
            return {
                'status': 'success',
                '@type': ['result'],
                'notification' : 'Permanently deleted ' + uuid,
                '@graph': [uuid]
            }
    else:
        result = delete_item(context, request)
        if result:
            return {
                'status': 'success',
                '@type': ['result'],
                'notification' : 'Set status of ' + uuid + ' to deleted',
                '@graph': [ render_item(request, context, render) ]
            }

    return {
        'status': 'failure',
        '@type': ['result'],
        'notification' : 'Deletion failed',
        '@graph': [uuid]
    }


@view_config(context=Item, permission='view', request_method='GET',
             name='validation-errors')
@debug_log
def item_view_validation_errors(context, request):
    """
    View config for validation_errors. If the current model is not using ES,
    do not calculate the validation errors as it would require an extra
    request and some tricky permission handling.

    Args:
        context: current Item
        request: current request

    Returns:
        A dictionary including item path and validation errors from ES
    """
    if context.model.used_datastore != 'elasticsearch':
        return {
            '@id': request.resource_path(context),
            'validation_errors': [],
        }
    source = context.model.source
    allowed = set(source['principals_allowed']['view'])  # use view permissions
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden
    return {
        '@id': source['object']['@id'],
        'validation_errors': source.get('validation_errors', [])
    }


@calculated_property(context=Item, category='page', name='validation-errors',
                     condition=lambda request: request.has_permission('view'))
def validation_errors_property(context, request):
    """
    Frame=page calculated property to add validation_errors to response.
    The request.embed calls item_view_validation_errors
    Args:
        context: current Item
        request: current Request
    Returns:
        List result of validation errors
    """
    path = request.resource_path(context)
    return request.embed(path, '@@validation-errors')['validation_errors']


def get_item_revision_history(request, uuid):
    """ Computes the revision history of the given item from the DB.
        The more edits an item has undergone, the more expensive this
        operation is.
    """
    revisions = request.registry[STORAGE].revision_history(uuid=uuid)
    return revisions


@view_config(context=Item, permission='edit', request_method='GET',
             name='revision-history')
@debug_log
def item_view_revision_history(context, request):
    """ View config for viewing an item's revision history.
        For now, to view revision history the caller must have EDIT permissions.
    """
    uuid = str(context.uuid)
    revisions = get_item_revision_history(request, uuid)
    return {
        'uuid': uuid,
        'revisions': revisions
    }
