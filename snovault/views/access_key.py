"""Access_key types file."""

from pyramid.view import view_config
from pyramid.settings import asbool
from .. import (
    collection,
    load_schema,
)
from ..crud_views import (
    collection_add,
    item_edit,
)
from ..validators import (
    validate_item_content_post,
)
from ..util import debug_log
from ..authentication import (
    generate_password,
    generate_user,
    CRYPT_CONTEXT,
)

from ..types.access_key import AccessKey


def includeme(config):
    config.scan()


# access keys have view permissions for update so readonly admin and the like
# can create access keys to download files.
@view_config(context=AccessKey.Collection, request_method='POST',
             permission='add',
             validators=[validate_item_content_post])
@debug_log
def access_key_add(context, request):
    """smth."""
    crypt_context = request.registry[CRYPT_CONTEXT]

    if 'access_key_id' not in request.validated:
        request.validated['access_key_id'] = generate_user()

    if 'user' not in request.validated:
        request.validated['user'], = [
            principal.split('.', 1)[1]
            for principal in request.effective_principals
            if principal.startswith('userid.')
        ]

    password = None
    if 'secret_access_key_hash' not in request.validated:
        password = generate_password()
        request.validated['secret_access_key_hash'] = crypt_context.hash(password)

    result = collection_add(context, request)

    if password is None:
        result['secret_access_key'] = None
    else:
        result['secret_access_key'] = password

    result['access_key_id'] = request.validated['access_key_id']
    result['description'] = request.validated.get('description', "")
    return result


@view_config(name='reset-secret', context=AccessKey,
             permission='add',
             request_method='POST', subpath_segments=0)
@debug_log
def access_key_reset_secret(context, request):
    """smth."""
    request.validated = context.properties.copy()
    crypt_context = request.registry[CRYPT_CONTEXT]
    password = generate_password()
    new_hash = crypt_context.hash(password)
    request.validated['secret_access_key_hash'] = new_hash
    result = item_edit(context, request, render=False)
    result['access_key_id'] = request.validated['access_key_id']
    result['secret_access_key'] = password
    return result


@view_config(context=AccessKey, permission='view_raw', request_method='GET',
             name='raw')
@debug_log
def access_key_view_raw(context, request):
    """smth."""
    if asbool(request.params.get('upgrade', True)):
        properties = context.upgrade_properties()
    else:
        properties = context.properties.copy()
    del properties['secret_access_key_hash']
    return properties
