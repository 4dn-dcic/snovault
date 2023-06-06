"""Access_key types file."""

from pyramid.view import view_config
from pyramid.security import (
    Allow,
    Deny,
    Authenticated,
    Everyone,
)
from pyramid.settings import asbool
import datetime
from .base import (
    Item,
    DELETED_ACL,
    ONLY_ADMIN_VIEW_ACL,
)
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
# xyzzy from ..views.access_key import access_key_add, access_key_reset_secret, access_key_view_raw


@collection(
    name='access-keys',
    unique_key='access_key:access_key_id',
    properties={
        'title': 'Access keys',
        'description': 'Programmatic access keys',
    },
    acl=[
        (Allow, Authenticated, 'add'),
        (Allow, 'group.admin', 'list'),
        (Allow, 'group.read-only-admin', 'list'),
        (Allow, 'remoteuser.INDEXER', 'list'),
        (Allow, 'remoteuser.EMBED', 'list'),
        (Deny, Everyone, 'list'),
    ])
class AccessKey(Item):
    """AccessKey class."""
    ACCESS_KEY_EXPIRATION_TIME = 90  # days
    item_type = 'access_key'
    schema = load_schema('snovault:schemas/access_key.json')
    name_key = 'access_key_id'
    embedded_list = []

    STATUS_ACL = {
        'current': [(Allow, 'role.owner', ['view', 'edit'])] + ONLY_ADMIN_VIEW_ACL,
        'deleted': DELETED_ACL,
    }

    @classmethod
    def create(cls, registry, uuid, properties, sheets=None):
        """ Sets the access key timeout 90 days from creation. """
        properties['expiration_date'] = (datetime.datetime.utcnow() + datetime.timedelta(
            days=cls.ACCESS_KEY_EXPIRATION_TIME)).isoformat()
        return super().create(registry, uuid, properties, sheets)

    def __ac_local_roles__(self):
        """grab and return user as owner."""
        owner = 'userid.%s' % self.properties['user']
        return {owner: 'role.owner'}

    def __json__(self, request):
        """delete the secret access key has from the object when used."""
        properties = super(AccessKey, self).__json__(request)
        del properties['secret_access_key_hash']
        return properties

    def update(self, properties, sheets=None):
        """smth."""
        # make sure PUTs preserve the secret access key hash
        if 'secret_access_key_hash' not in properties:
            new_properties = self.properties.copy()
            new_properties.update(properties)
            properties = new_properties
        # set new expiration
        properties['expiration_date'] = (datetime.datetime.utcnow() + datetime.timedelta(
            days=self.ACCESS_KEY_EXPIRATION_TIME)).isoformat()
        self._update(properties, sheets)

    class Collection(Item.Collection):
        pass
