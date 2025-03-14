# See http://docs.pylonsproject.org/projects/pyramid/en/latest/narr/resources.html
import logging
from collections.abc import Mapping
from copy import deepcopy
from dcicutils.misc_utils import ignored
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPInternalServerError
from pyramid.security import (
    Allow,
    Deny,
    ALL_PERMISSIONS,
    Everyone,
    Authenticated,
    principals_allowed_by_permission
)
from pyramid.traversal import (
    resource_path,
    traverse
)
from .server_defaults_user import get_userid
from .types.acl import ONLY_ADMIN_VIEW_ACL
from .calculated import (
    calculate_properties,
    calculated_property,
)
from .interfaces import (
    COLLECTIONS,
    CONNECTION,
    ROOT,
    TYPES,
    UPGRADER,
)
from .validation import ValidationFailure
from .util import (
    ensurelist,
    simple_path_ids,
    uuid_to_path
)
from .util import add_default_embeds, IndexSettings

logger = logging.getLogger(__name__)


def includeme(config):
    config.include(auth0_config)
    config.include(recaptcha_config)
    config.scan(__name__)


def acl_from_settings(settings):
    # XXX Unsure if any of the demo instance still need this
    acl = []
    for k, v in settings.items():
        if k.startswith('allow.'):
            action = Allow
            permission = k[len('allow.'):]
            principals = v.split()
        elif k.startswith('deny.'):
            action = Deny
            permission = k[len('deny.'):]
            principals = v.split()
        else:
            continue
        if permission == 'ALL_PERMISSIONS':
            permission = ALL_PERMISSIONS
        for principal in principals:
            if principal == 'Authenticated':
                principal = Authenticated
            elif principal == 'Everyone':
                principal = Everyone
            acl.append((action, principal, permission))
    return acl


class Resource(object):
    """
    Just used to add global calculated properties
    """
    @calculated_property(name='@id', schema={
        "title": "ID",
        "type": "string",
    })
    def jsonld_id(self, request):
        return request.resource_path(self)

    @calculated_property(name='@context', category='page')
    def jsonld_context(self, request):
        return request.route_path('jsonld_context')

    @calculated_property(category='page')
    def actions(self, request):
        actions = calculate_properties(self, request, category='action')
        if actions:
            return list(actions.values())


class Root(Resource):
    __name__ = ''
    __parent__ = None
    properties = {}

    def __init__(self, registry):
        self.registry = registry

    @reify
    def __acl__(self):
        acl = acl_from_settings(self.registry.settings) + [
            (Allow, Everyone, ['list', 'search']),
            (Allow, 'group.admin', ALL_PERMISSIONS),
            (Allow, 'remoteuser.EMBED', 'restricted_fields'),
        ] + [
                  (Allow, 'remoteuser.INDEXER', ['view', 'view_raw', 'list', 'index']),
                  (Allow, 'remoteuser.EMBED', ['view', 'view_raw', 'expand']),
                  (Allow, Everyone, ['visible_for_edit'])
              ]
        return acl

    @reify
    def connection(self):
        return self.registry[CONNECTION]

    @reify
    def collections(self):
        return self.registry[COLLECTIONS]

    def __getitem__(self, name):
        try:
            resource = self.get(name)
        except KeyError:
            # Just in case we get an unexpected KeyError
            # FIXME: exception logging.
            raise HTTPInternalServerError('Traversal raised KeyError')
        if resource is None:
            raise KeyError(name)
        return resource

    def __contains__(self, name):
        return self.get(name, None) is not None

    def get(self, name, default=None):
        """
        Underlying get function used in traversal. Handles Collections (by
        direct `get`) and Items (through `Connection.__getitem__`)
        """
        resource = self.collections.get(name)
        if resource is not None:
            return resource
        try:
            resource = self.connection[name]  # Connection.__getitem__
        except KeyError:
            resource = default
        return resource

    def __json__(self, request=None):
        ignored(request)
        return self.properties.copy()

    @calculated_property(name='@type', schema={
        "title": "Type",
        "type": "array",
        "items": {
            "type": "string",
        },
    })
    def jsonld_type(self):
        return ['Portal']


def auth0_config(config):
    """ Route that exposes the auth0 domain and client ID for this app. """
    config.add_route(
        'auth0-config',
        '/auth0_config'
    )
    auth0_config_values = {  # determines which values are echoed
        'auth0.client': 'auth0Client',
        'auth0.domain': 'auth0Domain',
        'auth0.options': 'auth0Options'
    }

    def auth0_config_view(request):
        response = request.response
        response.content_type = 'application/json; charset=utf-8'
        response_dict = {
            'title': 'Auth0 Config',
        }
        settings = config.registry.settings
        for config_key, result_key in auth0_config_values.items():
            if config_key in settings:
                response_dict[result_key] = settings[config_key]

        # If Redis enabled, set the callback value in Auth0Config
        if 'redis.server' in request.registry.settings:
            callback = f'{request.host_url}/callback'
            response_dict['auth0Options']['auth']['redirectUrl'] = callback
        return response_dict

    config.add_view(auth0_config_view, route_name='auth0-config')


def recaptcha_config(config):
    """ Route that exposes the recaptcha site key """
    config.add_route(
        'recaptcha-config',
        '/recaptcha_config'
    )
    recaptcha_config_values = {  # determines which values are echoed
        'g.recaptcha.key': 'RecaptchaKey',
    }

    def recaptcha_config_view(request):
        response = request.response
        response.content_type = 'application/json; charset=utf-8'
        response_dict = {
            'title': 'Recaptcha Config',
        }
        settings = config.registry.settings
        for config_key, result_key in recaptcha_config_values.items():
            if config_key in settings:
                response_dict[result_key] = settings[config_key]
        return response_dict

    config.add_view(recaptcha_config_view, route_name='recaptcha-config')


class AbstractCollection(Resource, Mapping):
    """
    Collection for a certain type of resource that stores the following info:
    - registry (pyramid registry)
    - type_info (TypeInfo for a certain item type, see snovault.typeinfo.py)
    - __acl__
    - uniqueKey for the collection (e.g. item_name:key)
    - properties_datastore for items in the collection (defaults to 'database')
    And some other info as well.

    Collections allow retrieval of specific items with them by using the `get`
    method with uuid or the unique_key
    """
    properties = {}
    unique_key = None
    default_properties_datastore = 'database'

    def __init__(self, registry, name, type_info, properties=None, acl=None,
                 unique_key=None, properties_datastore=None):
        self.registry = registry
        self.__name__ = name
        self.type_info = type_info
        if properties is not None:
            self.properties = properties
        if acl is not None:
            self.__acl__ = acl
        if unique_key is not None:
            self.unique_key = unique_key
        # use a default value configured on the class if not set
        if properties_datastore is not None:
            self.properties_datastore = properties_datastore
        else:
            self.properties_datastore = self.default_properties_datastore

    @reify
    def connection(self):
        return self.registry[CONNECTION]

    @reify
    def __parent__(self):
        return self.registry[ROOT]

    def __getitem__(self, name):
        try:
            item = self.get(name)
        except KeyError:
            # Just in case we get an unexpected KeyError
            # FIXME: exception logging.
            raise HTTPInternalServerError('Traversal raised KeyError')
        if item is None:
            raise KeyError(name)
        return item

    def __iter__(self):
        for uuid in self.connection.__iter__(*self.type_info.subtypes):
            yield uuid

    def __len__(self):
        return self.connection.__len__(*self.type_info.subtypes)

    def __hash__(self):
        return object.__hash__(self)

    def __eq__(self, other):
        return self is other

    def _allow_contained(self, resource):
        return resource.__parent__ is self or \
            resource.type_info.name in resource.type_info.subtypes

    def get(self, name, default=None):
        """
        Get an item by name using this collection. First try uuid and then
        unique key. If neither are found, return default, which is usually None.
        self.properties_datastore is checked on the item class and used with
        Connection to force a certain datastore if needed to retrieve item
        """
        resource = self.connection.get_by_uuid(name, default=None,
                                               datastore=self.properties_datastore)
        if resource is not None:
            if not self._allow_contained(resource):
                return default
            return resource
        if self.unique_key is not None:
            item_type_snake_case = getattr(self.type_info, 'item_type', None)
            resource = self.connection.get_by_unique_key(self.unique_key, name,
                                                         datastore=self.properties_datastore,
                                                         item_type=item_type_snake_case)
            if resource is not None:
                if not self._allow_contained(resource):
                    return default
                return resource
        return default

    def iter_no_subtypes(self):
        """
        Make a generator that yields all items in the collection, but not
        subtypes
        """
        for uuid in self.connection.__iter__(self.type_info.item_type):
            yield uuid

    def __json__(self, request):
        ignored(request)
        return self.properties.copy()

    @calculated_property(name='@type', schema={
        "title": "Type",
        "type": "array",
        "items": {
            "type": "string",
        },
    })
    def jsonld_type(self):
        return [
            '{type_name}Collection'.format(type_name=self.type_info.name),
            'Collection',
        ]


class Collection(AbstractCollection):
    """ Separate class so add views do not apply to AbstractCollection """

    @staticmethod
    def index_settings():
        """ Corresponds to the default index settings prior to the existence of this property """
        return IndexSettings()


# Almost every single display_title should have the same
# schema definition, so we define it here to import & re-use.
display_title_schema = {
    "title": "Display Title",
    "description": "A calculated title for every object",
    "type": "string",
}

class Item(Resource):
    """
    Base Item resource that corresponds to a Collection or AbstractCollection
    """
    item_type = None
    base_types = ['Item']
    name_key = None
    rev = {}
    aggregated_items = {}
    embedded_list = []
    default_diff = []
    filtered_rev_statuses = ()
    schema = None
    AbstractCollection = AbstractCollection
    Collection = Collection
    STATUS_ACL = {}  # note that this should ALWAYS be overridden by downstream application
    ALLOWED_PATH_CHARACTERS = ["_", "-", ":", ",", ".", " ", "@"]

    def __init__(self, registry, model):
        self.registry = registry
        self.model = model

    def __acl__(self):
        """This sets the ACL for the item based on mapping of status to ACL.
           If there is no status or the status is not included in the STATUS_ACL
           lookup then the access is set to admin only
        """
        # Don't finalize to avoid validation here.
        properties = self.upgrade_properties().copy()
        status = properties.get('status')
        return self.STATUS_ACL.get(status, ONLY_ADMIN_VIEW_ACL)

    def __repr__(self):
        return '<%s at %s>' % (type(self).__name__, resource_path(self))

    @reify
    def type_info(self):
        return self.registry[TYPES][type(self)]

    @reify
    def collection(self):
        collections = self.registry[COLLECTIONS]
        return collections[self.type_info.name]

    @reify
    def properties_datastore(self):
        return self.collection.properties_datastore

    @reify
    def default_properties_datastore(self):
        return self.collection.default_properties_datastore

    @property
    def __parent__(self):
        return self.collection

    @property
    def __name__(self):
        """
        Used in the resource path for this item. Use `self.name_key` if
        present, otherwise `self.uuid`
        """
        if self.name_key is None:
            return self.uuid
        properties = self.upgrade_properties()
        if properties.get('status') == 'replaced':
            return self.uuid
        return properties.get(self.name_key, None) or self.uuid

    @property
    def properties(self):
        return self.model.properties

    @property
    def propsheets(self):
        return self.model.propsheets

    @property
    def uuid(self):
        return self.model.uuid

    @property
    def sid(self):
        return self.db_model.sid

    @property
    def max_sid(self):
        return self.db_model.max_sid

    @property
    def db_model(self):
        """
        Always returns the resouce model from write storage, which is needed
        for operations like getting current sid/max_sid, rev_links, and
        updating. Leverage `model.used_datastore` to determine source
        """
        if self.model.used_datastore != 'database':
            connection = self.registry[CONNECTION]
            return connection.storage.write.get_by_uuid(str(self.uuid))
        return self.model

    def links(self, properties):
        return {
            path: list(set(simple_path_ids(properties, path)))
            for path in self.type_info.schema_links
        }

    def get_rev_links(self, request, name):
        """
        Return a list of uuid rev_links for the given rev name (in self.rev)
        from the given item.

        Args:
            request: current Request
            name (str): name of the rev (must be in self.rev)

        Returns:
            list of str uuids of the given rev_link
        """
        ignored(request)
        types = self.registry[TYPES]
        type_name, rel = self.rev[name]
        types = types[type_name].subtypes
        return self.registry[CONNECTION].get_rev_links(self.db_model, rel, *types)

    def get_filtered_rev_links(self, request, name):
        """
        Run get_rev_links, but only return items that do not have a status
        in self.filtered_rev_statuses (a tuple defined on the Item)
        If we are indexing, add rev_link info to _rev_linked_uuids_by_item.

        Args:
            request: current Request
            name (str): name of the rev (must be in self.rev)

        Returns:
            list of str uuids of the given rev_link, filtered by status
        """
        # Consider caching rev links on the request? Would save DB requests
        # May not be worth it because they are quite fast
        rev_uuids = self.get_rev_links(request, name)
        filtered_uuids = [
            str(rev_id) for rev_id in rev_uuids
            if traverse(request.root, str(rev_id))['context'].__json__(request).get('status')
            not in self.filtered_rev_statuses
        ]
        if request._indexing_view is True:
            to_update = {name: filtered_uuids}
            if str(self.uuid) in request._rev_linked_uuids_by_item:
                request._rev_linked_uuids_by_item[str(self.uuid)].update(to_update)
            else:
                request._rev_linked_uuids_by_item[str(self.uuid)] = to_update
        return filtered_uuids

    def rev_link_atids(self, request, rev_name):
        """
        Returns the list of reverse linked items given a defined reverse link,
        which should be formatted like:
        rev = {
            '<reverse field name>': ('<reverse item class>', '<reverse field to find>'),
        }

        """
        conn = request.registry[CONNECTION]
        return [request.resource_path(conn[uuid]) for uuid in
                self.get_filtered_rev_links(request, rev_name)]

    def unique_keys(self, properties):
        """ This function used to only resolve keys from schema, it has been
            updated to handle both accession and alternate_accession
        """
        keys = {
            name: [v for prop in props for v in ensurelist(properties.get(prop, ()))]
            for name, props in self.type_info.schema_keys.items()
        }
        if 'accession' not in self.schema['properties']:
            return keys
        keys.setdefault('accession', []).extend(properties.get('alternate_accessions', []))
        if properties.get('status') != 'replaced' and 'accession' in properties:
            keys['accession'].append(properties['accession'])
        return keys

    def add_accession_to_title(self, title):
        if self.properties.get('accession') is not None:
            return title + ' - ' + self.properties.get('accession')
        return title

    def is_update_by_admin_user(self):
        # determine if the submitter in the properties is an admin user
        userid = get_userid()
        users = self.registry['collections']['User']
        user = users.get(userid)
        if 'groups' in user.properties:
            if 'admin' in user.properties['groups']:
                return True
        return False

    def upgrade_properties(self):
        """
        Calls the upgrader on the Item if properties.schema_version is not current
        """
        try:
            properties = deepcopy(self.properties)
        except KeyError:
            # don't fail if we try to upgrade properties on something not there yet
            return None
        current_version = properties.get('schema_version', '1')
        target_version = self.type_info.schema_version
        if target_version is not None and current_version != target_version:
            upgrader = self.registry[UPGRADER]
            try:
                properties = upgrader.upgrade(
                    self.type_info.name, properties, current_version, target_version,
                    context=self, registry=self.registry)
            except RuntimeError:
                raise
            except Exception:
                logger.warning(
                    'Unable to upgrade %s from %r to %r',
                    resource_path(self.__parent__, self.uuid),
                    current_version, target_version, exc_info=True)
        return properties

    def __json__(self, request):
        """
        This function is used to get the "complete" properties of the Item
        after calling `upgrade_properties`
        """
        ignored(request)
        return self.upgrade_properties()

    def item_with_links(self, request):
        """
        Key function that transforms uuids into resource paths in properties.
        It is also responsible for calling upgraders using the __json__ method

        VERY IMPORTANT NOTE:
            Lastly, adds this items' uuid to request._linked_uuids when indexing
            This is now a tuple of uuid, item_type so when retrieving links from ES
            for all links we know both the uuid AND the type.
        """
        # *** context.__json__ CALLS THE UPGRADER (upgrade_properties) ***
        # This works from the schema rather than the links table
        # so that upgrade on GET can work.
        properties = self.__json__(request)
        # use schema_links rather than DB links so upgrades work on ES GETs
        for path in self.type_info.schema_links:
            uuid_to_path(request, properties, path)

        # if indexing, add the uuid of this object to request._linked_uuids
        # and add the sid to _sid_cache if not already present
        if request._indexing_view is True:
            request._linked_uuids.add((str(self.uuid), self.type_info.name))  # _linked_uuids holds 2 tuple
            if str(self.uuid) not in request._sid_cache:
                request._sid_cache[str(self.uuid)] = self.sid
        return properties

    def __resource_url__(self, request, info):
        ignored(request, info)
        return None

    @classmethod
    def create(cls, registry, uuid, properties, sheets=None):
        """
        This class method is called in crud_views.py - `collection_add` (API endpoint) > `create_item` (method) > `type_info.factory.create` (this class method)

        This method instantiates a new Item class instance from provided `uuid` and `properties`,
        then runs the `_update` (instance method) to save the Item to the database.
        """
        model = registry[CONNECTION].create(cls.__name__, uuid)
        item_instance = cls(registry, model)
        item_instance._update(properties, sheets)
        return item_instance

    def validate_path_characters(self, field, value):
        """
        Check that the field with given value does not contain any characters
        that interfere with the resource_path. Currently, we allow all
        alphanumeric characters and few others
        """
        if not isinstance(value, str):  # formerly basestring
            raise ValueError('Identifying property %s must be a string. Value: %s' % (field, value))
        forbidden = [
            char for char in value
            if (not char.isalnum() and char not in self.ALLOWED_PATH_CHARACTERS)
        ]
        if any(forbidden):
            msg = ("Forbidden character(s) %s are not allowed in field: %s. Value: %s"
                   % (set(forbidden), field, value))
            raise ValidationFailure('body', 'Item: path characters', msg)

    def update(self, properties, sheets=None):
        """Alias of _update, called in crud_views.py - `update_item` (method)"""
        self._update(properties, sheets)

    def _update(self, properties, sheets=None):
        """
        This instance method is called in Item.create (classmethod) as well as in crud_views.py - `item_edit` (API endpoint) > `update_item` (method) > `context.update` (instance method).

        This method is used to assert lack of duplicate unique keys in database and then to perform database update of `properties` (dict).

        Optionally define this method in inherited classes to extend `properties` on Item updates.
        """
        unique_keys = None
        links = None
        if properties is not None:
            if 'uuid' in properties:
                properties = properties.copy()
                del properties['uuid']

            # validation on name key and unique keys
            nk_val = properties.get(self.name_key, '')
            self.validate_path_characters(self.name_key, nk_val)

            unique_keys = self.unique_keys(properties)
            for k, values in unique_keys.items():
                if len(set(values)) != len(values):
                    msg = "Duplicate keys for %r: %r" % (k, values)
                    raise ValidationFailure('body', 'Item: duplicate keys', msg)
                for uk_val in values:
                    self.validate_path_characters(k, uk_val)

            links = self.links(properties)

        # actually propogate the update to the DB
        connection = self.registry[CONNECTION]
        connection.update(self.db_model, properties, sheets, unique_keys, links,
                          datastore=self.properties_datastore)

    @reify
    def embedded(self):
        """
        Use the embedded_list defined for the individual types to create the
        embedded attribute through expansion using add_default_embeds
        """
        total_schema = self.schema.get('properties', {}).copy()
        calc_props_schema = {}
        types = self.registry[TYPES]
        if self.registry['calculated_properties']:
            for calc_props_key, calc_props_val in self.registry['calculated_properties'].props_for(self).items():
                if calc_props_val.schema:
                    calc_props_schema[calc_props_key] = calc_props_val.schema
        total_schema.update(calc_props_schema)
        this_type = self.type_info.item_type
        return add_default_embeds(this_type, types, self.embedded_list, total_schema)

    @calculated_property(name='@type', schema={
        "title": "Type",
        "type": "array",
        "items": {
            "type": "string",
        },
    })
    def jsonld_type(self):
        return [self.type_info.name] + self.base_types

    @calculated_property(name='uuid')
    def prop_uuid(self):
        return str(self.uuid)

    @calculated_property(schema={
        "title": "principals_allowed",
        "description": "Calculated permissions used for ES filtering",
        "type": "object",
        'properties': {
            'view': {
                'type': 'string'
            },
            'edit': {
                'type': 'string'
            }
        }
    })
    def principals_allowed(self):
        allowed = {}
        # these are the relevant Item permissions
        for permission in ('view', 'edit'):
            principals = principals_allowed_by_permission(self, permission)
            if principals is Everyone:
                principals = [Everyone]
            elif Everyone in principals:
                principals = [Everyone]
            elif Authenticated in principals:
                principals = [Authenticated]
            # Filter our roles
            # NOTE: previously, we removed things that began with role. however this
            # breaks how smaht-portal permissioning is implemented, so we are no longer
            # suppressing them - Will Jan 31 2024
            allowed[permission] = [
                p for p in sorted(principals)  # if not p.startswith('role.')
            ]
        return allowed

    @calculated_property(schema=display_title_schema)
    def display_title(self, request=None):
        """create a display_title field."""
        display_title = ""
        look_for = [
            "title",
            "name",
            "location_description",
            "accession",
        ]
        properties = self.upgrade_properties()
        for field in look_for:
            # special case for user: concatenate first and last names
            display_title = properties.get(field, None)
            if display_title:
                if field != 'accession':
                    display_title = self.add_accession_to_title(display_title)
                return display_title
        # if none of the existing terms are available, use @type + date_created
        try:
            type_date = self.__class__.__name__ + " from " + properties.get("date_created", None)[:10]
            return type_date
        # last resort, use uuid
        except Exception:
            return properties.get('uuid', None)
#   def display_title(self):
#       return str(self.uuid)
