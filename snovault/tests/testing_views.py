import copy
from pyramid.security import (
    # ALL_PERMISSIONS,
    Allow,
    # Authenticated,
    Deny,
    # DENY_ALL,
    Everyone,
    principals_allowed_by_permission,
)
from pyramid.traversal import find_root, traverse
from pyramid.view import view_config
from sqlalchemy import inspect
from transaction.interfaces import TransientError
from ..resources import (
    AbstractCollection as BaseAbstractCollection,
    Collection as BaseCollection,
    Item as BaseItem,
)
from ..calculated import calculated_property
from ..config import collection, abstract_collection
from ..schema_utils import load_schema
from ..attachment import ItemWithAttachment
from ..interfaces import CONNECTION
# from .root import TestRoot


def includeme(config):
    config.scan(__name__)


# Item acls

def append_acls(acls1: list, acls2: list) -> list:
    # PyCharm gets overly aggressive about type-checking lists of tuples when they are added,
    # fearing every detail of every internal element has to match. This asserts that only list-ness
    # is being relied upon.
    return acls1 + acls2


ONLY_ADMIN_VIEW = [
    (Allow, 'group.admin', ['view', 'edit']),
    (Allow, 'group.read-only-admin', ['view']),
    (Allow, 'remoteuser.INDEXER', ['view']),
    (Allow, 'remoteuser.EMBED', ['view']),
    (Allow, Everyone, ['view', 'edit']),
]

ALLOW_EVERYONE_VIEW = append_acls(
    [
        (Allow, Everyone, ['view', 'list']),
    ],
    ONLY_ADMIN_VIEW)

ALLOW_VIEWING_GROUP_VIEW = append_acls(
    [
        (Allow, 'role.viewing_group_member', 'view'),
    ],
    ONLY_ADMIN_VIEW)

ALLOW_LAB_SUBMITTER_EDIT = append_acls(
    [
        (Allow, 'role.viewing_group_member', 'view'),
        (Allow, 'role.lab_submitter', 'edit'),
    ],
    ONLY_ADMIN_VIEW)

ALLOW_CURRENT_AND_SUBMITTER_EDIT = append_acls(
    [
        (Allow, Everyone, 'view'),
        (Allow, 'role.lab_submitter', 'edit'),
    ],
    ONLY_ADMIN_VIEW)

ALLOW_CURRENT = append_acls(
    [
        (Allow, Everyone, 'view'),
    ],
    ONLY_ADMIN_VIEW)

DELETED = append_acls(
    [
        (Deny, Everyone, 'visible_for_edit'),
    ],
    ONLY_ADMIN_VIEW)


# Collection acls

ALLOW_SUBMITTER_ADD = [
    (Allow, Everyone, ['add']),
]


@view_config(name='testing-user', request_method='GET')
def user(request):
    return {
        'authenticated_userid': request.authenticated_userid,
        'effective_principals': request.effective_principals,
    }


@view_config(name='testing-allowed', request_method='GET')
def allowed(context, request):
    permission = request.params.get('permission', 'view')
    return {
        'has_permission': bool(request.has_permission(permission, context)),
        'principals_allowed_by_permission': principals_allowed_by_permission(context, permission),
    }


def paths_filtered_by_status(request, paths, exclude=('deleted', 'replaced'), include=None):
    """
    This function has been deprecated in Fourfront, but is still used by
    access_keys calc property in types/user.py (only for snowflakes)
    filter out status that shouldn't be visible.
    Also convert path to str as functions like rev_links return uuids
    """
    if include is not None:
        return [
            path for path in paths
            if traverse(request.root, str(path))['context'].__json__(request).get('status') in include
        ]
    else:
        return [
            path for path in paths
            if traverse(request.root, str(path))['context'].__json__(request).get('status') not in exclude
        ]


class AbstractCollection(BaseAbstractCollection):
    def get(self, name, default=None):
        resource = super(AbstractCollection, self).get(name, None)
        if resource is not None:
            return resource
        if ':' in name:
            resource = self.connection.get_by_unique_key('alias', name)
            if resource is not None:
                if not self._allow_contained(resource):
                    return default
                return resource
        return default


class Collection(BaseCollection):
    def __init__(self, *args, **kw):
        super(Collection, self).__init__(*args, **kw)
        if hasattr(self, '__acl__'):
            return
        # XXX collections should be setup after all types are registered.
        # Don't access type_info.schema here as that precaches calculated schema too early.
        self.__acl__ = (ALLOW_SUBMITTER_ADD + ALLOW_EVERYONE_VIEW)


@abstract_collection(
    name='items',
    properties={
        'title': "Item Listing",
        'description': 'Abstract collection of all Items.',
    })
class Item(BaseItem):
    item_type = 'item'
    AbstractCollection = AbstractCollection
    Collection = Collection
    STATUS_ACL = {
        # standard_status
        'released': ALLOW_CURRENT,
        'deleted': DELETED,
        'replaced': DELETED,

        # shared_status
        'current': ALLOW_CURRENT,
        'disabled': ONLY_ADMIN_VIEW,

        # file
        'obsolete': ONLY_ADMIN_VIEW,

        # "sets"
        'release ready': ALLOW_VIEWING_GROUP_VIEW,
        'revoked': ALLOW_CURRENT,
        'in review': ALLOW_CURRENT_AND_SUBMITTER_EDIT,

        # publication
        'published': ALLOW_CURRENT,

        # pipeline
        'active': ALLOW_CURRENT,
        'archived': ALLOW_CURRENT,
    }
    filtered_rev_statuses = ('deleted', 'replaced')

    @property
    def __name__(self):
        if self.name_key is None:
            return self.uuid
        properties = self.upgrade_properties()
        if properties.get('status') == 'replaced':
            return self.uuid
        return properties.get(self.name_key, None) or self.uuid

    def __acl__(self):
        # Don't finalize to avoid validation here.
        properties = self.upgrade_properties().copy()
        status = properties.get('status')
        if status is None:
            return [(Allow, Everyone, ['list', 'add', 'view', 'edit', 'add_unvalidated', 'index',
                                       'storage', 'import_items', 'search'])]
        return self.STATUS_ACL.get(status, ALLOW_LAB_SUBMITTER_EDIT)

    def __ac_local_roles__(self):
        roles = {}
        properties = self.upgrade_properties().copy()
        if 'lab' in properties:
            lab_submitters = 'submits_for.%s' % properties['lab']
            roles[lab_submitters] = 'role.lab_submitter'
        if 'award' in properties:
            # TODO: This will fail. There is no function _award_viewing_group anywhere in snovault.
            #       Maybe we should create a hook that Fourfront but not CGAP can set that knows about awards.
            #       Probably 'lab' above has the same issues. -kmp 4-Jul-2020
            viewing_group = _award_viewing_group(properties['award'], find_root(self))
            if viewing_group is not None:
                viewing_group_members = 'viewing_group.%s' % viewing_group
                roles[viewing_group_members] = 'role.viewing_group_member'
        return roles

    def unique_keys(self, properties):
        keys = super(Item, self).unique_keys(properties)
        if 'accession' not in self.schema['properties']:
            return keys
        keys.setdefault('accession', []).extend(properties.get('alternate_accessions', []))
        if properties.get('status') != 'replaced' and 'accession' in properties:
            keys['accession'].append(properties['accession'])
        return keys

    @calculated_property(schema={
        "title": "Display Title",
        "description": "A calculated title for every object in 4DN",
        "type": "string"
    },)
    def display_title(self):
        """create a display_title field."""
        # Unused
        # display_title = ""
        look_for = [
            "title",
            "name",
            "location_description",
            "accession",
        ]
        for field in look_for:
            # special case for user: concatenate first and last names
            display_title = self.properties.get(field, None)
            if display_title:
                return display_title
        # if none of the existing terms are available, use @type + date_created
        try:
            # TODO: PyCharm thinks self.__class__.__name__ will return 'property' not 'string'.
            #       That's probably a bug in PyCharm I should report. -kmp 4-Jul-2020
            type_date = self.__class__.__name__ + " from " + self.properties.get("date_created", None)[:10]
            return type_date
        # last resort, use uuid
        except Exception:
            return self.properties.get('uuid', None)


@calculated_property(context=Item.Collection, category='action')
def add(context, request):
    if request.has_permission('add'):
        return {
            'name': 'add',
            'title': 'Add',
            'profile': '/profiles/{ti.name}.json'.format(ti=context.type_info),
            'href': '{item_uri}#!add'.format(item_uri=request.resource_path(context)),
            }


@calculated_property(context=Item, category='action')
def edit(context, request):
    if request.has_permission('edit'):
        return {
            'name': 'edit',
            'title': 'Edit',
            'profile': '/profiles/{ti.name}.json'.format(ti=context.type_info),
            'href': '{item_uri}#!edit'.format(item_uri=request.resource_path(context)),
        }


@calculated_property(context=Item, category='action')
def edit_json(context, request):
    if request.has_permission('edit'):
        return {
            'name': 'edit-json',
            'title': 'Edit JSON',
            'profile': '/profiles/{ti.name}.json'.format(ti=context.type_info),
            'href': '{item_uri}#!edit-json'.format(item_uri=request.resource_path(context)),
        }


@abstract_collection(
    name='abstractItemTests',
    unique_key='accession',
    properties={
        'title': "AbstractItemTests",
        'description': "Abstract Item that is inherited for testing",
    })
class AbstractItemTest(Item):
    item_type = 'AbstractItemTest'
    base_types = ['AbstractItemTest'] + Item.base_types
    name_key = 'accession'


@collection(
    name='abstract-item-test-sub-items',
    unique_key='accession',
    properties={
        'title': "AbstractItemTestSubItems",
        'description': "Item based off of AbstractItemTest"
    })
class AbstractItemTestSubItem(AbstractItemTest):
    item_type = 'abstract_item_test_sub_item'
    schema = load_schema('snovault:test_schemas/AbstractItemTestSubItem.json')


@collection(
    name='abstract-item-test-second-sub-items',
    unique_key='accession',
    properties={
        'title': 'AbstractItemTestSecondSubItems',
        'description': "Second item based off of AbstractItemTest"
    })
class AbstractItemTestSecondSubItem(AbstractItemTest):
    item_type = 'abstract_item_test_second_sub_item'
    schema = load_schema('snovault:test_schemas/AbstractItemTestSecondSubItem.json')


@collection(
    name='embedding-tests',
    unique_key='accession',
    properties={
        'title': 'EmbeddingTests',
        'description': 'Listing of EmbeddingTests'
    })
class EmbeddingTest(Item):
    item_type = 'embedding_test'
    schema = load_schema('snovault:test_schemas/EmbeddingTest.json')
    name_key = 'accession'

    # use TestingDownload to test
    embedded_list = [
        'attachment.*'
    ]

# Formerly b58bc82f-249e-418f-bbcd-8a80af2e58d3
NESTED_OBJECT_LINK_TARGET_GUID_1 = 'f738e192-85f4-4886-bdc4-e099a2e2102a'
NESTED_OBJECT_LINK_TARGET_GUID_2 = 'c48dfba9-ad62-4b32-ad29-a4b6ca47e5d4'

# Formerly 100a0bb8-2974-446b-a5de-6937aa313be4
NESTED_EMBEDDING_CONTAINER_GUID = "6d3e9e27-cf87-4103-aa36-9f481c9d9a66"

NESTED_OBJECT_LINK_TARGET_GUIDS = [  # These IDs are defined in test_views.py so this is a low-tech revlink
    NESTED_OBJECT_LINK_TARGET_GUID_1,
    NESTED_OBJECT_LINK_TARGET_GUID_2,
]


@collection(
    name='nested-embedding-container',
    unique_key='accession',
    properties={
        'title': 'NestedEmbeddingContainer',
        'description': 'Test of ...'
    })
class NestedEmbeddingContainer(Item):
    item_type = 'nested_embedding_container'
    schema = load_schema('snovault:test_schemas/NestedEmbeddingContainer.json')
    name_key = 'accession'

    # use TestingDownload to test
    embedded_list = [
        'link_to_nested_object.associates.x',
        'link_to_nested_object.associates.y',
        'link_to_nested_objects.associates.x',
        'link_to_nested_objects.associates.y',
        'nested_calculated_property.associates.x',
        'nested_calculated_property.associates.y',
    ]

    @calculated_property(schema={
            "title": "Nested Calculated property",
            "description": "something calculated",
            "type": "array",
            "items": {
                "title": "Nested Calculated Property",
                "type": ["string", "object"],
                "linkTo": "NestedObjectLinkTarget"
            }
        })
    def nested_calculated_property(self):
        return copy.copy(NESTED_OBJECT_LINK_TARGET_GUIDS)


@collection(
    name='nested-object-link-target',
    unique_key='accession',
    properties={
        'title': 'NestedObjectLinkTarget',
        'description': '...'
    })
class NestedObjectLinkTarget(Item):
    item_type = 'nested_object_link_target'
    schema = load_schema('snovault:test_schemas/NestedObjectLinkTarget.json')
    name_key = 'accession'


@collection(
    'testing-downloads',
    properties={
        'title': 'Test download collection',
        'description': 'Testing. Testing. 1, 2, 3.',
    },
)
class TestingDownload(ItemWithAttachment):
    item_type = 'testing_download'
    schema = load_schema('snovault:test_schemas/TestingDownload.json')


@collection('testing-link-sources-sno', unique_key='testing_link_sources-sno:name')
class TestingLinkSourceSno(Item):
    item_type = 'testing_link_source_sno'
    schema = load_schema('snovault:test_schemas/TestingLinkSourceSno.json')
    embedded_list = ['target_es.status', 'target.status']


@collection('testing-link-aggregates-sno')
class TestingLinkAggregateSno(Item):
    item_type = 'testing_link_aggregate_sno'
    schema = load_schema('snovault:test_schemas/TestingLinkAggregateSno.json')
    aggregated_items = {
        "targets": ['target.uuid', 'test_description']
    }


@collection('testing-link-targets-sno', unique_key='testing_link_target_sno:name')
class TestingLinkTargetSno(Item):
    item_type = 'testing_link_target_sno'
    name_key = 'name'
    schema = load_schema('snovault:test_schemas/TestingLinkTargetSno.json')
    rev = {
        'reverse': ('TestingLinkSourceSno', 'target'),
    }
    filtered_rev_statuses = ('deleted', 'replaced')
    embedded_list = [
        'reverse.name',
    ]

    def rev_link_atids(self, request, rev_name):
        conn = request.registry[CONNECTION]
        return [request.resource_path(conn[uuid]) for uuid in
                self.get_filtered_rev_links(request, rev_name)]

    @calculated_property(schema={
        "title": "Sources",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkTo": "TestingLinkSourceSno",
        },
    })
    def reverse(self, request):
        return self.rev_link_atids(request, "reverse")


# Renamed from TestingPostPutPatch to TestingPostPutPatchSno so that indices
# would not coincide with Fourfront tests, which also use that index name
@collection(
    'testing-post-put-patch-sno',
    acl=[
        (Allow, 'group.submitter', ['add', 'edit', 'view']),
    ],
)
class TestingPostPutPatchSno(Item):
    item_type = 'testing_post_put_patch_sno'
    embedded_list = ['protected_link.*']
    schema = load_schema('snovault:test_schemas/TestingPostPutPatchSno.json')


@collection('testing-server-defaults')
class TestingServerDefault(Item):
    item_type = 'testing_server_default'
    schema = load_schema('snovault:test_schemas/TestingServerDefault.json')


@collection('testing-dependencies')
class TestingDependencies(Item):
    item_type = 'testing_dependencies'
    schema = load_schema('snovault:test_schemas/TestingDependencies.json')


@view_config(name='testing-render-error', request_method='GET')
def testing_render_error(request):
    return {
        '@type': ['TestingRenderError', 'Item'],
        '@id': request.path,
        'title': 'Item triggering a render error',
    }


@view_config(context=TestingPostPutPatchSno, name='testing-retry')
def testing_retry(context, request):

    model = context.model
    request.environ['_attempt'] = request.environ.get('_attempt', 0) + 1

    if request.environ['_attempt'] == 1:
        raise TransientError()

    return {
        'attempt': request.environ['_attempt'],
        'detached': inspect(model).detached,
    }


# properties_datastore sets makes this collection stored in ES
@collection('testing-link-targets-elastic-search',
            unique_key='testing_link_target_elastic_search:name',
            properties_datastore='elasticsearch')
class TestingLinkTargetElasticSearch(Item):
    """
    Like TestingLinkTargetSno, but leverages ElasticSearch storage exclusively.
    Includes a linkTo and a rev_link to test multiple behaviors.
    """
    item_type = 'testing_link_target_elastic_search'
    name_key = 'name'
    schema = load_schema('snovault:test_schemas/TestingLinkTargetElasticSearch.json')
    rev = {
        'reverse_es': ('TestingLinkSourceSno', 'target_es'),
    }
    filtered_rev_statuses = ('deleted', 'replaced')
    aggregated_items = {
        "ppp": ['simple1', 'uuid']
    }
    embedded_list = [
        'reverse_es.name',
        'ppp.simple1'
    ]

    def rev_link_atids(self, request, rev_name):
        conn = request.registry[CONNECTION]
        return [request.resource_path(conn[uuid]) for uuid in
                self.get_filtered_rev_links(request, rev_name)]

    @calculated_property(schema={
        "title": "Sources",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkTo": "TestingLinkSourceSno",
        },
    })
    def reverse_es(self, request):
        return self.rev_link_atids(request, "reverse_es")


@collection('testing-calculated-properties',
            unique_key='testing_calculated_properties:name')
class TestingCalculatedProperties(Item):
    """ An item type that has calculated properties on it meant for testing. """
    item_type = 'testing_calculated_properties'
    name_key = 'name'
    schema = load_schema('snovault:test_schemas/TestingCalculatedProperties.json')

    @calculated_property(schema={
        "title": "combination",
        "type": "object"
    })
    def combination(self, name, foo, bar):
        return {
            'name': name,
            'foo': foo,
            'bar': bar
        }

    @calculated_property(schema={  # THIS is the schema that will be "seen"
        "title": "nested",
        "type": "object",
        "sub-embedded": True,  # REQUIRED TO INDICATE
        "properties": {
            "key": {
                "type": "string"
            },
            "value": {
                "type": "string"
            },
            "keyvalue": {
                "type": "string"
            }
        }
    })
    def nested(self, nested):  # nested is the calculated property path that will update and the input
        """ Implements sub-embedded-object calculated properties.

            When merged into properties looks like this:
            {
                'nested' : {
                    'keyvalue': val
                }
            }
        """
        # return a dictionary with all sub-embedded key, value pairs on this sub-embedded path
        return {'keyvalue': nested['key'] + nested['value']}

    @calculated_property(schema={  # IN ORDER TO GET CORRECT MAPPINGS, YOU MUST SPECIFY THE ENTIRE SCHEMA
        "title": "nested2",
        "type": "array",
        "sub-embedded": True,
        "items": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string"
                },
                "value": {
                    "type": "string"
                },
                "keyvalue": {
                    "type": "string"
                }
            }
        }
    })
    def nested2(self, nested2):
        """ Implements sub-embedded object calculated property on array (of objects) type field

            When merged into properties looks like this:
            {
                'nested2': [
                    {
                        keyvalue: val
                    },

                    {
                        keyvalue: val
                    }
                ]
            }
        """
        # return an ARRAY of dictionaries
        result = []
        for entry in nested2:
            result.append({
                'keyvalue': entry['key'] + entry['value']
            })
        return result


@collection('testing-mixins', unique_key='testing_mixins:name')
class TestingMixins(Item):
    item_type = 'testing_mixins'
    name_key = 'name'
    schema = load_schema('snovault:test_schemas/TestingMixins.json')


@collection('testing-nested-enabled', unique_key='testing_nested_enabled:name')
class TestingNestedEnabled(Item):
    """ Type intended to test enabling nested mappings per-field. """
    item_type = 'testing_nested_enabled'
    name_key = 'name'
    schema = load_schema('snovault:test_schemas/TestingNestedEnabled.json')

    @calculated_property(schema={
        "title": "enabled_array_of_objects_in_calc_prop",
        "description": "Tests mapping calculated properties with enable_nested works correctly",
        "type": "array",
        "items": {
            "type": "object",
            "enable_nested": True,
            "properties": {
                "string_field": {
                    "type": "string"
                },
                "numerical_field": {
                    "type": "integer"
                }
            }
        }
    })
    def enabled_array_of_objects_in_calc_prop(self):
        """ This one will get mapped with nested """
        return [{
            'string_field': 'hello',
            'numerical_field': 0
        }]

    @calculated_property(schema={
        "title": "array_of_objects_in_calc_prop",
        "description": "Tests mapping calculated properties with disable_nested works correctly",
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "string_field": {
                    "type": "string"
                },
                "numerical_field": {
                    "type": "integer"
                }
            }
        }
    })
    def disabled_array_of_objects_in_calc_prop(self):
        """ This one will not get mapped with nested since it was not explicitly enabled """
        return [{
            'string_field': 'world',
            'numerical_field': 100
        }]
