from pyramid.security import (
    ALL_PERMISSIONS,
    Allow,
    Authenticated,
    Deny,
    DENY_ALL,
    Everyone,
    principals_allowed_by_permission,
)
from pyramid.traversal import find_root, traverse
from pyramid.view import view_config
from sqlalchemy import inspect
from transaction.interfaces import TransientError
from .. import (
    AbstractCollection as BaseAbstractCollection,
    Collection as BaseCollection,
    Item as BaseItem,
    calculated_property,
    collection,
    abstract_collection,
    load_schema,
)
from ..attachment import ItemWithAttachment
from ..interfaces import CONNECTION
from .root import TestRoot


def includeme(config):
    config.scan(__name__)


# Item acls

ONLY_ADMIN_VIEW = [
    (Allow, 'group.admin', ['view', 'edit']),
    (Allow, 'group.read-only-admin', ['view']),
    (Allow, 'remoteuser.INDEXER', ['view']),
    (Allow, 'remoteuser.EMBED', ['view']),
    (Allow, Everyone, ['view', 'edit']),
]

ALLOW_EVERYONE_VIEW = [
    (Allow, Everyone, ['view', 'list']),
] + ONLY_ADMIN_VIEW


ALLOW_VIEWING_GROUP_VIEW = [
    (Allow, 'role.viewing_group_member', 'view'),
] + ONLY_ADMIN_VIEW

ALLOW_LAB_SUBMITTER_EDIT = [
    (Allow, 'role.viewing_group_member', 'view'),
    (Allow, 'role.lab_submitter', 'edit'),
] + ONLY_ADMIN_VIEW

ALLOW_CURRENT_AND_SUBMITTER_EDIT = [
    (Allow, Everyone, 'view'),
    (Allow, 'role.lab_submitter', 'edit'),
] + ONLY_ADMIN_VIEW

ALLOW_CURRENT = [
    (Allow, Everyone, 'view'),
] + ONLY_ADMIN_VIEW

DELETED = [
    (Deny, Everyone, 'visible_for_edit')
] + ONLY_ADMIN_VIEW


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
        resource = super(BaseAbstractCollection, self).get(name, None)
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
        super(BaseCollection, self).__init__(*args, **kw)
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
            return [(Allow, Everyone, ['list', 'add', 'view', 'edit', 'add_unvalidated', 'index', 'storage', 'import_items', 'search'])]
        return self.STATUS_ACL.get(status, ALLOW_LAB_SUBMITTER_EDIT)

    def __ac_local_roles__(self):
        roles = {}
        properties = self.upgrade_properties().copy()
        if 'lab' in properties:
            lab_submitters = 'submits_for.%s' % properties['lab']
            roles[lab_submitters] = 'role.lab_submitter'
        if 'award' in properties:
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
        display_title = ""
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
            type_date = self.__class__.__name__ + " from " + self.properties.get("date_created", None)[:10]
            return type_date
        # last resort, use uuid
        except:
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
