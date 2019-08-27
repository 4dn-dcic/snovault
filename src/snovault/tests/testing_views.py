from pyramid.security import (
    Allow,
)
from pyramid.view import view_config
from snovault import (
    Item,
    calculated_property,
    collection,
    abstract_collection,
    load_schema,
)
from snovault.attachment import ItemWithAttachment
from snovault.interfaces import CONNECTION


def includeme(config):
    config.scan(__name__)


@view_config(name='testing-user', request_method='GET')
def user(request):
    return {
        'authenticated_userid': request.authenticated_userid,
        'effective_principals': request.effective_principals,
    }


@view_config(name='testing-allowed', request_method='GET')
def allowed(context, request):
    from pyramid.security import principals_allowed_by_permission
    permission = request.params.get('permission', 'view')
    return {
        'has_permission': bool(request.has_permission(permission, context)),
        'principals_allowed_by_permission': principals_allowed_by_permission(context, permission),
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
    name='AbstractItemTestSubItems',
    unique_key='accession',
    properties={
        'title': "AbstractItemTestSubItems",
        'description': "Item based off of AbstractItemTest"
    })
class AbstractItemTestSubItem(AbstractItemTest):
    item_type = 'AbstractItemTestSubItem'
    schema = {
        'title': "AbstractItemTestSubItem",
        'description': "This is a testing item",
        'properties': {},
    }


@collection(
    name='AbstractItemTestSecondSubItems',
    unique_key='accession',
    properties={
        'title': 'AbstractItemTestSecondSubItems',
        'description': "Second item based off of AbstractItemTest"
    })
class AbstractItemTestSecondSubItem(AbstractItemTest):
    item_type = 'AbstractItemTestSecondSubItem'
    schema = {
        'title': 'AbstractItemTestSecondSubItem',
        'description': "This is a second testing item",
        'properties': {},
    }


@collection(
    name='EmbeddingTests',
    unique_key='accession',
    properties={
        'title': 'EmbeddingTests',
        'description': 'Listing of EmbeddingTests'
    })
class EmbeddingTest(Item):
    item_type = 'EmbeddingTest'
    schema = load_schema('snovault:test_schemas/embedding_test.json')
    name_key = 'accession'

    # use TestingDownload to test
    embedded_list = [
        'attachment'
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
    schema = {
        'type': 'object',
        'properties': {
            'attachment': {
                'type': 'object',
                'attachment': True,
                'properties': {
                    'type': {
                        'type': 'string',
                        'enum': ['image/png'],
                    }
                }
            },
            'attachment2': {
                'type': 'object',
                'attachment': True,
                'properties': {
                    'type': {
                        'type': 'string',
                        'enum': ['image/png'],
                    }
                }
            }
        }
    }


@collection('testing-link-sources-sno', unique_key='testing_link_sources-sno:name')
class TestingLinkSourceSno(Item):
    item_type = 'testing_link_source_sno'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'uniqueKey': True,
            },
            'uuid': {
                'type': 'string',
            },
            'target': {
                'type': 'string',
                'linkTo': 'TestingLinkTargetSno',
            },
            'ppp': {
                'type': 'string',
                'linkTo': 'TestingPostPutPatchSno',
            },
            'status': {
                'type': 'string',
            },
        },
        'required': ['target'],
        'additionalProperties': False,
    }


@collection('testing-link-aggregates-sno')
class TestingLinkAggregateSno(Item):
    item_type = 'testing_link_aggregate_sno'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
            },
            'uuid': {
                'type': 'string',
            },
            'targets': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'test_description': {
                            'type': 'string'
                        },
                        'target':{
                            'type': 'string',
                            'linkTo': 'TestingLinkTargetSno'
                        }
                    }
                }
            },
            'status': {
                'type': 'string',
            }
        },
        'required': ['targets'],
        'additionalProperties': False,
    }
    aggregated_items = {
        "targets": ['target.uuid', 'test_description']
    }


@collection('testing-link-targets-sno', unique_key='testing_link_target_sno:name')
class TestingLinkTargetSno(Item):
    item_type = 'testing_link_target_sno'
    name_key = 'name'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'uniqueKey': True,
            },
            'uuid': {
                'type': 'string',
            },
            'status': {
                'type': 'string',
            },
        },
        'additionalProperties': False,
    }
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
    schema = {
        'required': ['required'],
        'type': 'object',
        "additionalProperties": False,
        'properties': {
            "schema_version": {
                "type": "string",
                "pattern": "^\\d+(\\.\\d+)*$",
                "requestMethod": [],
                "default": "1",
            },
            "uuid": {
                "title": "UUID",
                "description": "",
                "type": "string",
                "format": "uuid",
                "permission": "import_items",
                "requestMethod": "POST",
            },
            'required': {
                'type': 'string',
            },
            'simple1': {
                'type': 'string',
                'default': 'simple1 default',
            },
            'simple2': {
                'type': 'string',
                'default': 'simple2 default',
            },
            'field_no_default': {
                'type': 'string',
            },
            'enum_no_default': {
                'type': 'string',
                'enum': [
                    '1',
                    '2'
                ]
            },
            'protected': {
                # This should be allowed on PUT so long as value is the same
                'type': 'string',
                'default': 'protected default',
                'permission': 'import_items',
            },
            'protected_link': {
                # This should be allowed on PUT so long as the linked uuid is
                # the same
                'type': 'string',
                'linkTo': 'TestingLinkTargetSno',
                'permission': 'import_items',
            },
            'custom_object': {
                'type': 'object',
                "additionalProperties": True,
                "properties": {
                    "mapped_property": {
                        "type": "string"
                    }
                }
            }
        }
    }


@collection('testing-server-defaults')
class TestingServerDefault(Item):
    item_type = 'testing_server_default'
    schema = {
        'type': 'object',
        'properties': {
            'uuid': {
                'serverDefault': 'uuid4',
                'type': 'string',
            },
            'user': {
                'serverDefault': 'userid',
                'linkTo': 'User',
                'type': 'string',
            },
            'now': {
                'serverDefault': 'now',
                'format': 'date-time',
                'type': 'string',
            },
            'accession': {
                'serverDefault': 'accession',
                'accessionType': 'AB',
                'format': 'accession',
                'type': 'string',
            },
        }
    }


@collection('testing-dependencies')
class TestingDependencies(Item):
    item_type = 'testing_dependencies'
    schema = {
        'type': 'object',
        'dependencies': {
            'dep1': ['dep2'],
            'dep2': ['dep1'],
        },
        'properties': {
            'dep1': {
                'type': 'string',
            },
            'dep2': {
                'type': 'string',
                'enum': ['dep2'],
            },
        }
    }


@view_config(name='testing-render-error', request_method='GET')
def testing_render_error(request):
    return {
        '@type': ['TestingRenderError', 'Item'],
        '@id': request.path,
        'title': 'Item triggering a render error',
    }


@view_config(context=TestingPostPutPatchSno, name='testing-retry')
def testing_retry(context, request):
    from sqlalchemy import inspect
    from transaction.interfaces import TransientError

    model = context.model
    request.environ['_attempt'] = request.environ.get('_attempt', 0) + 1

    if request.environ['_attempt'] == 1:
        raise TransientError()

    return {
        'attempt': request.environ['_attempt'],
        'detached': inspect(model).detached,
    }
