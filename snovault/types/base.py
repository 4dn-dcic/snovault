"""base class creation for all the schemas that exist."""
from dcicutils.misc_utils import exported
from pyramid.security import (
    # ALL_PERMISSIONS,
    Allow,
    Deny,
    # DENY_ALL,
    Everyone,
)
from pyramid.view import (
    view_config,
)
import re
import string
from .. import Item, Collection, AbstractCollection, abstract_collection, calculated_property
from ..util import debug_log
from ..validators import (
    validate_item_content_post,
    validate_item_content_put,
    validate_item_content_patch,
    validate_item_content_in_place,
    no_validate_item_content_post,
    no_validate_item_content_put,
    no_validate_item_content_patch
)
from ..crud_views import (
    collection_add as sno_collection_add,
    item_edit
)
from ..interfaces import CONNECTION
from ..server_defaults import get_userid, add_last_modified
from .acl import (
    ONLY_ADMIN_VIEW_ACL,
    PUBLIC_ACL,
    DELETED_ACL
)
exported(
    Allow, Deny, Everyone,
    abstract_collection,
    validate_item_content_put,
    validate_item_content_patch,
    validate_item_content_in_place,
    no_validate_item_content_post,
    no_validate_item_content_put,
    no_validate_item_content_patch,
    item_edit,
    CONNECTION,
    get_userid,
    add_last_modified,
    ONLY_ADMIN_VIEW_ACL,
    PUBLIC_ACL,
    DELETED_ACL
)


def get_item_or_none(request, value, itype=None, frame='object'):
    """
    Return the view of an item with given frame. Can specify different types
    of `value` for item lookup

    Args:
        request: the current Request
        value (str): String item identifier or a dict containing @id/uuid
        itype (str): Optional string collection name for the item (e.g. /file-formats/)
        frame (str): Optional frame to return. Defaults to 'object'

    Returns:
        dict: given view of the item or None on failure
    """
    item = None

    if isinstance(value, dict):
        if 'uuid' in value:
            value = value['uuid']
        elif '@id' in value:
            value = value['@id']

    svalue = str(value)

    # Below case is for UUIDs & unique_keys such as accessions, but not @ids
    if not svalue.startswith('/') and not svalue.endswith('/'):
        svalue = '/' + svalue + '/'
        if itype is not None:
            svalue = '/' + itype + svalue

    # Request.embed will attempt to get from ES for frame=object/embedded
    # If that fails, get from DB. Use '@@' syntax instead of 'frame=' because
    # these paths are cached in indexing
    try:
        item = request.embed(svalue, '@@' + frame)
    except Exception:
        pass

    # could lead to unexpected errors if == None
    return item


def set_namekey_from_title(properties):
    name = None
    if properties.get('title'):
        exclude = set(string.punctuation.replace('-', ''))
        name = properties['title'].replace('&', ' n ')
        name = ''.join(ch if ch not in exclude and ch != ' ' else '-' for ch in name)
        name = re.sub(r"[-]+", '-', name).strip('-').lower()
    return name


def validate_item_type_of_linkto_field(context, request):
    """We are doing this case by case on item specific types files,
    but might want to carry it here if filter is used more often.
    If any of the submitted fields contain an ff_flag property starting with "filter",
    the field in the filter is used for validating the type of the linked item.
    Example: file has field file_format which is a linkTo FileFormat.
    FileFormat items contain a field called "valid_item_types".
    We have the ff_flag on file_format field called "filter:valid_item_types"."""
    pass


class AbstractCollection(AbstractCollection):
    """smth."""

    def __init__(self, *args, **kw):
        try:
            self.lookup_key = kw.pop('lookup_key')
        except KeyError:
            pass
        super(AbstractCollection, self).__init__(*args, **kw)

    def get(self, name, default=None):
        """
        heres' and example of why this is the way it is:
        ontology terms have uuid or term_id as unique ID keys
        and if neither of those are included in post, try to
        use term_name such that:
        No - fail load with non-existing term message
        Multiple - fail load with ‘ambiguous name - more than 1 term with that name exist use ID’
        Single result - get uuid and use that for post/patch
        """
        resource = super(AbstractCollection, self).get(name, None)
        if resource is not None:
            return resource
        if ':' in name:
            resource = self.connection.get_by_unique_key('alias', name)
            if resource is not None:
                if not self._allow_contained(resource):
                    return default
                return resource
        if getattr(self, 'lookup_key', None) is not None:
            # lookup key translates to query json by key / value and return if only one of the
            # item type was found... so for keys that are mostly unique, but do to whatever
            # reason (bad data mainly..) can be defined as unique keys
            item_type = self.type_info.item_type
            resource = self.connection.get_by_json(self.lookup_key, name, item_type)
            if resource is not None:
                if not self._allow_contained(resource):
                    return default
                return resource
        return default


class Collection(Collection, AbstractCollection):
    """smth."""

    def __init__(self, *args, **kw):
        """smth."""
        super(Collection, self).__init__(*args, **kw)
        if hasattr(self, '__acl__'):
            return


@calculated_property(context=Item.AbstractCollection, category='action')
def add(context, request):
    """smth."""
    if request.has_permission('add', context):
        type_name = context.type_info.name
        return {
            'name': 'add',
            'title': 'Add',
            'profile': '/profiles/{name}.json'.format(name=type_name),
            'href': '/search/?type={name}&currentAction=add'.format(name=type_name),
        }


@calculated_property(context=Item, category='action')
def edit(context, request):
    """smth."""
    if request.has_permission('edit'):
        return {
            'name': 'edit',
            'title': 'Edit',
            'profile': '/profiles/{ti.name}.json'.format(ti=context.type_info),
            'href': '{item_uri}?currentAction=edit'.format(item_uri=request.resource_path(context)),
        }


@calculated_property(context=Item, category='action')
def create(context, request):
    if request.has_permission('create'):
        return {
            'name': 'create',
            'title': 'Create',
            'profile': '/profiles/{ti.name}.json'.format(ti=context.type_info),
            'href': '{item_uri}?currentAction=create'.format(item_uri=request.resource_path(context)),
        }


@view_config(
    context=Collection,
    permission='add',
    request_method='POST',
    # validators=[]  # TURNS OFF VALIDATION HERE ([validate_item_content_post] previously)
    validators=[validate_item_content_post]
)
@view_config(
    context=Collection,
    permission='add_unvalidated',
    request_method='POST',
    validators=[no_validate_item_content_post],
    request_param=['validate=false']
)
@debug_log
def collection_add(context, request, render=None):

    # institution_needed = False
    # project_needed = False
    # data = request.json
    # schema = context.type_info.schema
    #
    # required_properties = schema.get("required", [])
    # if "institution" in required_properties and "institution" not in data:
    #     institution_needed = True
    #
    # if "project" in required_properties and "project" not in data:
    #     project_needed = True
    #
    # if request.authenticated_userid and (institution_needed or project_needed):
    #     namespace, userid = request.authenticated_userid.split(".", 1)
    #     user_item = get_item_or_none(request, userid, itype="/users/", frame="object")
    #     new_data = data.copy()
    #     if institution_needed and "institution" in user_item:
    #         new_data["institution"] = user_item["institution"]
    #     if project_needed and "project" in user_item:
    #         new_data["project"] = user_item["project"]
    #
    #     # Override initial JSON body of request (hacky? better way?)
    #     setattr(request, "json", new_data)
    #
    # # Perform validation that would occur otherwise
    # validate_item_content_post(context, request)
    # if request.errors:
    #     return HTTPUnprocessableEntity(
    #         json={'errors': request.errors},
    #         content_type='application/json'
    #     )
    return sno_collection_add(context, request, render)
