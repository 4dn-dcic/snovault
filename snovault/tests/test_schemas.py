import pytest
from unittest import mock
import contextlib

from .. import project_app
from ..interfaces import TYPES
from ..schema_utils import load_schema
from .test_views import PARAMETERIZED_NAMES
from snovault import schema_views
from snovault.schema_views import (
    _get_required_propnames_from_oneof,
    _has_property_attr_with_val,
    _get_item_name_from_schema_id,
    _is_submittable_schema,
    _annotate_submittable_props,
    _get_submittable_props,
    _get_submittable_schema
)


@pytest.mark.parametrize('schema', PARAMETERIZED_NAMES)
def test_load_schema(schema):
    assert load_schema('snovault:test_schemas/%s' % (schema + '.json'))


def test_dependencies(testapp):
    collection_url = '/testing-dependencies/'
    testapp.post_json(collection_url, {'dep1': 'dep1', 'dep2': 'dep2'}, status=201)
    testapp.post_json(collection_url, {'dep1': 'dep1'}, status=422)
    testapp.post_json(collection_url, {'dep2': 'dep2'}, status=422)
    testapp.post_json(collection_url, {'dep1': 'dep1', 'dep2': 'disallowed'}, status=422)


def test_changelogs(testapp, registry):
    for typeinfo in registry[TYPES].by_item_type.values():
        changelog = typeinfo.schema.get('changelog')
        if changelog is not None:
            res = testapp.get(changelog)
            assert res.status_int == 200, changelog
            assert res.content_type == 'text/markdown'


def test_schemas_etag(testapp):
    etag = testapp.get('/profiles/', status=200).etag
    assert etag
    testapp.get('/profiles/', headers={'If-None-Match': etag}, status=304)


class MockedAppProject:
    def __init__(self, sub_item_names=[], sub_prop=None, excl_props=[], excl_attrs={}):
        self.sub_item_names = sub_item_names
        self.sub_prop = sub_prop
        self.excl_props = excl_props
        self.excl_attrs = excl_attrs

    def get_submittable_item_names(self):
         return self.sub_item_names
    
    def get_prop_for_submittable_items(self):
        return self.sub_prop
    
    def get_properties_for_exclusion(self):
        return self.excl_props
    
    def get_attributes_for_exclusion(self):
        return self.excl_attrs


@contextlib.contextmanager
def mocked_app_project(sub_item_names=[], sub_prop=None, excl_props=[], excl_attrs={}):
    yield MockedAppProject(sub_item_names, sub_prop, excl_props, excl_attrs)

def test_is_submittable_schema_given_item_name(schema_for_testing):
    with mocked_app_project(sub_item_names=['tester']):
        with mock.patch.object(project_app, "app_project", mocked_app_project):
            import pdb; pdb.set_trace()
            ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing.get('properties'))
            assert ans is True

@pytest.fixture
def schema_for_testing():
    return {
        "title": "Tester",
        "$id": "/profiles/tester.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["name"],
        "dependentRequired": {
                "test_prop_A": ["test_prop_B"],
                "test_prop_B": ["test_prop_A"]
        },
        "oneOf": [
            {"required": ["aliases"]},
            {"required": ["submitted_id"]}
        ],
        "identifyingProperties": [
            "submitted_id",
            "uuid"
        ],
        "additionalProperties": False,
        "mixinProperties": [
            {"$ref": "mixins.json#/aliases"},
            {"$ref": "mixins.json#/name"},
            {"$ref": "mixins.json#/submitted_id"},
            {"$ref": "mixins.json#/uuid"},
            {"$ref": "mixins.json#/schema_version"}
        ],
        "properties": {
            "last_modified": {
                "title": "Last Modified",
                "exclude_from": ["FFedit-create"],
                "type": "object",
                "additionalProperties": False, 
                "properties": {
                    "date_modified": {
                        "title": "Date Modified", 
                        "description": "Do not submit, value is assigned by the server. The date the object is modified.",
                        "type": "string",
                        "anyOf": [{"format": "date-time"}, {"format": "date"}],
                        "permission": "restricted_fields"
                    },
                    "modified_by": {
                        "title": "Modified By",
                        "description": "Do not submit, value is assigned by the server. The user that modfied the object.",
                        "type": "string",
                        "linkTo": "User",
                        "permission": "non_restricted"
                    }
                }
            },   
            "date_created": {
                "rdfs:subPropertyOf": "dc:created",
                "title": "Date Created",
                "type": "string",
                "anyOf": [{"format": "date-time"}, {"format": "date"}]
            },
            "uuid": {
                "title": "UUID", "type": "string",
                "format": "uuid",
                "serverDefault": "uuid4",
                "permission": "restricted_fields"
            },
            "schema_version": {
                "title": "Schema Version",
                "type": "string", 
                "pattern": "^\\d+(\\.\\d+)*$", 
                "default": "1"
            },
            "submitted_id": {
                "title": "Submitter ID",
                "description": "Identifier on submission",
                "type": "string",
                "uniqueKey": "submitted_id"
            },
            "aliases": {
                "title": "Aliases",
                "description": "Lab specific ID (e.g. dcic_lab:my_biosample1).",
                "type": "array",
                "uniqueItems": True,
                "items": {
                    "uniqueKey": "alias",
                    "title": "Lab alias",
                    "type": "string"
                }
            },
            "name": {
                "title": "Name",
                "type": "string"
            },
            "test_prop_A": {
                "title": "A",
                "type": "stirng"
            },
            "test_prop_B": {
                "title": "B",
                "type": "stirng"
            },
        }
    }



@pytest.fixture
def attrs_to_check():
    return {"permission": ["restricted_fields"], "exclude_from": ["FFedit-create"]}


@pytest.fixture
def props_with_attrs():
    return {
        "project": {
            "title": "Project",
            "description": "Project associated with the submission.",
            "type": "string",
            "exclude_from": [
                "FFedit-create"
            ],
            "linkTo": "Project",
            "serverDefault": "userproject"
        },
        "documents": {
            "title": "Documents",
            "description": "Documents that provide additional information (not data file).",
            "comment": "See Documents sheet or collection for existing items.",
            "type": "array",
            "uniqueItems": True,
            "items": {
                "title": "Document",
                "description": "A document that provides additional information (not data file).",
                "type": "string",
                "linkTo": "Document"
            }
        },
        "modified_by": {
            "title": "Modified By",
            "description": "Do not submit, value is assigned by the server. The user that modfied the object.",
            "type": "string",
            "linkTo": "User",
            "permission": "restricted_fields"
        }
    }


def test_get_required_propnames_from_oneof(schema_for_testing):
    expected_props = ['aliases', 'submitted_id']
    fetched_props = _get_required_propnames_from_oneof(schema_for_testing)
    assert set(expected_props) == set(fetched_props)


def test_get_required_propnames_from_oneof_without_oneof(schema_for_testing):
    """ checks that if no oneOf stanza empty list is result"""
    del schema_for_testing['oneOf']
    fetched_props = _get_required_propnames_from_oneof(schema_for_testing)
    assert not fetched_props


def test_has_property_attr_with_val(props_with_attrs, attrs_to_check):
    for propname, propattrs in props_with_attrs.items():
        if propname == "documents":
            assert not _has_property_attr_with_val(propattrs, attrs_to_check)
        else:
            assert _has_property_attr_with_val(propattrs, attrs_to_check)


def test_has_property_attr_with_val_diff_val(props_with_attrs, attrs_to_check):
    """
    ensures that if a named attribute is present but the value of that attrbute is different
    if returns False
    """
    prop_attrs = props_with_attrs.get("modified_by")
    # update a checked attribute to a diff value
    prop_attrs["permission"] = 'anyone_can_access'
    assert not _has_property_attr_with_val(prop_attrs, attrs_to_check)


def test_get_item_name_from_schema_id():
    name = 'access_key'
    ak_strings = ['/profiles/access_key.json', '/profiles/access_key', 
                  'access_key.json', 'access_key']
    other_strings = ['profiles/access_key.jsonbgood', '']

    for s in ak_strings:
        assert name == _get_item_name_from_schema_id(s)
    for os in other_strings:
        assert name != _get_item_name_from_schema_id(os)


def test_submittable(testapp, registry):
    test_uri = '/can-submit/access_key.json'
    res = testapp.get(test_uri, status=200)
    import pdb; pdb.set_trace()
    assert not res.json

def test_submittables(testapp, registry):
    test_uri = '/can-submit/'
    res = testapp.get(test_uri, status=200)
    import pdb; pdb.set_trace()
    assert not res.json

