import contextlib
from typing import Any, Dict, List
from unittest import mock

import pytest
from dcicutils.schema_utils import SchemaConstants


from .test_views import PARAMETERIZED_NAMES
from ..interfaces import TYPES
from ..project.schema_views import SnovaultProjectSchemaViews
from ..schema_utils import load_schema
from ..schema_views import (
    SubmissionSchemaConstants,
    _get_conditionally_required_propnames,
    _has_property_attr_with_val,
    _get_item_name_from_schema_id,
    _is_submittable_schema,
    _update_required_annotation,
)


@pytest.mark.parametrize('schema', PARAMETERIZED_NAMES)
def test_load_schema(schema):
    assert load_schema('snovault:test_schemas/%s' % (schema + '.json'))


@pytest.fixture
def loaded_test_schemas():
    return {schema: load_schema(f'snovault:test_schemas/{schema}.json')
            for schema in PARAMETERIZED_NAMES}


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


@contextlib.contextmanager
def mock_get_submittable_item_names(sub_item_names):
    with mock.patch.object(SnovaultProjectSchemaViews, 'get_submittable_item_names', lambda x: sub_item_names):
        yield


@contextlib.contextmanager
def mock_get_prop_for_submittable_items(sub_prop):
    with mock.patch.object(SnovaultProjectSchemaViews, 'get_prop_for_submittable_items', lambda x: sub_prop):
        yield


@contextlib.contextmanager
def mock_get_properties_for_exclusion(excl_props):
    with mock.patch.object(SnovaultProjectSchemaViews, 'get_properties_for_exclusion', lambda x: excl_props):
        yield


@contextlib.contextmanager
def mock_get_attributes_for_exclusion(excl_attrs):
    with mock.patch.object(SnovaultProjectSchemaViews, 'get_attributes_for_exclusion', lambda x: excl_attrs):
        yield


@contextlib.contextmanager
def composite_mocker_for_schema_utils(sub_item_names=[], sub_prop=None, excl_props=[], excl_attrs={}):
    """ This function is generally repurposable but has been customized for a conditional contextmanager
        that will handle any combination """
    conditions_managers = [
        (sub_item_names, mock_get_submittable_item_names),
        (sub_prop, mock_get_prop_for_submittable_items),
        (excl_props, mock_get_properties_for_exclusion),
        (excl_attrs, mock_get_attributes_for_exclusion)
    ]

    with contextlib.ExitStack() as stack:
        for condition, context_manager in conditions_managers:
            if condition:
                stack.enter_context(context_manager(condition))
        yield


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
                "type": "string"
            },
            "test_prop_B": {
                "title": "B",
                "type": "string"
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


def test_get_conditionally_required_propnames(schema_for_testing):
    expected_props = ['aliases', 'submitted_id']
    fetched_props = _get_conditionally_required_propnames(schema_for_testing, 'oneOf')
    assert set(expected_props) == set(fetched_props)


def test_get_conditionally_required_propnames_with_no_condition(schema_for_testing):
    """ checks that if no oneOf stanza empty list is result"""
    del schema_for_testing['oneOf']
    fetched_props = _get_conditionally_required_propnames(schema_for_testing, 'oneOf')
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


def test_is_not_submittable_schema_if_abstract(schema_for_testing):
    schema_for_testing['isAbstract'] = True
    with composite_mocker_for_schema_utils(sub_item_names=['tester']):
        ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing)
        assert ans is False


def test_is_submittable_schema_given_item_name(schema_for_testing):
    with composite_mocker_for_schema_utils(sub_item_names=['tester']):
        ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing)
        assert ans is True


def test_is_not_submittable_schema_given_item_name(schema_for_testing):
    with composite_mocker_for_schema_utils(sub_item_names=['not_tester']):
        ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing)
        assert ans is False


def test_is_submittable_schema_given_key_prop(schema_for_testing):
    with composite_mocker_for_schema_utils(sub_prop='submitted_id'):
        ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing)
        assert ans is True


def test_is_not_submittable_schema_wo_key_prop(schema_for_testing):
    with composite_mocker_for_schema_utils(sub_prop='other_id'):
        ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing)
        assert ans is False


def test_is_not_submittable_schema_wo_info(schema_for_testing):
    with composite_mocker_for_schema_utils():
        ans = _is_submittable_schema(schema_for_testing.get('$id'), schema_for_testing)
        assert ans is False


def test_submittable_no_app_info(testapp):
    """ without providing item name or key prop we expect
        a 404 response with a specific error message
    """
    test_uri = '/submission-schemas/access_key.json'
    res = testapp.get(test_uri, status=404)
    det_txt = f"The schema you requested with {res.request.url} is not submittable or has no submittable fields"
    assert res.json.get('detail') == det_txt


def test_submittable_given_item_name(testapp):
    schema_name = 'testing_note_sno'
    test_schema = load_schema('snovault:test_schemas/TestingNoteSno.json')
    test_uri = f'/submission-schemas/{schema_name}.json'
    with composite_mocker_for_schema_utils(sub_item_names=[schema_name]):
        with mock.patch('snovault.schema_views.schema',
                        return_value=test_schema):
            res = testapp.get(test_uri, status=200).json
            assert res['$id'] == test_schema['$id']
            assert res['title'] == test_schema['title']
            test_props = test_schema['properties']
            res_props = res['properties']
            for tpname, tpval in test_props.items():
                if tpval.get('type') == 'object':
                    assert res_props[tpname].get('title') == tpval.get('title')
                    assert res_props[tpname].get('properties') == tpval.get('properties')
                else:
                    assert res_props[tpname] == tpval


def test_submittable_with_excluded_prop(testapp):
    schema_name = 'testing_note_sno'
    test_schema = load_schema('snovault:test_schemas/TestingNoteSno.json')
    test_uri = f'/submission-schemas/{schema_name}.json'
    inc_props = ['uuid', 'identifier', 'previous_note', 'superseding_note']
    ex_props = ['status', 'schema_version', 'assessment', 'review']
    with composite_mocker_for_schema_utils(sub_item_names=[schema_name],
                                           excl_props=ex_props):
        with mock.patch('snovault.schema_views.schema',
                        return_value=test_schema):
            res = testapp.get(test_uri, status=200).json
            assert res['$id'] == test_schema['$id']
            assert res['title'] == test_schema['title']
            res_props = res['properties']
            for pr in inc_props:
                assert pr in res_props
            for pr in ex_props:
                assert pr not in res_props


def test_submittable_with_excluded_attrs(testapp):
    schema_name = 'testing_note_sno'
    test_schema = load_schema('snovault:test_schemas/TestingNoteSno.json')
    test_uri = f'/submission-schemas/{schema_name}.json'
    with composite_mocker_for_schema_utils(sub_item_names=[schema_name],
                                           excl_attrs={'permission': 'restricted_fields'}):
        with mock.patch('snovault.schema_views.schema',
                        return_value=test_schema):
            res = testapp.get(test_uri, status=200).json
            assert res['$id'] == test_schema['$id']
            assert res['title'] == test_schema['title']
            res_props = res['properties']
            assert 'review' not in res_props
            assessment = res_props.get('assessment')
            assert assessment.get('title') == 'Call Assessment'
            assess_props = assessment.get('properties')
            assert 'call' in assess_props
            assert 'classification' in assess_props
            assert 'date_call_made' not in assess_props
            assert 'call_made_by' not in assess_props


def test_submittables_no_app_info(testapp):
    test_uri = '/submission-schemas/'
    res = testapp.get(test_uri, status=404)
    det_txt = "No submittable schemas found"
    assert res.json.get('detail') == det_txt


def test_submittables_w_uuid(testapp, loaded_test_schemas):
    test_uri = '/submission-schemas/'
    with composite_mocker_for_schema_utils(sub_prop='uuid'):
        with mock.patch('snovault.schema_views.schemas',
                        return_value=loaded_test_schemas):
            res = testapp.get(test_uri, status=200).json
            for sn in res.keys():
                assert sn in PARAMETERIZED_NAMES


@pytest.mark.parametrize(
    "property_,property_schema,required_properties,expected_annotation",
    [
        ("foo", {}, [], False),
        ("foo", {"type": "string"}, ["bar"], False),
        ("foo", {"type": "string"}, ["fu", "foo"], True),
        ("foo", {SchemaConstants.SUBMITTER_REQUIRED: True}, ["bar"], True),
    ],
)
def test_update_required_annotation(
    property_: str,
    property_schema: Dict[str, Any],
    required_properties: List[str],
    expected_annotation: bool,
) -> None:
    _update_required_annotation(
        property_, property_schema, required_properties
    )
    if expected_annotation:
        assert is_required_annotation_present(property_schema)
    else:
        assert not is_required_annotation_present(property_schema)


def is_required_annotation_present(property_schema: Dict[str, Any]) -> bool:
    return property_schema.get(SubmissionSchemaConstants.IS_REQUIRED, False)
