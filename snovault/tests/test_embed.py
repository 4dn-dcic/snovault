from typing import Any, Dict, Optional, Union
from unittest.mock import create_autospec, Mock

import pytest
from pyramid.request import Request

from ..embed import _set_subrequest_attributes


DEFAULT_PARENT_REQUEST_ATTRIBUTES = {
    "_indexing_view": "foo",
    "_aggregate_for": "bar",
    "_aggregated_items": "something",
    "_sid_cache": "else",
}
DEFAULT_SUBREQUEST_ATTRIBUTES = {"environ": {"HTTP_COOKIE": "foobar", "fu": "bur"}}
DEFAULT_PROPAGATED_ATTRIBUTES = {
    "override_renderer": "null_renderer",
    "_stats": {},
    **DEFAULT_PARENT_REQUEST_ATTRIBUTES,
}


def _make_mock_request(attributes_to_set: Dict[str, Any]) -> Mock:
    mock_request = create_autospec(Request, instance=True)
    for attribute_name, attribute_value in attributes_to_set.items():
        setattr(mock_request, attribute_name, attribute_value)
    return mock_request


def _make_mock_parent_request(
    attributes_to_set: Optional[Dict[str, Any]] = None
) -> Mock:
    if attributes_to_set is None:
        attributes_to_set = {}
    attributes_to_set.update(DEFAULT_PARENT_REQUEST_ATTRIBUTES)
    return _make_mock_request(attributes_to_set)


def _make_mock_subrequest() -> Mock:
    return _make_mock_request(DEFAULT_SUBREQUEST_ATTRIBUTES)


@pytest.mark.parametrize(
    "subrequest,parent_request,as_user,expected_attributes",
    [
        (
            _make_mock_subrequest(),
            _make_mock_parent_request(),
            True,
            {**DEFAULT_PROPAGATED_ATTRIBUTES, **DEFAULT_SUBREQUEST_ATTRIBUTES},
        ),
        (
            _make_mock_subrequest(),
            _make_mock_parent_request({"_stats": "some_stats"}),
            True,
            {
                **DEFAULT_PROPAGATED_ATTRIBUTES,
                **DEFAULT_SUBREQUEST_ATTRIBUTES,
                "_stats": "some_stats",
            },
        ),
        (
            _make_mock_subrequest(),
            _make_mock_parent_request(),
            False,
            {
                **DEFAULT_PROPAGATED_ATTRIBUTES,
                **DEFAULT_SUBREQUEST_ATTRIBUTES,
                "environ": {"fu": "bur"},
                "remote_user": False,
            },
        ),
    ],
)
def test_set_subrequest_attributes(
    subrequest: Mock,
    parent_request: Mock,
    as_user: Union[bool, str],
    expected_attributes: Dict[str, Any],
) -> None:
    _set_subrequest_attributes(subrequest, parent_request, as_user=as_user)
    for attribute_name, attribute_value in expected_attributes.items():
        assert getattr(subrequest, attribute_name) == attribute_value
