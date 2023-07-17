from contextlib import contextmanager
from typing import Iterator, Optional, Union
from unittest import mock

import pytest
from dcicutils.item_model_utils import JsonObject
from dcicutils.testing_utils import patch_context
from pyramid.request import Request

from .. import item_models as item_models_module
from ..item_models import PortalItem


SOME_UUID = "uuid1234"
SOME_ITEM_PROPERTIES = {
    "uuid": SOME_UUID,
}
SOME_AUTH = {"key": "some_key", "secret": "some_secret"}


@contextmanager
def patch_from_identifier_and_auth(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem.from_identifier_and_auth, **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_from_identifier_and_request(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem.from_identifier_and_request, **kwargs
    ) as mock_item:
        yield mock_item


def mock_request(embed_exception: bool = False) -> mock.MagicMock:
    result = mock.create_autospec(Request, instance=True)
    embed_method = mock.MagicMock()
    if embed_exception:
        embed_method.side_effect = Exception
    result.embed = embed_method
    return result


def get_portal_item(
    auth: Optional[JsonObject] = SOME_AUTH,
    fetch_links: bool = False,
    request: Optional[Request] = None,
) -> PortalItem:
    return PortalItem({}, auth=auth, fetch_links=fetch_links, request=request)


class TestPortalItem:
    @pytest.mark.parametrize(
        "auth,request_param,exception_expected",
        [
            (None, None, True),
            (SOME_AUTH, None, False),
            (None, mock_request(), False),
            (SOME_AUTH, mock_request(), False),
        ],
    )
    def test_from_identifier_and_existing_item(
        self,
        auth: Union[JsonObject, None],
        request_param: Union[Request, None],
        exception_expected: bool,
    ) -> None:
        identifier = SOME_UUID
        fetch_links = True
        portal_item = get_portal_item(
            fetch_links=fetch_links, auth=auth, request=request_param
        )
        with patch_from_identifier_and_auth() as mock_from_identifier_and_auth:
            with patch_from_identifier_and_request() as mock_from_identifier_and_request:
                if exception_expected:
                    with pytest.raises(RuntimeError):
                        PortalItem.from_identifier_and_existing_item(
                            identifier, portal_item
                        )
                else:
                    result = PortalItem.from_identifier_and_existing_item(
                        identifier, portal_item
                    )
                    if auth:
                        assert result == mock_from_identifier_and_auth.return_value
                        mock_from_identifier_and_auth.assert_called_once_with(
                            identifier, auth, fetch_links=fetch_links
                        )
                        mock_from_identifier_and_request.assert_not_called()
                    elif request_param:
                        assert result == mock_from_identifier_and_request.return_value
                        mock_from_identifier_and_request.assert_called_once_with(
                            identifier, request_param, fetch_links=fetch_links
                        )
                        mock_from_identifier_and_auth.assert_not_called()

    @pytest.mark.parametrize("exception_expected", [True, False])
    def test_get_item_via_request(self, exception_expected: bool) -> None:
        identifier = SOME_UUID
        request = mock_request(embed_exception=exception_expected)
        result = PortalItem._get_item_via_request(identifier, request)
        request.embed.assert_called_once_with(identifier, "@@object")
        if exception_expected:
            assert result == {}
        else:
            assert result == request.embed.return_value
