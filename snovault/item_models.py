from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Union

import structlog
from pyramid.request import Request

from dcicutils.item_model_utils import (
    JsonObject,
    PortalItem as PortalItemWithoutRequests,
)


logger = structlog.getLogger(__name__)


@dataclass(frozen=True)
class PortalItem(PortalItemWithoutRequests):
    request: Optional[Request] = field(default=None, hash=False)

    def get_request(self) -> Union[Request, None]:
        return self.request

    @classmethod
    def from_properties(
        cls,
        properties: JsonObject,
        fetch_links=False,
        auth=None,
        request=None,
        **kwargs: Any,
    ) -> PortalItem:
        return cls(properties, fetch_links=fetch_links, auth=auth, request=request)

    @classmethod
    def from_identifier_and_existing_item(
        cls, identifier: str, existing_item: PortalItem, **kwargs: Any
    ) -> PortalItem:
        fetch_links = existing_item.should_fetch_links()
        auth = existing_item.get_auth()
        request = existing_item.get_request()
        if auth:
            return cls.from_identifier_and_auth(
                identifier, auth, fetch_links=fetch_links, **kwargs
            )
        if request:
            return cls.from_identifier_and_request(
                identifier, request, fetch_links=fetch_links, **kwargs
            )
        raise RuntimeError(
            "Unable to fetch given identifier without auth key or request"
        )

    @classmethod
    def from_properties_and_existing_item(
        cls, properties: JsonObject, existing_item: PortalItem, **kwargs: Any
    ) -> PortalItem:
        fetch_links = existing_item.should_fetch_links()
        auth = existing_item.get_auth()
        request = existing_item.get_request()
        return cls.from_properties(
            properties, fetch_links=fetch_links, auth=auth, request=request, **kwargs
        )

    @classmethod
    def from_identifier_and_request(
        cls, identifier: str, request: Request, fetch_links: bool, **kwargs: Any
    ) -> PortalItem:
        properties = cls._get_item_via_request(identifier, request)
        return cls.from_properties(
            properties, request=request, fetch_links=fetch_links, **kwargs
        )

    @classmethod
    def _get_item_via_request(cls, identifier: str, request: Request) -> JsonObject:
        try:
            result = request.embed(identifier, "@@object")
        except Exception as e:
            logger.exception(f"Unable to fetch identifer {identifier}: {e}")
            result = {}
        return result
