from dataclass import dataclass, field
from typing import Any, Optional, Union

import structlog
from pyramid.request import Request

from dcicutils.item_models import JsonObject, PortalItem as PortalItemWithoutRequests


logger = structlog.getLogger(__name__)


@dataclass(frozen=True)
class PortalItem(PortalItemWithoutRequests):

    request: Optional[Request] = field(default=None, hash=False)

    def get_request(self) -> Union[Request, None]:
        return self.request

    @classmethod
    def from_properties(
        cls, properties: JsonObject, embed_items=False, auth=None, request=None, **kwargs: Any
    ) -> "PortalItem":
        return cls(
            properties=properties, embed_items=embed_items, auth=auth, request=request
        )

    @classmethod
    def from_identifier_and_existing_item(
        cls, identifier: str, existing_item: "PortalItem", **kwargs: Any
    ) -> "PortalItem":
        embed_items = existing_item.do_embeds()
        auth = existing_item.get_auth()
        request = existing_item.get_request()
        if auth:
            return cls.from_identifier_and_auth(
                identifier, auth, embed_items=embed_items, **kwargs
            )
        if request:
            return cls.from_identifier_and_request(
                identifier, request, embed_items=embed_items, **kwargs
            )
        raise ValueError("Unable to create item from existing item")

    @classmethod
    def from_properties_and_existing_item(
        cls, properties: JsonObject, existing_item: "PortalItem", **kwargs: Any
    ) -> "PortalItem":
        embed_items = existing_item.do_embeds()
        auth = existing_item.get_auth()
        request = existing_item.get_request()
        return cls.from_properties(
            properties, embed_items=embed_items, auth=auth, request=request, **kwargs
        )

    @classmethod
    def from_identifier_and_request(
        cls, identifier: str, request: Request, embed_items: bool, **kwargs: Any
    ) -> "PortalItem":
        properties = cls._get_item_via_request(identifier, request)
        return cls.from_properties(
            properties=properties, request=request, embed_items=embed_items, **kwargs
        )

    @classmethod
    def _get_item_via_request(cls, identifier: str, request: Request) -> JsonObject:
        try:
            result = request.embed(identifier, "@@object")
        except Exception as e:
            logger.exception(f"Unable to embed identifer {identifier}: {e}")
            result = {}
        return result
