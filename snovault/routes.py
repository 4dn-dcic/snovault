from dataclasses import dataclass
from typing import Any, Dict, List, Union

from pyramid.config import Configurator
from pyramid.interfaces import IIntrospectable
from pyramid.request import Request
from pyramid.security import Authenticated
from pyramid.view import view_config

from .resources import Item
from .util import debug_log


ROUTE_NAME = "routes"
ENDPOINT = f"/{ROUTE_NAME}"

MODULE = "module"
REQUEST_METHODS = "request_methods"
ROUTES = "routes"
VIEWS = "views"


def includeme(config: Configurator) -> None:
    config.add_route(ROUTE_NAME, ENDPOINT)
    config.scan(__name__)


@view_config(
    route_name=ROUTE_NAME, request_method="GET", effective_principals=Authenticated
)
@debug_log
def routes(context, request: Request) -> Dict[str, Any]:
    """Provide all routes and item views for the application."""
    routes = get_routes(request)
    route_views = get_route_views(request)
    item_views = get_item_views(request)
    formatted_routes = format_routes(route_views, routes)
    formatted_views = format_views(item_views)
    return {
        ROUTES: formatted_routes,
        VIEWS: formatted_views,
    }


def get_routes(request: Request) -> List[IIntrospectable]:
    """Get all routes from the request registry."""
    routes = request.registry.introspector.get_category("routes")
    return [get_introspectable(route) for route in routes if get_introspectable(route)]


def get_route_views(request: Request) -> List[IIntrospectable]:
    """Get all routes from the request registry."""
    all_views = get_all_views(request)
    return filter_route_views(all_views)


def get_all_views(request: Request) -> List[IIntrospectable]:
    """Get all views from the request registry."""
    views = request.registry.introspector.get_category("views")
    return [get_introspectable(view) for view in views if get_introspectable(view)]


def get_introspectable(view: Dict[str, Any]) -> IIntrospectable:
    """Get the introspectable object from a view."""
    return view.get("introspectable")


def get_all_routes(request: Request) -> List[Dict[str, Any]]:
    """Get all routes from the request registry."""
    return request.registry.introspector.get_category("routes")


def filter_route_views(views: List[IIntrospectable]) -> List[IIntrospectable]:
    """Filter views to only include routes."""
    return [view for view in views if get_route_name(view)]


def get_route_name(view: IIntrospectable) -> str:
    """Get the route name from a view."""
    return view.get("route_name")


def get_item_views(request: Request) -> List[IIntrospectable]:
    """Get all views from the request registry."""
    all_views = get_all_views(request)
    return filter_item_views(all_views)


def filter_item_views(views: List[IIntrospectable]) -> List[IIntrospectable]:
    """Filter views to only include item views."""
    return [view for view in views if is_item_view(view)]


def is_item_view(view: IIntrospectable) -> bool:
    """Check if a view is for an Item."""
    context = get_context(view)
    if context:
        return is_item(context)
    return False


def get_context(view: IIntrospectable) -> type:
    """Get the context from a view."""
    return view.get("context")


def is_item(context: type) -> bool:
    """Check if a context is an Item."""
    return issubclass(context, Item)


def format_routes(
    route_views: List[IIntrospectable], routes: List[IIntrospectable]
) -> Dict[str, Dict[str, Any]]:
    """Format routes for display."""
    endpoints = match_routes(route_views, routes)
    return {
        get_endpoint_name(endpoint): {
            MODULE: get_module(endpoint),
            REQUEST_METHODS: get_request_methods(endpoint),
        }
        for endpoint in endpoints
    }


@dataclass(frozen=True)
class Endpoint:
    view: IIntrospectable
    route: IIntrospectable


def match_routes(
    route_views: List[IIntrospectable], routes: List[IIntrospectable]
) -> List[Endpoint]:
    """Match routes and views."""
    endpoints = []
    for route_view in route_views:
        for route in routes:
            if is_route_match(route_view, route):
                endpoints.append(Endpoint(view=route_view, route=route))
                break
    return endpoints


def is_route_match(route_view: IIntrospectable, route: IIntrospectable) -> bool:
    """Check if a route and view match."""
    return get_route_name(route_view) == get_name(route)


def get_name(route: IIntrospectable) -> str:
    """Get the name of a route."""
    return route.get("name")


def get_endpoint_name(endpoint: Endpoint) -> str:
    """Get the endpoint name of a route."""
    return get_pattern(endpoint.route)


def get_pattern(route: IIntrospectable) -> str:
    """Get the pattern of a route."""
    return route.get("pattern")


def get_module(item: Union[Endpoint, IIntrospectable]) -> str:
    """Get the module of a route."""
    if isinstance(item, Endpoint):
        view = item.view
    else:
        view = item
    callable_ = get_callable(view)
    if callable_:
        return callable_.__module__
    return ""


def get_callable(view: IIntrospectable) -> Any:
    """Get the callable object from a view."""
    return view.get("callable")


def get_request_methods(item: Union[Endpoint, IIntrospectable]) -> List[str]:
    """Get the request methods of a route."""
    if isinstance(item, Endpoint):
        view = item.view
    else:
        view = item
    return view.get("request_methods", []) or []


def format_views(views: List[IIntrospectable]) -> Dict[str, Dict[str, Any]]:
    """Format views for display."""
    views_per_item = get_views_per_item(views)
    result = {
        get_item_name(context): format_item_views(item_views)
        for context, item_views in views_per_item.items()
        if get_item_name(context)
    }
    return {key: value for key, value in result.items() if value}


def get_views_per_item(
    views: List[IIntrospectable],
) -> Dict[Item, List[IIntrospectable]]:
    """Get all views per item."""
    items = {}
    for view in views:
        context = get_context(view)
        if is_item(context):
            if context not in items:
                items[context] = []
            items[context].append(view)
    return items


def get_item_name(context: Item) -> str:
    """Get the name of an item."""
    return context.__name__


def format_item_views(views: List[IIntrospectable]) -> Dict[str, Dict[str, Any]]:
    """Format item views for display."""
    return {
        get_name(view): {
            MODULE: get_module(view),
            REQUEST_METHODS: get_request_methods(view),
        }
        for view in views
        if get_name(view)
    }
