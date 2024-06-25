from webtest import TestApp

from ..routes import ENDPOINT, MODULE, REQUEST_METHODS, ROUTES, VIEWS


def test_routes(testapp: TestApp) -> None:
    """Test display of routes for the application."""
    result = testapp.get(ENDPOINT).json
    routes = result.get(ROUTES)
    views = result.get(VIEWS)
    assert routes
    assert views
    for route_info in routes.values():
        assert MODULE in route_info
        assert REQUEST_METHODS in route_info
    for item_views in views.values():
        for view_name, view_info in item_views.items():
            assert view_name
            assert MODULE in view_info
            assert REQUEST_METHODS in view_info
