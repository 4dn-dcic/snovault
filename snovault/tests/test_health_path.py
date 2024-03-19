import pytest


def test_health_page_basic(testapp):
    """ Tests that we can reach the health page and that the customization works """
    health = testapp.get('/health').json
    assert health['display_title'] == 'ENCODED Portal Status and Foursight Monitoring'
    assert health['database'] == 'postgres'
