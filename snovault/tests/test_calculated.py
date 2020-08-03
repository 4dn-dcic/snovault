import pytest


CONNECTION_URL = '/TestingCalculatedProperties'


@pytest.fixture
def basic_calculated_item():
    return {
        'name': 'cat',
        'foo': 'dog',
        'bar': 'mouse',
        'nested': {
            'key': 'foo',
            'value': 'bar'
        }
    }


def test_calculated_build_object(testapp, basic_calculated_item):
    """ Tests that we can run calculated properties """
    [res] = testapp.post_json(CONNECTION_URL, basic_calculated_item, status=201).json['@graph']
    assert 'combination' in res
    for k in ['name', 'foo', 'bar']:
        assert k in res['combination']
    nested = res['nested']
    for k in ['key', 'value', 'keyvalue']:  # XXX: get this to work
        assert k in nested
