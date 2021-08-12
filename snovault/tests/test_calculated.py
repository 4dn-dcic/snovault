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
        },
        'nested2': [
            {
                'key': 'foo',
                'value': 'bar'
            },
            {
                'key': 'apple',
                'value': 'orange'
            },
            {
                'key': 'blah',
                'value': 'blah'
            }
        ]
    }


def test_calculated_build_object(testapp, basic_calculated_item):
    """ Tests that we can run calculated properties """
    [res] = testapp.post_json(CONNECTION_URL, basic_calculated_item, status=201).json['@graph']
    assert 'combination' in res
    for k in ['name', 'foo', 'bar']:
        assert k in res['combination']
    nested = res['nested']
    for k, v in zip(['key', 'value', 'keyvalue'], ['foo', 'bar', 'foobar']):
        assert nested[k] == v
    nested2 = res['nested2']
    for entry in nested2:
        if entry['key'] == 'foo':
            assert entry['keyvalue'] == 'foobar'
        elif entry['key'] == 'apple':
            assert entry['keyvalue'] == 'appleorange'
        elif entry['key'] == 'blah':
            assert entry['keyvalue'] == 'blahblah'
