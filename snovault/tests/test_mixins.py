import pytest


TEST_COLLECTION = '/testing_mixins'


@pytest.fixture
def test_object_with_mixin():
    return {
        'name': 'i_should_have_a_mixin'
    }


def test_mixins_basic(authenticated_testapp, test_object_with_mixin):
    resp = authenticated_testapp.post_json(TEST_COLLECTION, test_object_with_mixin, status=201)
    assert 'status' in resp.json['@graph'][0]
