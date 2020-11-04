import pytest

from ..interfaces import TYPES
from ..schema_utils import load_schema
from .test_views import PARAMETERIZED_NAMES


@pytest.mark.parametrize('schema', PARAMETERIZED_NAMES)
def test_load_schema(schema):
    assert load_schema('snovault:test_schemas/%s' % (schema + '.json'))


def test_dependencies(testapp):
    collection_url = '/testing-dependencies/'
    testapp.post_json(collection_url, {'dep1': 'dep1', 'dep2': 'dep2'}, status=201)
    testapp.post_json(collection_url, {'dep1': 'dep1'}, status=422)
    testapp.post_json(collection_url, {'dep2': 'dep2'}, status=422)
    testapp.post_json(collection_url, {'dep1': 'dep1', 'dep2': 'disallowed'}, status=422)


def test_changelogs(testapp, registry):
    for typeinfo in registry[TYPES].by_item_type.values():
        changelog = typeinfo.schema.get('changelog')
        if changelog is not None:
            res = testapp.get(changelog)
            assert res.status_int == 200, changelog
            assert res.content_type == 'text/markdown'


def test_schemas_etag(testapp):
    etag = testapp.get('/profiles/', status=200).etag
    assert etag
    testapp.get('/profiles/', headers={'If-None-Match': etag}, status=304)
