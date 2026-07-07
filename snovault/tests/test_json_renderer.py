"""
Unit tests for snovault.json_renderer -- the default JSON renderer's type
adapters (UUID/set/frozenset/datetime), the BinaryFromJSON wrapper, and
JSONResult.serializer. Every response body in the system flows through this
module; it previously had no direct test. No services required.
"""
import datetime
import json
import uuid

import pytest

from ..json_renderer import (
    json_renderer,
    uuid_adapter,
    listy_adapter,
    datetime_adapter,
    BinaryFromJSON,
    JSONResult,
)


pytestmark = [pytest.mark.unit]


class TestAdapters:

    def test_uuid_adapter_stringifies(self):
        value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        assert uuid_adapter(value, None) == '12345678-1234-5678-1234-567812345678'

    def test_listy_adapter_converts_sets(self):
        assert listy_adapter({'a'}, None) == ['a']
        assert listy_adapter(frozenset(['b']), None) == ['b']

    def test_datetime_adapter_isoformats(self):
        value = datetime.datetime(2026, 7, 6, 12, 30, 15)
        assert datetime_adapter(value, None) == '2026-07-06T12:30:15'


class TestJsonRendererDumps:

    def test_dumps_handles_registered_types_without_a_request(self):
        # dumps() resolves adapters via get_current_request(); it must work
        # outside any request (returns None) since indexers call it that way.
        value = {
            'id': uuid.UUID('12345678-1234-5678-1234-567812345678'),
            'when': datetime.datetime(2026, 7, 6, 12, 30, 15),
            'tags': frozenset(['only']),
        }
        parsed = json.loads(json_renderer.dumps(value))
        assert parsed == {
            'id': '12345678-1234-5678-1234-567812345678',
            'when': '2026-07-06T12:30:15',
            'tags': ['only'],
        }

    def test_dumps_returns_text(self):
        assert isinstance(json_renderer.dumps({'a': 1}), str)


class TestJSONResult:

    def test_serializer_yields_utf8_bytes(self):
        result = JSONResult.serializer({'a': 1})
        assert isinstance(result, BinaryFromJSON)
        assert b''.join(result) == b'{"a": 1}'

    def test_binary_from_json_preserves_chunking(self):
        wrapped = BinaryFromJSON(['ab', 'cd'])
        assert len(wrapped) == 2
        assert list(wrapped) == [b'ab', b'cd']
