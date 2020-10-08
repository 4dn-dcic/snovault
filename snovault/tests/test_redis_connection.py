import pytest
import time
import json
from ..redis.redis_connection import RedisBase


class TestRedisBase:
    """ Uses redisdb, function scope fixture that automatically destroys DB on every run
        NOTE: when in production, use pass
    """

    def test_redis_simple(self, redisdb):
        """ Tests getting/setting simple string keys. """
        rd = RedisBase(redisdb)
        assert rd._set('hello', 'world')
        assert rd._get('hello') == 'world'
        assert rd._get('world') is None

    def test_redis_hset_hgetall(self, redisdb):
        """ Builds a simple object and tests using it. """
        rd = RedisBase(redisdb)
        my_key = 'foobar'
        assert rd._hset(my_key, 'foo', 'bar')
        assert b'foo' in rd._hgetall(my_key)
        rd._delete(my_key)
        assert rd._hgetall(my_key) == {}

    def test_redis_hset_hgetall_complex(self, redisdb):
        """ Builds a complex object resembling our structure """
        rd = RedisBase(redisdb)
        my_key_meta = 'snovault:items:uuid:meta'
        obj = {
            'this is': 'an object',
            'with multiple': 'fields'
        }

        def build_item_metadata():
            rd._hset(my_key_meta, 'dirty', 0)
            rd._hset(my_key_meta, 'item_type', 'Sample')
            rd._hset(my_key_meta, 'properties', json.dumps(obj))
        build_item_metadata()

        res = rd._hgetall(my_key_meta)
        assert res[b'dirty'] == b'0'
        assert res[b'item_type'] == b'Sample'
        assert json.loads(res[b'properties']) == obj
