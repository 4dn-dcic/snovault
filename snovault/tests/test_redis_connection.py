import pytest
import datetime
import json
from unittest import mock
from ..redis.redis_connection import RedisBase, RedisSessionToken


class TestRedisBase:
    """ Uses redisdb, function scope fixture that automatically destroys DB on every run """

    def test_redis_simple(self, redisdb):
        """ Tests getting/setting simple string keys. """
        rd = RedisBase(redisdb)
        assert rd._set('hello', 'world')
        assert rd._get('hello') == 'world'
        assert rd._get('world') is None
        assert rd._dbsize() == 1

    def test_redis_hset_hgetall(self, redisdb):
        """ Builds a simple object and tests using it. """
        rd = RedisBase(redisdb)
        my_key = 'foobar'
        assert rd._hset(my_key, 'foo', 'bar')
        assert b'foo' in rd._hgetall(my_key)
        assert rd._dbsize() == 1
        rd._delete(my_key)
        assert rd._hgetall(my_key) == {}

    def test_redis_hset_multiple(self, redisdb):
        """ Builds a record with multiple hset entries in a single call """
        rd = RedisBase(redisdb)
        my_key = 'foobar'
        n_set = rd._hset_multiple(my_key, {
            'foo': 'bar',
            'bar': 'foo'
        })
        assert n_set == 2
        assert b'foo' in rd._hgetall(my_key)
        assert b'bar' in rd._hgetall(my_key)

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


class TestRedisSession:
    """ Uses redisdb and Session abstraction on top of RedisBase to implement some APIs for managing sessions
        using Redis """
    NAMESPACE = 'snovault-unit-test'
    DUMMY_EMAIL = 'snovault@test.com'
    DUMMY_JWT = 'example'

    # normal method to fit the mock structure
    def mock_build_session_expiration(self):  # noQA
        """ Simulate an expired datetime when validating """
        return str(datetime.datetime.utcnow() - datetime.timedelta(minutes=1))

    def test_redis_session_basic(self, redisdb):
        """ Generate a session, validate it, test validation failure cases """
        rd = RedisBase(redisdb)
        session_token = RedisSessionToken(
            namespace=self.NAMESPACE,
            email=self.DUMMY_EMAIL,
            jwt=self.DUMMY_JWT
        )
        session_token.store_session_token(redis_handler=rd)
        # passing token just built should validate
        assert session_token.validate_session_token(redis_handler=rd,
                                                    token=session_token.session_token)
        # invalid token should fail
        assert not session_token.validate_session_token(redis_handler=rd,
                                                        token='blah')
        # update with a new token and expiration
        old_token = session_token.session_token
        session_token.update_session_token(redis_handler=rd, jwt=self.DUMMY_JWT)
        assert not session_token.validate_session_token(redis_handler=rd,
                                                        token=old_token)
        assert session_token.validate_session_token(redis_handler=rd,
                                                    token=session_token.session_token)

    def test_redis_session_expired_token(self, redisdb):
        """ Tests that when patching in a function that will generate an expired timestamp
            session token validation will fail.
        """
        rd = RedisBase(redisdb)
        with mock.patch.object(RedisSessionToken, '_build_session_expiration', self.mock_build_session_expiration):
            session_token = RedisSessionToken(
                namespace=self.NAMESPACE,
                email=self.DUMMY_EMAIL,
                jwt=self.DUMMY_JWT
            )
            session_token.store_session_token(redis_handler=rd)
            assert not session_token.validate_session_token(redis_handler=rd,
                                                            token=session_token.session_token)
        # update then should validate
        session_token.update_session_token(redis_handler=rd, jwt=self.DUMMY_JWT)
        assert session_token.validate_session_token(redis_handler=rd,
                                                    token=session_token.session_token)

    def test_redis_session_many_sessions(self, redisdb):
        """ Tests generating and pushing many session objects into Redis and checking
            that they do not validate against one another.
        """
        rd = RedisBase(redisdb)
        sessions = []
        emails = [f'snovault{n}@test.com' for n in range(5)]
        for email in emails:
            session_token = RedisSessionToken(
                namespace=self.NAMESPACE,
                email=email,
                jwt=self.DUMMY_JWT
            )
            session_token.store_session_token(redis_handler=rd)
            sessions.append(session_token)
        assert rd._dbsize() == 5
        # check all sessions work
        tokens = []
        for session in sessions:
            assert session.validate_session_token(redis_handler=rd, token=session.session_token)
            tokens.append(session.session_token)
        # check tokens don't work with wrong session
        for session, token in zip(sessions, tokens[::-1]):
            if session.session_token != token:  # all but middle should fail
                assert not session.validate_session_token(redis_handler=rd, token=token)
            else:
                assert session.validate_session_token(redis_handler=rd, token=token)

    def test_redis_session_from_redis_equality(self, redisdb):
        """ Tests generating a session then grabbing that same session from Redis, assuring
            that they result in the same downstream session object.
        """
        rd = RedisBase(redisdb)
        session_token_local = RedisSessionToken(
            namespace=self.NAMESPACE,
            email=self.DUMMY_EMAIL,
            jwt=self.DUMMY_JWT
        )
        session_token_local.store_session_token(redis_handler=rd)
        session_token_remote = RedisSessionToken.from_redis(redis_handler=rd, namespace=self.NAMESPACE,
                                                            email=self.DUMMY_EMAIL)
        assert rd._dbsize() == 1
        assert session_token_remote == session_token_local
        session_token_remote.delete_session_token(redis_handler=rd)
        assert rd._dbsize() == 0
