import secrets
import datetime
import structlog
from .interfaces import REDIS


log = structlog.getLogger(__name__)


def includeme(config):
    registry = config.registry
    redis_storage = RedisConnection(registry)
    # TODO: register this storage as appropriate
    registry[REDIS] = redis_storage


class RedisException(Exception):
    pass


class RedisBase(object):
    """ This class contains low level methods meant to implement useful Redis APIs. The idea is these functions
        are used to implement the methods needed in RedisConnection.

        TODO: implement more APIs
    """

    def __init__(self, redis_handle):
        self.redis = redis_handle

    def _set(self, key, value):
        """ Sets the given key to the given value. """
        return self.redis.set(key, value)

    def _get(self, key):
        """ Gets the given key from Redis. """
        val = self.redis.get(key)
        if val is not None:
            val = val.decode('utf-8')
        return val

    def _delete(self, key):
        """ Deletes the given key from Redis. """
        self.redis.delete(key)

    def _hgetall(self, key):
        """ Gets all values of the given hash. """
        return self.redis.hgetall(key)

    def _hset(self, key, field, value):
        """ Sets a single field on a hash key. """
        return self.redis.hset(key, field, value)

    def _hset_multiple(self, key, items):
        """ Sets all k,v pairs in items on hash key. """
        return self.redis.hset(key, mapping=items)

    def _dbsize(self):
        """ Returns number of keys in redis """
        return self.redis.dbsize()


def make_session_token(n_bytes=32):
    """ Uses the secrets module to create a cryptographically secure and URL safe string """
    return secrets.token_urlsafe(n_bytes)


class RedisSessionToken:
    """
    Model used by Redis to store session tokens
    Keystore structure:
        <env_namespace>:session:email
            -> Redis hset containing the associated JWT, session token and expiration time (3 hours)
    """
    JWT = b'jwt'
    SESSION = b'session_token'
    EXPIRATION = b'expiration'

    @staticmethod
    def _build_session_expiration():
        """ Builds a session expiration date 3 hours after generation """
        return str(datetime.datetime.utcnow() + datetime.timedelta(hours=3))

    def _build_session_hset(self, jwt, token, expiration=None):
        """ Builds Redis hset record for the session token """
        return {
            self.JWT: jwt,
            self.SESSION: token,
            self.EXPIRATION: self._build_session_expiration() if not expiration else expiration
        }

    def __init__(self, *, namespace, email, jwt, token=None, expiration=None):
        """ Creates a Redis Session object, storing a hash of the JWT into Redis and returning this
            value as the session token.
        """
        self.redis_key = f'{namespace}:session:{email}'
        self.email = email
        self.jwt = jwt
        if token:
            self.session_token = token
        else:
            self.session_token = make_session_token()
        self.session_hset = self._build_session_hset(self.jwt, self.session_token, expiration=expiration)

    def __eq__(self, other):
        """ Evaluates equality of two session objects based on the value of the session hset """
        return self.session_hset == other.session_hset

    @classmethod
    def from_redis(cls, *, redis_handler, namespace, email):
        """ Builds a RedisSessionToken from an existing record """
        redis_key = f'{namespace}:session:{email}'
        redis_token = redis_handler._hgetall(redis_key)
        return cls(namespace=namespace, email=email, jwt=redis_token[cls.JWT].decode('utf-8'),
                   token=redis_token[cls.SESSION].decode('utf-8'),
                   expiration=redis_token[cls.EXPIRATION].decode('utf-8'))

    def store_session_token(self, *, redis_handler: RedisBase) -> bool:
        """ Stores the created session token object as an hset in Redis """
        try:
            redis_handler._hset_multiple(self.redis_key, self.session_hset)
        except Exception as e:
            log.error(str(e))
            raise RedisException()
        return True

    def validate_session_token(self, *, redis_handler: RedisBase, token) -> bool:
        """ Validates the given session token against that stored in redis """
        redis_token = redis_handler._hgetall(self.redis_key)
        token_is_valid = (redis_token[self.SESSION].decode('utf-8') == token)
        timestamp_is_valid = (datetime.datetime.fromisoformat(redis_token[self.EXPIRATION].decode('utf-8')) > datetime.datetime.utcnow())
        return token_is_valid and timestamp_is_valid

    def update_session_token(self, *, redis_handler: RedisBase, jwt) -> bool:
        """ Refreshes the session token, jwt (if different) and expiration stored in Redis """
        self.session_token = make_session_token()
        self.jwt = jwt
        self.session_hset = self._build_session_hset(jwt, self.session_token)
        return self.store_session_token(redis_handler=redis_handler)

    def delete_session_token(self, *, redis_handler) -> bool:
        """ Deletes the session token from redis, effectively logging out """
        return redis_handler._delete(self.redis_key)


class RedisModel(object):
    """
    Model used by resources returned from RedisStorage
    Analogous to esstorage.CachedModel, storage.Resource

    Redis Keystore Structure:
        * 2 tiered namespace, first by environment then customizable within the environment.
        * 2 sub-embedded objects, each with a dirty bit
            * 'meta' holds item metadata along with the raw view (properties)
            * 'embedded' holds the latest embedded view of this item

        <env_namespace>:<namespace>:<uuid>:meta|embedded -> Redis HASH object

        NOTE: all objects are serialized into JSON

            meta : {
                "dirty": 0|1,
                "item_type": <type>,  # Sample for example,
                "properties":  { ... item properties ... }
                "propsheets": { ... item propsheets ... },
                "unique_keys": { ... item unique_keys ... },
                "links": { ... item links ... },
                "uuid": <uuid>,
                "sid": <sid>,
                "max_sid": <max_sid>
            }

            OR

            embedded : {
                "dirty" : 0|1,
                "view" : { ... view of item ... }
            }


    Note that this allows for future use in different <namespace>'s.

    NOT USED AT THIS TIME.
    """
    used_datastore = 'redis'

    def __init__(self, meta):
        """ Takes dictionary document 'meta' """
        self.meta = meta

    @property
    def item_type(self):  # TODO
        raise NotImplementedError

    @property
    def properties(self):  # TODO
        raise NotImplementedError

    @property
    def propsheets(self):  # TODO
        raise NotImplementedError

    @property
    def unique_keys(self):  # TODO
        raise NotImplementedError

    @property
    def links(self):  # TODO
        raise NotImplementedError

    @property
    def uuid(self):  # TODO
        """
        Return UUID object to be consistent with Resource.uuid
        """
        raise NotImplementedError

    @property
    def sid(self):  # TODO
        raise NotImplementedError

    @property
    def max_sid(self):  # TODO
        raise NotImplementedError


class RedisConnection(object):
    """ This class is meant to be the intermediary between the RedisBase class and the PickStorage class.
        Requires helper methods that take advantage of the above redis APIs.

        NOT USED AT THIS TIME.
    """

    def __init__(self, registry, default=None):
        self.registry = registry
        self.namespace = registry.settings.get('indexer.namespace', '')  # use indexer namespace as Redis 1st key
        self.redis = RedisBase(registry[REDIS])  # raw connection API
        self.__default = 'default' if default is None else default  # default key namespace, name is configurable

    def get_by_uuid(self, uuid: str, namespace=None):
        """ Looks up the given uuid in the Redis keystore. If no namespace is specified, the default namespace is used.

            :param uuid: uuid of the item to find
            :param namespace: key namespace to use
            :returns: the requested view
        """
        pass

    def get_by_unique_key(self, unique_key: str, name: str):
        """ Looks up the given item by unique_key : name

            :param unique_key: name of field
            :param name: value of field
            :returns: the requested view
        """
        pass

    def purge_uuid(self, rid, item_type):
        """ Purges the given uuid from Redis

            :param rid: resource id
            :param item_type: type of item
        """
        pass

    def __len__(self):
        """ Count # of documents in Redis. """
        pass

    def update(self, model, properties, sheets, unique_keys, links):
        """ Update method. """
        pass
