import structlog
from .interfaces import REDIS
from dcicutils.redis_utils import RedisBase, create_redis_client


log = structlog.getLogger(__name__)


def includeme(config):
    registry = config.registry
    try:
        registry[REDIS] = RedisBase(create_redis_client(url=registry.settings['redis.server']))
    except Exception as e:
        log.error(f"Cannot create RedisBase object: {registry.settings.get('redis.server')}")
        log.error(str(e))
        registry[REDIS] = None


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
