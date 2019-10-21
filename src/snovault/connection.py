from past.builtins import basestring
from pyramid.decorator import reify
from uuid import UUID
from .cache import ManagerLRUCache
from .interfaces import (
    CONNECTION,
    STORAGE,
    TYPES,
)


def includeme(config):
    registry = config.registry
    registry[CONNECTION] = Connection(registry)


class UnknownItemTypeError(Exception):
    pass


class Connection(object):
    '''
    Intermediates between the storage and the rest of the system.
    Storage class should be storage.PickStorage, which is used to interface
    between different storage types (presumably RDS and ES)

    Many methods of the class take `datastore` parameter. This can be used
    to force which storage is used. Should be set to 'elasticsearch' to force
    usage of `PickStorage.read` or 'database' for usage of `PickStorage.write`.
    See `PickStorage.storage` for more info
    '''
    def __init__(self, registry):
        self.registry = registry
        self.item_cache = ManagerLRUCache('snovault.connection.item_cache', 1000)
        self.unique_key_cache = ManagerLRUCache('snovault.connection.key_cache', 1000)
        embed_cache_capacity = int(registry.settings.get('embed_cache.capacity', 2000))
        self.embed_cache = ManagerLRUCache('snovault.connection.embed_cache', embed_cache_capacity)

    @reify
    def storage(self):
        return self.registry[STORAGE]

    @reify
    def types(self):
        return self.registry[TYPES]

    def get_by_json(self, key, value, item_type, default=None, datastore=None):
        model = self.storage.get_by_json(key, value, item_type, default, datastore)

        if model is None:
            return default

        try:
            Item = self.types.by_item_type[model.item_type].factory
        except KeyError:
            raise UnknownItemTypeError(model.item_type)

        item = Item(self.registry, model)
        model.used_for(item)
        return item


    def get_by_uuid(self, uuid, default=None, datastore=None):
        if isinstance(uuid, basestring):
            # some times we get @id type things here
            uuid = uuid.strip("/").split("/")[-1]
            try:
                uuid = UUID(uuid)
            except ValueError:
                return default
        elif not isinstance(uuid, UUID):
            raise TypeError(uuid)

        uuid = str(uuid)
        cached = self.item_cache.get(uuid)
        if cached is not None:
            return cached

        model = self.storage.get_by_uuid(uuid, datastore)
        if model is None:
            return default

        try:
            Item = self.types.by_item_type[model.item_type].factory
        except KeyError:
            raise UnknownItemTypeError(model.item_type)

        item = Item(self.registry, model)
        model.used_for(item)
        self.item_cache[uuid] = item
        return item

    def get_by_unique_key(self, unique_key, name, default=None, datastore=None):
        pkey = (unique_key, name)

        cached = self.unique_key_cache.get(pkey)
        if cached is not None:
            return self.get_by_uuid(cached, datastore)

        model = self.storage.get_by_unique_key(unique_key, name, datastore)
        if model is None:
            return default

        uuid = model.uuid
        self.unique_key_cache[pkey] = uuid
        cached = self.item_cache.get(uuid)
        if cached is not None:
            return cached

        try:
            Item = self.types.by_item_type[model.item_type].factory
        except KeyError:
            raise UnknownItemTypeError(model.item_type)

        item = Item(self.registry, model)
        model.used_for(item)
        self.item_cache[uuid] = item
        return item

    def get_rev_links(self, model, rel, *types, datastore=None):
        item_types = [self.types[t].item_type for t in types]
        return self.storage.get_rev_links(model, rel, *item_types, datastore=datastore)

    def __iter__(self, *types, datastore=None):
        if not types:
            item_types = self.types.by_item_type.keys()
        else:
            item_types = [self.types[t].item_type for t in types]
        for uuid in self.storage.__iter__(*item_types, datastore=datastore):
            yield uuid

    def __len__(self, *types, datastore=None):
        if not types:
            item_types = self.types.by_item_type.keys()
        else:
            item_types = [self.types[t].item_type for t in types]
        return self.storage.__len__(*item_types, datastore=datastore)

    def __getitem__(self, uuid, datastore=None):
        item = self.get_by_uuid(uuid, datastore)
        if item is None:
            raise KeyError(uuid)
        return item

    def create(self, type_, uuid, datastore=None):
        ti = self.types[type_]
        return self.storage.create(ti.item_type, uuid, datastore)

    def update(self, model, properties, sheets=None, unique_keys=None, links=None, datastore=None):
        self.storage.update(model, properties, sheets, unique_keys, links, datastore)
