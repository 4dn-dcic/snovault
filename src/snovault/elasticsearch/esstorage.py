import elasticsearch.exceptions
from snovault.util import get_root_request
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search, Q
from pyramid.threadlocal import get_current_request
from pyramid.httpexceptions import (
    HTTPLocked
)
from zope.interface import alsoProvides
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER_QUEUE_MIRROR,
    INDEXER,
    ICachedItem,
)
from ..storage import RDBStorage
from .indexer_utils import find_uuids_for_indexing
from dcicutils import es_utils
import structlog

log = structlog.getLogger(__name__)

SEARCH_MAX = 99999  # OutOfMemoryError if too high. Previously: (2 ** 31) - 1

def includeme(config):
    from snovault import STORAGE
    registry = config.registry
    es = registry[ELASTIC_SEARCH]
    # ES 5 change: 'snovault' index removed, search among '_all' instead
    es_index = '_all'
    wrapped_storage = registry[STORAGE]
    registry[STORAGE] = PickStorage(ElasticSearchStorage(es, es_index), wrapped_storage, registry)


def find_linking_property(our_dict, value_to_find):
    """
    Helper function used in PickStorage.find_uuids_linked_to_item
    """
    def find_it(d, parent_key=None):
        if isinstance(d, list):
            for idx, v in enumerate(d):
                if isinstance(v, dict) or isinstance(v, list):
                    found = find_it(v, parent_key)
                    if found:
                        return (parent_key if parent_key else '') + '[' + str(idx) + '].' + found
                elif v == value_to_find:
                    return '[' + str(idx) + ']'
        elif isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict) or isinstance(v, list):
                    found = find_it(v, k)
                    if found:
                        return found
                elif v == value_to_find:
                    return k
        return None

    return find_it(our_dict)


class CachedModel(object):
    def __init__(self, hit):
        self.source = hit.to_dict()
        self.meta = hit.meta.to_dict()

    @property
    def item_type(self):
        return self.source['item_type']

    @property
    def properties(self):
        return self.source['properties']

    @property
    def propsheets(self):
        return self.source['propsheets']

    @property
    def uuid(self):
        return self.source['uuid']

    @property
    def sid(self):
        return self.source['sid']

    def used_for(self, item):
        alsoProvides(item, ICachedItem)


class PickStorage(object):
    def __init__(self, read, write, registry):
        self.read = read
        self.write = write
        self.registry = registry

    def storage(self):
        request = get_current_request()
        if request and request.datastore == 'elasticsearch':
            return self.read
        return self.write

    def get_by_uuid(self, uuid):
        storage = self.storage()
        model = storage.get_by_uuid(uuid)
        if storage is self.read:
            if model is None:
                return self.write.get_by_uuid(uuid)
        return model

    def get_by_unique_key(self, unique_key, name):
        storage = self.storage()
        model = storage.get_by_unique_key(unique_key, name)
        if storage is self.read:
            if model is None:
                return self.write.get_by_unique_key(unique_key, name)
        return model

    def get_by_json(self, key, value, item_type, default=None):
        storage = self.storage()
        model = storage.get_by_json(key, value, item_type)
        if storage is self.read:
            if model is None:
                return self.write.get_by_json(key, value, item_type)
        return model

    def find_uuids_linked_to_item(self, rid):
        """
        Given a resource id (rid), such as uuid, find all items in the DB
        that have a linkTo to that item.
        Returns some extra information about the fields/links that are
        present
        """
        linked_info = []
        # we only care about linkTos the item and not reverse links here
        uuids_linking_to_item = find_uuids_for_indexing(self.registry, set([rid]))
        # remove the item itself from the list
        uuids_linking_to_item = uuids_linking_to_item - set([rid])
        if len(uuids_linking_to_item) > 0:
            # Return list of { '@id', 'display_title', 'uuid' } in 'comment'
            # property of HTTPException response to assist with any manual unlinking.
            for linking_uuid in uuids_linking_to_item:
                linking_dict = self.read.get_by_uuid(linking_uuid).source.get('embedded')
                linking_property = find_linking_property(linking_dict, rid)
                linked_info.append({
                    '@id' : linking_dict.get('@id', linking_dict['uuid']),
                    'display_title' : linking_dict.get('display_title', linking_dict['uuid']),
                    'uuid' : linking_uuid,
                    'field' : linking_property or "Not Embedded"
                })
        return linked_info

    def purge_uuid(self, rid, item_type=None):
        """
        Attempt to purge an item by given resource id (rid), completely
        removing it from ES and DB.
        """
        if not item_type: # ES deletion requires index & doc_type, which are both == item_type
            model = self.get_by_uuid(rid)
            item_type = model.item_type
        uuids_linking_to_item = self.find_uuids_linked_to_item(rid)
        if len(uuids_linking_to_item) > 0:
            raise HTTPLocked(detail="Cannot purge item as other items still link to it",
                             comment=uuids_linking_to_item)
        log.error('PURGE: purging %s' % rid)

        # delete the item from DB
        self.write.purge_uuid(rid)
        # delete the item from ES and also the mirrored ES if present
        self.read.purge_uuid(rid, item_type, self.registry)
        # queue related items for reindexing
        self.registry[INDEXER].find_and_queue_secondary_items(set([rid]), set())

    def get_rev_links(self, model, rel, *item_types):
        return self.storage().get_rev_links(model, rel, *item_types)

    def __iter__(self, *item_types):
        return self.storage().__iter__(*item_types)

    def __len__(self, *item_types):
        return self.storage().__len__(*item_types)

    def create(self, item_type, uuid):
        return self.write.create(item_type, uuid)

    def update(self, model, properties=None, sheets=None, unique_keys=None, links=None):
        return self.write.update(model, properties, sheets, unique_keys, links)


class ElasticSearchStorage(object):
    writeable = False

    def __init__(self, es, index):
        self.es = es
        self.index = index

    def _one(self, search):
        # execute search and return a model if there is one hit
        hits = search.execute()
        if len(hits) != 1:
            return None
        model = CachedModel(hits[0])
        return model

    def get_by_uuid(self, uuid):
        """
        This calls a search, and index/doc_type does not need to be provided
        Returns a CachedModel built with the es hit, or None if it is not found

        Args:
            uuid (str): uuid of the item to find

        Returns:
            CachedModel of the hit or None
        """
        search = Search(using=self.es)
        id_query = Q('ids', values=[str(uuid)])
        search = search.query(id_query)
        return self._one(search)

    def get_by_uuid_direct(self, uuid, item_type):
        """
        See if a document exists under the index/doc_type given by item_type.
        self.es.get calls a GET request, which will refresh the given
        document (and all other docs) in realtime, if necessary. Explicitly
        ignore 404 responses from elasticsearch to reduce logging.

        NOTE: this function DOES NOT use CachedModel, as it is used for direct
              querying of ES items during indexing only. It might be performant
              to set the CachedModel from these results; see commented code
              below. Right now, I'm not doing it because get_by_uuid_direct
              is possibly called very often during indexing.

        Args:
            uuid (str): uuid of the item to GET
            item_type (str): item_type of the item to GET

        Returns:
            The _source value from the document, if it exists
        """
        # use the CachedModel. Would need to change usage of get_by_uuid_direct
        # in snovault to res.source from res['_source']
        # try:
        #     res = self.es.get(index=item_type, doc_type=item_type, id=uuid,
        #                       _source=True, realtime=True, ignore=404)
        # except elasticsearch.exceptions.NotFoundError:
        #     model = None
        # else:
        #     model = CachedModel(res)
        # return model
        try:
            res = self.es.get(index=item_type, doc_type=item_type, id=uuid,
                              _source=True, realtime=True, ignore=404)
        except elasticsearch.exceptions.NotFoundError:
            res = None
        return res

    def get_by_json(self, key, value, item_type, default=None):
        # find the term with the specific type
        term = 'embedded.' + key + '.raw'
        search = Search(using=self.es)
        search = search.filter('term', **{term: value})
        search = search.filter('type', value=item_type)
        return self._one(search)

    def get_by_unique_key(self, unique_key, name):
        term = 'unique_keys.' + unique_key
        # had to use ** kw notation because of variable in field name
        search = Search(using=self.es)
        search = search.filter('term', **{term: name})
        search = search.extra(version=True)
        return self._one(search)

    def get_rev_links(self, model, rel, *item_types):
        search = Search(using=self.es)
        search = search.extra(size=SEARCH_MAX)
        # rel links use '~' instead of '.' due to ES field restraints
        proc_rel = rel.replace('.', '~')
        # had to use ** kw notation because of variable in field name
        search = search.filter('term', **{'links.' + proc_rel: str(model.uuid)})
        if item_types:
            search = search.filter('terms', item_type=item_types)
        hits = search.execute()
        return [hit.to_dict().get('uuid', hit.to_dict().get('_id')) for hit in hits]

    def get_sids_by_uuids(self, rids):
        """
        Currently not implemented for ES. Just return an empty dict

        Args:
            rids (list): list of string rids (uuids)

        Returns:
            dict keyed by rid with integer sid values
        """
        return {}

    def get_max_sid(self):
        """
        Currently not implemented for ES. Just return None
        """
        return None

    def purge_uuid(self, rid, item_type=None, registry=None):
        """
        Purge a uuid from the write storage (Elasticsearch)
        If there is a mirror environment set up for the indexer, also attempt
        to remove the uuid from the mirror Elasticsearch
        """
        if not item_type:
            model = self.get_by_uuid(rid)
            item_type = model.item_type
        try:
            self.es.delete(id=rid, index=item_type, doc_type=item_type)
        except elasticsearch.exceptions.NotFoundError:
            # Case: Not yet indexed
            log.error('PURGE: Couldn\'t find %s in ElasticSearch. Continuing.' % rid)
        except Exception as exc:
            log.error('PURGE: Cannot delete %s in ElasticSearch. Error: %s Continuing.' % (item_type, str(exc)))
        if not registry:
            log.error('PURGE: Registry not available for ESStorage purge_uuid')
            return
        # if configured, delete the item from the mirrored ES as well
        if registry.settings.get('mirror.env.es'):
            mirror_es = registry.settings['mirror.env.es']
            use_aws_auth = registry.settings.get('elasticsearch.aws_auth')
            # make sure use_aws_auth is bool
            if not isinstance(use_aws_auth, bool):
                use_aws_auth = True if use_aws_auth == 'true' else False
            mirror_client = es_utils.create_es_client(mirror_es, use_aws_auth=use_aws_auth)
            try:
                mirror_client.delete(id=rid, index=item_type, doc_type=item_type)
            except elasticsearch.exceptions.NotFoundError:
                # Case: Not yet indexed
                log.error('PURGE: Couldn\'t find %s in mirrored ElasticSearch (%s). Continuing.' % (rid, mirror_es))
            except Exception as exc:
                log.error('PURGE: Cannot delete %s in mirrored ElasticSearch (%s). Error: %s Continuing.' % (item_type, mirror_es, str(exc)))

    def __iter__(self, *item_types):
        query = {'query': {
            'bool': {
                'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
            }
        }}
        for hit in scan(self.es, query=query):
            yield hit.get('uuid', hit.get('_id'))

    def __len__(self, *item_types):
        query = {'query': {
            'bool': {
                'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
            }
        }}
        result = self.es.count(index=self.index, body=query)
        return result['count']
