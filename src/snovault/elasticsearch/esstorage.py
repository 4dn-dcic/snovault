import elasticsearch.exceptions
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search, Q
from pyramid.httpexceptions import HTTPLocked
from pyramid.settings import asbool
from zope.interface import alsoProvides
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER_QUEUE_MIRROR,
    INDEXER,
    ICachedItem,
)
from .indexer_utils import find_uuids_for_indexing
from .create_mapping import SEARCH_MAX
from dcicutils import es_utils
import structlog

log = structlog.getLogger(__name__)


def includeme(config):
    from snovault import STORAGE, DBSESSION
    from ..storage import register_storage
    registry = config.registry
    es = registry[ELASTIC_SEARCH]
    # ES 5 change: 'snovault' index removed, search among '_all' instead
    es_index = '_all'
    # update read storage on PickStorage created in storage.py
    read_storage = ElasticSearchStorage(es, es_index)
    register_storage(registry, read_override=read_storage)


def find_linking_property(our_dict, value_to_find):
    """
    Helper function used in ElasticSearchStorage.find_uuids_linked_to_item
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

    @property
    def max_sid(self):
        return self.source['max_sid']

    def used_for(self, item):
        alsoProvides(item, ICachedItem)


class ElasticSearchStorage(object):
    """
    Storage class used to interface with Elasticsearch.
    Corresponds to `PickStorage.read`
    """
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

    def purge_uuid(self, registry, rid, item_type=None, max_sid=None):
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
            log.error('PURGE: Could not find %s in ElasticSearch. Continuing.' % rid)
        except Exception as exc:
            log.error('PURGE: Cannot delete %s in ElasticSearch. Error: %s Continuing.' % (item_type, str(exc)))
        else:
            log.info('PURGE: successfully deleted %s in ElasticSearch' % rid)

        # queue related items for reindexing
        registry[INDEXER].find_and_queue_secondary_items(set([rid]), set(), sid=max_sid)
        # if configured, delete item from the mirrored ES
        if registry.settings.get('mirror.env.es'):
            mirror_es = registry.settings['mirror.env.es']
            use_aws_auth = asbool(registry.settings.get('elasticsearch.aws_auth'))
            mirror_client = es_utils.create_es_client(mirror_es, use_aws_auth=use_aws_auth)
            try:
                mirror_client.delete(id=rid, index=item_type, doc_type=item_type)
            except elasticsearch.exceptions.NotFoundError:
                # Case: Not yet indexed
                log.error('PURGE: Could not find %s in mirrored ElasticSearch (%s). Continuing.' % (rid, mirror_es))
            except Exception as exc:
                log.error('PURGE: Cannot delete %s in mirrored ElasticSearch (%s). Error: %s Continuing.' % (item_type, mirror_es, str(exc)))
            else:
                log.info('PURGE: sucessfully deleted %s in mirrored ElasticSearch (%s)'
                         % (item_type, mirror_es))

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

    def find_uuids_linked_to_item(self, registry, rid):
        """
        Given a registry and resource id (uuid), find all items in ES
        that have a linkTo to that item.
        Returns some extra information about the fields/links that are present
        """
        linked_info = []
        # we only care about linkTos the item and not reverse links here
        uuids_linking_to_item = find_uuids_for_indexing(registry, set([rid]))
        # remove the item itself from the list
        uuids_linking_to_item = uuids_linking_to_item - set([rid])
        if len(uuids_linking_to_item) > 0:
            # Return list of { '@id', 'display_title', 'uuid' } in 'comment'
            # property of HTTPException response to assist with any manual unlinking.
            for linking_uuid in uuids_linking_to_item:
                linking_dict = self.get_by_uuid(linking_uuid).source.get('embedded')
                linking_property = find_linking_property(linking_dict, rid)
                linked_info.append({
                    '@id' : linking_dict.get('@id', linking_dict['uuid']),
                    'display_title' : linking_dict.get('display_title', linking_dict['uuid']),
                    'uuid' : linking_uuid,
                    'field' : linking_property or "Not Embedded"
                })
        return linked_info
