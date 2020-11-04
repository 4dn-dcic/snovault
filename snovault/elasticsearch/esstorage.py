import elasticsearch.exceptions
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search, Q
from zope.interface import alsoProvides
from uuid import UUID
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER_QUEUE_MIRROR,
    INDEXER,
    ICachedItem,
)
from .indexer_utils import get_namespaced_index, namespace_index_from_health, find_uuids_for_indexing
from .create_mapping import SEARCH_MAX
from ..storage import register_storage
from ..util import CachedField
from dcicutils import es_utils, ff_utils
import structlog

log = structlog.getLogger(__name__)


def includeme(config):
    registry = config.registry
    # IMPORTANT: update read storage on PickStorage created in storage.py
    # Without `register_storage` call, cannot read from ES in the portal
    read_storage = ElasticSearchStorage(registry)
    register_storage(registry, read_override=read_storage)


class CachedModel(object):
    """
    Model used by resources returned from ElasticSearchStorage.
    Leverages cached_views.ICachedItem to create different item views
    that use this model.
    Analogous to storage.Resource, the model used for database resources
    """
    used_datastore = 'elasticsearch'

    def __init__(self, source):
        """
        Takes a dictionary document `source`
        """
        self.source = source

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
    def unique_keys(self):
        return self.source['unique_keys']

    @property
    def links(self):
        return self.source['links']

    @property
    def uuid(self):
        """
        Return UUID object to be consistent with Resource.uuid
        """
        return UUID(self.source['uuid'])

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
    Corresponds to storage.PickStorage.read and is analagous to
    storage.RDBStorage, which is used when working with DB models
    """
    def __init__(self, registry):
        self.registry = registry
        self.es = registry[ELASTIC_SEARCH]
        self.index = get_namespaced_index(registry, '*')
        self.mirror = self.registry.settings.get('mirror_health', None) is not None
        self.mirror_client = None
        # XXX: cache elastic search mappings here subject to a TTL
        # Use this field in search so you don't have to get mappings on every search
        self.mappings = CachedField('mappings',
                                    lambda: self.es.indices.get_mapping(index=self.index))

    def _one(self, search):
        # execute search and return a model if there is one hit
        hits = search.execute()
        if len(hits) != 1:
            return None
        model = CachedModel(hits[0].to_dict())
        return model

    @staticmethod
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

    def get_by_uuid(self, uuid):
        """
        This calls a search, and index/doc_type does not need to be provided
        Returns a CachedModel built with the es hit, or None if it is not found

        Args:
            uuid (str): uuid of the item to find

        Returns:
            CachedModel of the hit or None
        """
        search = Search(using=self.es, index=self.index)
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
              to set the CachedModel from these results.
              Right now, I'm not doing it because get_by_uuid_direct
              is possibly called very often during indexing.

        Args:
            uuid (str): uuid of the item to GET
            item_type (str): item_type of the item to GET

        Returns:
            dict: the Elasticsearch document if found, else None
        """
        index_name = get_namespaced_index(self.registry, item_type)
        try:
            res = self.es.get(index=index_name, doc_type=item_type, id=uuid,
                              _source=True, realtime=True, ignore=404)
        except elasticsearch.exceptions.NotFoundError:
            res = None
        return res

    def get_by_json(self, key, value, item_type, default=None):
        """
        Perform a search with an given key and value.
        Returns CachedModel if found, otherwise None
        """
        # find the term with the specific type
        term = 'embedded.' + key + '.raw'
        index = get_namespaced_index(self.registry, item_type)
        search = Search(using=self.es, index=index)
        search = search.filter('term', **{term: value})
        search = search.filter('type', value=item_type)
        return self._one(search)

    def get_by_unique_key(self, unique_key, name, item_type=None):
        """
        Perform a search against unique keys with given unique_key (field) and
        name (value).
        Returns CachedModel if found, otherwise None
        """
        term = 'unique_keys.' + unique_key
        index = self.index
        if item_type:
            index = get_namespaced_index(self.registry, item_type)
        # had to use ** kw notation because of variable in field name
        search = Search(using=self.es, index=index)
        search = search.filter('term', **{term: name})
        search = search.extra(version=True)
        return self._one(search)

    def get_rev_links(self, model, rel, *item_types):
        """
        Get the reverse links given a item model and a rev link name (rel), as
        well as the applicable item types of the reverse links.
        Perform a search and return a list of uuids of the resulting documents
        """
        search = Search(using=self.es, index=self.index)
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

    @staticmethod
    def _delete_from_es(es, rid, index_name, item_type):
        """
        Helper method that deletes an item from Elasticsearch, returning a boolean success value.
        Success means the given rid did not exist or no longer exists in ES.

        :param es: es client to use
        :param rid: resource id to purge
        :param index_name: index_name to purge rid from
        :param item_type: type of rid
        :returns: True if rid does not exist in ES, False if it does (AT THIS TIME)
        """
        try:
            es.delete(id=rid, index=index_name, doc_type=item_type)
        except elasticsearch.exceptions.NotFoundError:
            # Case: Not yet indexed
            log.error('PURGE: Could not find %s in ElasticSearch. Continuing.' % rid)
        except Exception as exc:
            log.error('PURGE: Cannot delete %s in ElasticSearch. Error: %s. NOT proceeding with deletion.'
                      % (item_type, str(exc)))
            return False  # only return False here as this is the only scenario where we want to STOP the purge process
        else:
            log.info('PURGE: successfully deleted %s in ElasticSearch' % rid)
        return True

    def _get_cached_mirror_health(self, mirror_env):

        cached_mirror_health = self.registry.settings['mirror_health']
        if 'error' not in cached_mirror_health:
            return cached_mirror_health

        mirror_env = self.registry.settings['mirror.env.name']
        mirror_health_now = ff_utils.get_health_page(ff_env=mirror_env)
        if 'error' not in mirror_health_now:
            return mirror_health_now

        raise RuntimeError('PURGE: Could not resolve mirror health on retry with error: %s' % mirror_health_now)

    def _assure_mirror_client(self, es_mirror_server_and_port):
        if not self.mirror:
            raise RuntimeError("Attempt to call ._assure_mirror_client() when there is no self.mirror.")

        use_aws_auth = self.registry.settings.get('elasticsearch.aws_auth')

        # make sure use_aws_auth is bool
        if not isinstance(use_aws_auth, bool):
            use_aws_auth = True if use_aws_auth == 'true' else False

        if not self.mirror_client:  # only recompute if we've never done this before
            self.mirror_client = es_utils.create_es_client(es_mirror_server_and_port, use_aws_auth=use_aws_auth)

    def _purge_uuid_from_mirror_es(self, rid, item_type):
        """
        Helper method for purge_uuid that purges rid from the mirror on ESStorage

        :param rid: resource id to purge
        :param item_type: type of rid
        :returns: result of 'delete_from_es'
        :raises: RuntimeError if we are unable to get the mirror health page
        """

        if not self.mirror:
            raise RuntimeError("Attempt to call ._purge_uuid_from_mirror_es() when there is no self.mirror.")

        mirror_env = self.registry.settings['mirror.env.name']
        log.info('PURGE: attempting to purge %s from mirror storage %s' % (rid, mirror_env))

        try:
            mirror_health = self._get_cached_mirror_health(mirror_env)
        except RuntimeError:
            log.error("PURGE: Tried to purge %s from mirror storage but couldn't get health page. Is staging up?" % rid)
            raise

        self._assure_mirror_client(es_mirror_server_and_port=mirror_health['elasticsearch'])

        mirror_index_name = namespace_index_from_health(health=mirror_health, index=item_type)
        return self._delete_from_es(es=self.mirror_client, rid=rid, index_name=mirror_index_name, item_type=item_type)

    def _purge_uuid_from_primary_es(self, rid, item_type, max_sid=None):

        index_name = get_namespaced_index(config=self.registry, index=item_type)
        if not self._delete_from_es(es=self.es, rid=rid, index_name=index_name, item_type=item_type):
            return False

        # queue related items for reindexing if deletion was successful
        self.registry[INDEXER].find_and_queue_secondary_items({rid}, set(), sid=max_sid)

        return True

    def purge_uuid(self, rid, item_type, max_sid=None):
        """
        Purge a uuid from the write storage (Elasticsearch)
        If the indexer has an ElasticSearch mirror environment, also attempt to remove the uuid from that mirror.

        Returns True if the deletion was successful or the item was not present, and False otherwise.
        """
        if not self._purge_uuid_from_primary_es(rid=rid, item_type=item_type, max_sid=max_sid):
            return False

        # if configured, delete the item from the mirrored ES as well.
        # If that is successful, we were successful, otherwise not.
        if self.mirror:
            return self._purge_uuid_from_mirror_es(rid=rid, item_type=item_type)
        else:
            # We don't usually need over and over in logs for cgap, where it's normal for there to be no mirror.
            log.debug('PURGE: Did not find a mirror env. Continuing.')
            return True

    def __iter__(self, *item_types):
        """
        Return a generator that yields string uuids of all documents matching
        given item types (through kwargs). Use all item types if none provided
        """
        query = {'query': {
            'bool': {
                'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
            }
        }}
        for hit in scan(self.es, index=self.index, query=query):
            yield hit.get('uuid', hit.get('_id'))

    def __len__(self, *item_types):
        """
        Return an integer count of the number of documents resulting for a
        search across all given item types (through kwargs). Use all item types
        if none provided
        """
        query = {'query': {
            'bool': {
                'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
            }
        }}
        result = self.es.count(index=self.index, body=query)
        return result['count']

    def find_uuids_linked_to_item(self, rid):
        """
        Given a resource id (uuid), find all items in ES that linkTo that item.
        Returns some extra information about the fields/links that are present
        """
        linked_info = []
        # we only care about linkTos the item and not reverse links here
        # we also do not care about invalidation scope
        uuids_linking_to_item, _ = find_uuids_for_indexing(self.registry, set([rid]))
        # remove the item itself from the list
        uuids_linking_to_item = uuids_linking_to_item - set([rid])
        if len(uuids_linking_to_item) > 0:
            # Return list of { '@id', 'display_title', 'uuid' } in 'comment'
            # property of HTTPException response to assist with any manual unlinking.
            for linking_uuid in uuids_linking_to_item:
                linking_dict = self.get_by_uuid(linking_uuid).source.get('embedded')
                linking_property = self.find_linking_property(linking_dict, rid)
                linked_info.append({
                    '@id' : linking_dict.get('@id', linking_dict['uuid']),
                    'display_title' : linking_dict.get('display_title', linking_dict['uuid']),
                    'uuid' : linking_uuid,
                    'field' : linking_property or "Not Embedded"
                })
        return linked_info

    def update(self, model, properties, sheets, unique_keys, links):
        """
        Update the ES contents for an ElasticSearch-based item.
        `model` is database model (storage.Resource), which is needed for
        getting correct sid/max_sid.
        This function gets existing ES document for the given item and updates
        the fields contained in `document` below. If there is no existing item,
        create an ES document with the minimal information.
        This will create a discrepancy between the
        new attributes and the indexed ones, like `properties` vs `embedded`.
        However, this is consistent with the database approach of indexing
        and the document will be updated after it is indexed
        """
        # links in ES use "~" instead of "."
        es_links = {}
        for key, val in links.items():
            es_links['~'.join(key.split('.'))] = val

        document = {
            'item_type': model.item_type,
            'uuid': str(model.uuid),
            'max_sid': model.max_sid,
            'sid': model.sid,
            'properties': properties,
            'propsheets': {},
            'unique_keys': unique_keys,
            'links': es_links
        }
        if sheets is not None:
            document['propsheets'] = sheets

        # get existing document if it exists
        existing_doc = self.get_by_uuid(document['uuid'])
        if existing_doc is not None:
            existing_doc.source.update(document)
            document = existing_doc.source

        index_name = get_namespaced_index(self.registry, document['item_type'])
        # use `refresh='waitfor'` so that the ES model is immediately available
        self.es.index(
            index=index_name, doc_type=document['item_type'], body=document,
            id=document['uuid'], version=document['sid'],
            version_type='external_gte', refresh='wait_for'
        )
