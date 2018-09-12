from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    TransportError,
)
from ..indexing_views import SidException
from pyramid.view import view_config
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    INDEXER_QUEUE
)
from snovault import (
    DBSESSION,
)
from .indexer_utils import find_uuids_for_indexing
import datetime
import structlog
import time
import copy
import json
from timeit import default_timer as timer

log = structlog.getLogger(__name__)


def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)  # if True, make a record in es
    dry_run = request.json.get('dry_run', False)  # if True, do not actually index
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]

    # ensure we get the latest version of what is in the db as much as possible
    session = request.registry[DBSESSION]()
    connection = session.connection()
    connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')


    if not dry_run:
        index_start_time = datetime.datetime.now()
        index_start_str = index_start_time.isoformat()

        # create indexing record, with _id = indexing_start_time timestamp
        indexing_record = {
            'uuid': index_start_str,
            'indexing_status': 'started',
        }

        # get info on what actually is being indexed
        indexing_content = {
            'type': 'sync' if request.json.get('uuids') else 'queue',
        }
        if indexing_content['type'] == 'sync':
            indexing_content['sync_uuids'] = len(request.json.get('uuids'))
        else:
            indexing_content['initial_queue_status'] = indexer.queue.number_of_messages()
        indexing_record['indexing_content'] = indexing_content
        indexing_record['indexing_started'] = index_start_str
        indexing_counter = [0]  # do this so I can pass it as a reference
        # actually index
        # try to ensure ES is reasonably up to date
        es.indices.refresh(index='_all')
        # prepare_for_indexing

        indexing_record['errors'] = indexer.update_objects(request, indexing_counter)
        index_finish_time = datetime.datetime.now()
        indexing_record['indexing_finished'] = index_finish_time.isoformat()
        indexing_record['indexing_elapsed'] = str(index_finish_time - index_start_time)
        # update record with final queue snapshot
        if indexing_content['type'] == 'queue':
            indexing_content['finished_queue_status'] = indexer.queue.number_of_messages()
        indexing_record['indexing_count'] = indexing_counter[0]
        indexing_record['indexing_status'] = 'finished'

        # with the index listener running more frequently, we don't want to
        # store a ton of useless records. Only store queue records that have
        # errors or have non-zero indexing count
        if record and indexing_content['type'] == 'queue' and not indexing_record['errors']:
            record = indexing_record['indexing_count'] > 0

        if record:
            try:
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id=index_start_str)
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id='latest_indexing')
            except:
                indexing_record['indexing_status'] = 'errored'
                error_messages = copy.deepcopy(indexing_record['errors'])
                del indexing_record['errors']
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id=index_start_str)
                es.index(index='indexing', doc_type='indexing', body=indexing_record, id='latest_indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error', **item)
                        item['error_message'] = "Error occured during indexing, check the logs"
    # this will make documents in all lucene buffers available to search
    es.indices.refresh(index='_all')
    return indexing_record


class Indexer(object):
    def __init__(self, registry):
        self.registry = registry
        self.es = registry[ELASTIC_SEARCH]
        self.queue = registry[INDEXER_QUEUE]

    def update_objects(self, request, counter=None):
        """
        Top level update routing
        """
        # indexing is either run with sync uuids passed through the request
        # (which is synchronous) OR uuids from the queue
        sync_uuids = request.json.get('uuids', None)
        # actually index
        # TODO: these provides large speed increases... need to test with live data more to see
        # if it produces correct resutls
        # self.es.indices.put_settings(index='_all', body={'index' : {'refresh_interval': '-1'}})
        if sync_uuids:
            errors = self.update_objects_sync(request, sync_uuids, counter)
        else:
            errors = self.update_objects_queue(request, counter)
        # resets the refresh_interval to the default value
        self.es.indices.put_settings(index='_all', body={'index' : {'refresh_interval': None}})

    def get_messages_from_queue(self, skip_deferred=False):
        """
        Simple helper method. Attempt to get items from deferred queue first,
        and if none are found, check primary and then secondary queues. Both use
        long polling. Returns list of messages received and the string target
        the the queue came from.
        If skip_deferred, don't check that queue.
        """
        try_order = ['primary', 'secondary'] if skip_deferred else ['deferred', 'primary', 'secondary']
        messages = None
        target_queue = None
        for try_queue in try_order:
            # SPECIAL CASE: if we are looking at secondary but have items in
            # deferred, exit so that we can get a new transaction
            if skip_deferred and try_queue == 'secondary':
                deferred_waiting = self.queue.number_of_messages().get('deferred_waiting')
                if deferred_waiting and deferred_waiting > 0:
                    break
            messages = self.queue.receive_messages(target_queue=try_queue)
            if messages:
                target_queue = try_queue
                break
        return messages, target_queue


    def find_and_queue_secondary_items(self, source_uuids, rev_linked_uuids, telemetry_id=None):
        """
        Find all associated uuids of the given set of  non-strict uuids using ES
        and queue them in the secondary queue. Associated uuids include uuids
        that linkTo or are rev_linked to a given item.
        Add rev_linked_uuids linking to source items found from @@indexing-view
        after finding secondary uuids (they are "strict")
        """
        # find_uuids_for_indexing() will return items linking to and items
        # rev_linking to this item currently in ES (find old rev_links)
        associated_uuids = find_uuids_for_indexing(self.registry, source_uuids, log)
        # update this with rev_links found from @@indexing-view (includes new rev_links)
        associated_uuids |= rev_linked_uuids
        # remove already indexed primary uuids used to find them
        secondary_uuids = list(associated_uuids - source_uuids)
        # items queued through this function are ALWAYS strict in secondary queue
        return self.queue.add_uuids(self.registry, secondary_uuids, strict=True,
                                    target_queue='secondary', telemetry_id=telemetry_id)


    def update_objects_queue(self, request, counter):
        """
        Used with the queue
        """
        errors = []
        # hold uuids that will be used to find secondary uuids
        non_strict_uuids = set()
        # hold the reverse-linked uuids that need to be invalidated
        rev_linked_uuids = set()
        to_delete = []  # hold messages that will be deleted
        # only check deferred queue on the first run, since there shouldn't
        # be much in there at any given point
        messages, target_queue = self.get_messages_from_queue(skip_deferred=False)
        while len(messages) > 0:
            for idx, msg in enumerate(messages):
                # get all the details
                msg_body = json.loads(msg['Body'])
                msg_uuid= msg_body['uuid']
                msg_sid = msg_body['sid']
                msg_curr_time = msg_body['timestamp']
                msg_detail = msg_body.get('detail')
                msg_telemetry = msg_body.get('telemetry_id')

                # check to see if we are using the same txn that caused a deferral
                if target_queue == 'deferred' and msg_detail == str(request.tm.get()):
                    # re-create a new message so we don't affect retry count (dlq)
                    self.queue.send_messages([msg_body], target_queue=target_queue)
                    to_delete.append(msg)
                    continue
                if msg_body['strict'] is False:
                    non_strict_uuids.add(msg_uuid)
                # build the object and index into ES
                # if strict==True, do not add uuids rev_linking to item to queue
                if msg_body['strict'] is True:
                    error = self.update_object(request, msg_uuid, None,
                                               sid=msg_sid, curr_time=msg_curr_time,
                                               target_queue=target_queue,
                                               telemetry_id=msg_telemetry)
                else:
                    error = self.update_object(request, msg_uuid, rev_linked_uuids,
                                               sid=msg_sid, curr_time=msg_curr_time,
                                               target_queue=target_queue,
                                               telemetry_id=msg_telemetry)
                if error:
                    if error.get('error_message') == 'deferred_retry':
                        # send this to the deferred queue
                        msg_body['detail'] = error['txn_str']
                        self.queue.send_messages([msg_body], target_queue='deferred')
                        # delete the old message
                        to_delete.append(msg)
                    else:
                        # on a regular error, replace the message back in the queue
                        # could do something with error, like putting on elasticache
                        # set VisibilityTimeout high so that other items can process
                        self.queue.replace_messages([msg], target_queue=target_queue, vis_timeout=180)
                        errors.append(error)
                else:
                    if counter: counter[0] += 1  # do not increment on error
                    to_delete.append(msg)
                # delete messages when we have the right number
                if len(to_delete) == self.queue.delete_batch_size:
                    self.queue.delete_messages(to_delete, target_queue=target_queue)
                    to_delete = []
            # add to secondary queue, if applicable
            # search for all items that linkTo the non-strict items or contain
            # a rev_link to to them
            # reset uuid tracking (non_strict_uuids and rev_linked_uuids) after
            if non_strict_uuids or rev_linked_uuids:
                queued, failed = self.find_and_queue_secondary_items(non_strict_uuids,
                                                                     rev_linked_uuids,
                                                                     msg_telemetry)
                if failed:
                    error_msg = 'Failure(s) queueing secondary uuids: %s' % str(failed)
                    log.error('INDEXER: ', error=error_msg)
                    errors.append({'error_message': error_msg})
                non_strict_uuids = set()
                rev_linked_uuids = set()
            prev_target_queue = target_queue
            messages, target_queue = self.get_messages_from_queue(skip_deferred=True)
            # if we have switched between primary and secondary queues, delete
            # outstanding messages using previous queue
            if prev_target_queue != target_queue and to_delete:
                self.queue.delete_messages(to_delete, target_queue=prev_target_queue)
                to_delete = []
        # we're done. delete any outstanding messages
        if to_delete:
            self.queue.delete_messages(to_delete, target_queue=target_queue)
        return errors


    def update_objects_sync(self, request, sync_uuids, counter):
        """
        Used with sync uuids (simply loop through)
        sync_uuids is a list of string uuids. Use timestamp of index run
        all uuids behave here as strict == true
        """
        errors = []
        for i, uuid in enumerate(sync_uuids):
            # add_to_secondary = None here since invalidation is not used
            error = self.update_object(request, uuid, None)
            if error is not None:  # don't increment counter on an error
                errors.append(error)
            elif counter:
                counter[0] += 1
        return errors


    def update_object(self, request, uuid, add_to_secondary=None, sid=None,
                      curr_time=None, target_queue=None, telemetry_id=None):
        """
        Actually index the uuid using the index-data view.
        add_to_secondary is a set that gets the uuids_rev_linked_to_me
        from the request.embed(/<uuid>/@@index-data)
        target_queue is an optional string queue name:
            'primary', 'secondary', or 'deferred'
        """

        # logging constant
        cat = 'update object'

        #timing stuff
        start = timer()
        if not curr_time:
            curr_time = datetime.datetime.utcnow().isoformat()  # utc

        # to add to each log message
        log.bind(embed_uuid=uuid, sid=sid, uo_start_time=curr_time)
        if telemetry_id:
            log.bind(telemetry_id=telemetry_id)

        # check the sid with a less intensive view than @@index-data
        if sid:
            index_data_query = '/%s/@@index-data?sid=%s' % (uuid, sid)
        else:
            index_data_query = '/%s/@@index-data' % uuid

        try:
            result = request.embed(index_data_query, as_user='INDEXER')
            duration = timer() - start
            log.bind(collection=result.get('item_type'))
            # log.info("time to embed", duration=duration, cat="embed time")
        except SidException as e:
            duration = timer() - start
            log.warning('Invalid sid found', duration=duration, cat=cat)
            # this will cause the item to be sent to the deferred queue
            return {'error_message': 'deferred_retry', 'txn_str': str(request.tm.get())}
        except KeyError as e:
            # only consider a KeyError deferrable if not already in deferred queue
            duration = timer() - start
            if target_queue != 'deferred':
                log.info('KeyError', duration=duration, cat=cat)
                # this will cause the item to be sent to the deferred queue
                return {'error_message': 'deferred_retry', 'txn_str': str(request.tm.get())}
            else:
                log.error('KeyError rendering @@index-data', duration=duration, exc_info=True, cat=cat)
                return {'error_message': repr(e), 'time': curr_time, 'uuid': str(uuid)}
        except Exception as e:
            duration = timer() - start
            log.error('Error rendering @@index-data', duration=duration, exc_info=True, cat=cat)
            return {'error_message': repr(e), 'time': curr_time, 'uuid': str(uuid)}
        # add found uuids that rev_link this item to be put in the secondary queue
        # find_and_queue_secondary_items() serves to find rev_linking items that
        # are currently in ES; this will pick up new rev links as well
        if add_to_secondary is not None:
            add_to_secondary.update(result['uuids_rev_linked_to_me'])

        last_exc = None
        for backoff in [0, 1, 2]:
            time.sleep(backoff)
            try:
                self.es.index(
                    index=result['item_type'], doc_type=result['item_type'], body=result,
                    id=str(uuid), version=result['sid'], version_type='external_gte',
                    request_timeout=30
                )
            except ConflictError:
                duration = timer() - start
                log.warning('Conflict indexing', sid=result['sid'], duration=duration, cat=cat)
                # this may be somewhat common and is not harmful
                # do not return an error so the item is removed from the queue
                return
            except (ConnectionError, ReadTimeoutError, TransportError) as e:
                duration = timer() - start
                log.warning('Retryable error indexing', error=str(e), duration=duration, cat=cat)
                last_exc = repr(e)
            except Exception as e:
                duration = timer() - start
                log.error('Error indexing', duration=duration, exc_info=True, cat=cat)
                last_exc = repr(e)
                break
            else:
                duration = timer() - start
                # log.info('update object success', duration=duration, cat=cat)
                return

        return {'error_message': last_exc, 'time': curr_time, 'uuid': str(uuid)}


    def shutdown(self):
        pass
