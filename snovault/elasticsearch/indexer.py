import copy
import datetime
import json
import time
from timeit import default_timer as timer

import structlog
from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    TransportError,
)
from pyramid.view import view_config
from urllib3.exceptions import ReadTimeoutError

from ..interfaces import (
    DBSESSION,
    STORAGE
)
from .indexer_utils import get_namespaced_index, find_uuids_for_indexing, filter_invalidation_scope
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
    INDEXER_QUEUE
)
from ..embed import MissingIndexItemException
from ..util import debug_log, dictionary_lookup


log = structlog.getLogger(__name__)


def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)


# really simple exception to know when the sid check fails
class SidException(Exception):
    pass


def check_sid(sid, max_sid):
    """
    Simple function to compare a given sid to given max_sid.
    Raise an Exception if malformed or lesser max_sid

    Args:
        sid (int): query sid
        max_sid (int): maximum sid to compare to

    Raises:
        ValueError: if sid or max_sid are not valid
        SidException: if sid in request is greater than max sid
    """
    try:
        sid = int(sid)
        max_sid = int(max_sid)
    except ValueError:
        raise ValueError('sid (%s) and max sid (%s) must be integers.'
                         % (sid, max_sid))
    if max_sid < sid:
        raise SidException('Query sid (%s) is greater than max sid (%s).'
                           % (sid, max_sid))


@view_config(route_name='index', request_method='POST', permission="index")
@debug_log
def index(context, request):
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)  # if True, make a record in es
    dry_run = request.json.get('dry_run', False)  # if True, do not actually index
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    namespace_star = get_namespaced_index(request, '*')
    namespaced_index = get_namespaced_index(request, 'indexing')

    if not dry_run:
        index_start_time = datetime.datetime.now()
        index_start_str = index_start_time.isoformat()

        # create indexing record, with _id equal to starting timestamp
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
        indexing_counter = [0]
        # actually index
        # try to ensure ES is reasonably up to date
        es.indices.refresh(index=namespace_star)

        # NOTE: the refresh interval is left as default because it doesn't seem
        # to help performance much.
        # However, disabling it is okay, since check_es_and_cache_linked_sids
        # uses a GET, which will call a refresh if needed
        # Enabling the line below adds ~1.5 second overhead before and after indexing
        # es.indices.put_settings(index='_all', body={'index' : {'refresh_interval': '-1'}})

        # do the indexing!
        indexing_record['errors'] = indexer.update_objects(request, indexing_counter)

        # get some final info for the record
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
                es.index(index=namespaced_index, doc_type='indexing', body=indexing_record, id=index_start_str)
                es.index(index=namespaced_index, doc_type='indexing', body=indexing_record, id='latest_indexing')
            except:
                indexing_record['indexing_status'] = 'errored'
                error_messages = copy.deepcopy(indexing_record['errors'])
                del indexing_record['errors']
                es.index(index=namespaced_index, doc_type='indexing', body=indexing_record, id=index_start_str)
                es.index(index=namespaced_index, doc_type='indexing', body=indexing_record, id='latest_indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error', **item)
                        item['error_message'] = "Error occured during indexing, check the logs"

        # this will make documents in all lucene buffers available to search
        es.indices.refresh(index=namespace_star)
        # resets the refresh_interval to the default value (must reset if disabled earlier)
        # es.indices.put_settings(index='_all', body={'index' : {'refresh_interval': '1s'}})
    return indexing_record


class Indexer(object):
    def __init__(self, registry):
        self.registry = registry
        self.es = registry[ELASTIC_SEARCH]
        self.queue = registry[INDEXER_QUEUE]

    def update_objects(self, request, counter):
        """
        Top level routing between `Indexer.update_objects_sync` (synchronous)
        and `Indexer.update_objects_queue` (asynchronous, usually used).
        Also sets isolation level for the DB connection
        """
        session = request.registry[DBSESSION]()
        connection = session.connection()
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')

        # indexing is either run with sync uuids passed through the request
        # (which is synchronous) OR uuids from the queue
        sync_uuids = request.json.get('uuids', None)

        # actually index
        if sync_uuids:
            errors = self.update_objects_sync(request, sync_uuids, counter)
        else:
            errors, _ = self.update_objects_queue(request, counter)
        return errors

    def get_messages_from_queue(self):
        """
        Simple helper method. Attempt to get items from deferred queue first,
        and if none are found, check primary and then secondary queues. Both use
        long polling. Returns list of messages received and the string target
        the the queue came from.
        """
        messages = None
        target_queue = None
        for try_queue in self.queue.queue_targets:
            messages = self.queue.receive_messages(target_queue=try_queue)
            if messages:
                target_queue = try_queue
                break
        return messages, target_queue

    def find_and_queue_secondary_items(self, source_uuids, rev_linked_uuids,
                                       sid=None, telemetry_id=None, diff=None):
        """
        Find all associated uuids of the given set of non-strict uuids using ES
        and queue them in the secondary queue. Associated uuids include uuids
        that linkTo or are rev_linked to a given item.
        Add rev_linked_uuids linking to source items found from @@indexing-view
        after finding secondary uuids (they are "strict")
        """
        # find_uuids_for_indexing() will return items linking to and items
        # rev_linking to this item currently in ES (find old rev_links)
        associated_uuids, invalidated_with_type = find_uuids_for_indexing(self.registry, source_uuids)

        # remove already indexed primary uuids used to find them
        secondary_uuids = associated_uuids - source_uuids

        # we are updating from an edit and have a corresponding diff
        if diff is not None:
            filter_invalidation_scope(self.registry, diff, invalidated_with_type, secondary_uuids)

        # update this with rev_links found from @@indexing-view (includes new rev_links)
        # AFTER invalidation scope filtering, since invalidation scope does not account for rev-links
        secondary_uuids |= rev_linked_uuids

        # items queued through this function are ALWAYS strict in secondary queue
        return self.queue.add_uuids(self.registry, list(secondary_uuids), strict=True,
                                    target_queue='secondary', sid=sid,
                                    telemetry_id=telemetry_id)

    def update_objects_queue(self, request, counter):
        """
        Used for asynchronous indexing with the indexer queues. Some notes:
        - Keep track of `max_sid` of the transaction scope to defer out-of-scope
          indexing to a new run.
        - Iterate through queue messages, calling `update_object` on each
        - Handle deleting and recycling messages to the queues on errors/defers
        - Handles getting messages by priority with `get_messages_from_queue`
        """
        errors = []
        # hold uuids that will be used to find secondary uuids
        non_strict_uuids = set()
        # hold the reverse-linked uuids that need to be invalidated
        rev_linked_uuids = set()
        to_delete = []  # hold messages that will be deleted
        to_defer = []  # hold messages once we need to restart the worker
        # max_sid does not change of the lifetime of request
        max_sid = request.registry[STORAGE].write.get_max_sid()
        deferred = False  # if true, we need to restart the worker
        messages, target_queue = self.get_messages_from_queue()
        while len(messages) > 0:
            for idx, msg in enumerate(messages):
                msg_body = json.loads(msg['Body'])

                # handle case where worker needs to restart
                # recycle additional messages (no effect on dlq count) and
                # delete old messages
                if deferred:
                    to_defer.append(msg_body)
                    to_delete.append(msg)
                    if len(to_delete) == self.queue.delete_batch_size:
                        self.queue.delete_messages(to_delete, target_queue=target_queue)
                        to_delete = []
                    continue

                # This rather than msg_body['uuid'] to get better error reporting
                # in case uuid is missing from dictionary, which probably happens
                # because the msg_body is some other kind of object entirely. -kmp 9-Feb-2020
                msg_uuid = dictionary_lookup(msg_body, 'uuid')
                msg_sid = msg_body['sid']
                msg_curr_time = msg_body['timestamp']
                msg_detail = msg_body.get('detail')
                msg_telemetry = msg_body.get('telemetry_id')
                msg_diff = msg_body.get('diff', None)

                # build the object and index into ES
                # if strict, do not add uuids rev_linking to item to queue
                if msg_body['strict'] is True:
                    error = self.update_object(request, msg_uuid,
                                               add_to_secondary=None,
                                               sid=msg_sid, max_sid=max_sid,
                                               curr_time=msg_curr_time,
                                               telemetry_id=msg_telemetry)
                else:
                    error = self.update_object(request, msg_uuid,
                                               add_to_secondary=rev_linked_uuids,
                                               sid=msg_sid, max_sid=max_sid,
                                               curr_time=msg_curr_time,
                                               telemetry_id=msg_telemetry)
                if error:
                    if error.get('error_message') == 'defer_resend':
                        # resend the message and delete original so that receive
                        # count is not affected. set `deferred` to restart worker
                        to_defer.append(msg_body)
                        to_delete.append(msg)
                        deferred = True
                    elif error.get('error_message') == 'defer_replace':
                        # replace the message with a VisibilityTimeout
                        # set `deferred` to restart worker
                        self.queue.replace_messages([msg], target_queue=target_queue, vis_timeout=180)
                        deferred = True
                    else:
                        # regular error, replace the message with a VisibilityTimeout
                        # could do something with error, like putting on elasticache
                        self.queue.replace_messages([msg], target_queue=target_queue, vis_timeout=180)
                        errors.append(error)
                else:
                    # Sucessfully processed! (i.e. indexed or discarded conflict)
                    # if non-strict, adding will queue associated items to secondary
                    if msg_body['strict'] is False:
                        non_strict_uuids.add(msg_uuid)
                    counter[0] += 1  # do not increment on error
                    to_delete.append(msg)

                # delete messages when we have the right number
                if len(to_delete) == self.queue.delete_batch_size:
                    self.queue.delete_messages(to_delete, target_queue=target_queue)
                    to_delete = []

                # CHANGE - this needs to happen PER MESSAGE now
                # add to secondary queue, if applicable
                # search for all items that linkTo the non-strict items or contain
                # a rev_link to to them
                if non_strict_uuids or rev_linked_uuids:
                    queued, failed = self.find_and_queue_secondary_items(non_strict_uuids,  # THIS IS NOW A SINGLE UUID
                                                                         rev_linked_uuids,
                                                                         msg_sid,
                                                                         msg_telemetry,
                                                                         diff=msg_diff)
                    if failed:
                        error_msg = 'Failure(s) queueing secondary uuids: %s' % str(failed)
                        log.error('INDEXER: ', error=error_msg)
                        errors.append({'error_message': error_msg})
                    non_strict_uuids = set()
                    rev_linked_uuids = set()

            # if we need to restart the worker, break out of while loop
            if deferred:
                if to_defer:
                    self.queue.send_messages(to_defer, target_queue=target_queue)
                break

            # obtain more messages, possibly from a different queue
            prev_target_queue = target_queue
            messages, target_queue = self.get_messages_from_queue()
            # if we have switched between primary and secondary queues, delete
            # outstanding messages using previous queue
            if prev_target_queue != target_queue and to_delete:
                self.queue.delete_messages(to_delete, target_queue=prev_target_queue)
                to_delete = []

        # we're done. delete any outstanding messages before returning
        if to_delete:
            self.queue.delete_messages(to_delete, target_queue=target_queue)
        return errors, deferred

    def update_objects_sync(self, request, sync_uuids, counter):
        """
        Used with sync uuids (simply loop through)
        sync_uuids is a list of string uuids. Use timestamp of index run
        all uuids behave here as strict == true

        NOTE: This method does NOT take care of invalidation of items linked to
        indexed items. It is assumed that ALL items you want to index, including
        linked items are passed through the `sync_uuids` parameter
        """
        errors = []
        for i, uuid in enumerate(sync_uuids):
            error = self.update_object(request, uuid)
            if error is not None:
                errors.append(error)
            else:
                counter[0] += 1  # don't increment counter on an error
        return errors

    def update_object(self, request, uuid, add_to_secondary=None, sid=None,
                      max_sid=None, curr_time=None, telemetry_id=None):
        """
        Actually index the uuid using the index-data view.
        add_to_secondary is a set that gets the rev_linked_to_me
        from the request.embed(/<uuid>/@@index-data)
        """
        # logging constant
        cat = 'index object'

        # timing stuff
        start = timer()
        if not curr_time:
            curr_time = datetime.datetime.utcnow().isoformat()  # utc

        # to add to each log message
        log.bind(item_uuid=uuid, sid=sid, uo_start_time=curr_time)
        cm_source = False
        if telemetry_id:
            log.bind(telemetry_id=telemetry_id)
            # see if this message was generated by create-mapping
            if telemetry_id.startswith('cm_run_'):
                cm_source = True

        index_data_query = '/%s/@@index-data' % uuid
        try:
            # check sid first against max_sid from `update_objects_queue`
            # Raises SidException if invalid
            if sid and max_sid:
                check_sid(sid, max_sid)

            # invoke subrequest to get contents to index
            result = request.embed(index_data_query, as_user='INDEXER')
            duration = timer() - start
            # add total duration to indexing_stats in document
            result['indexing_stats']['total_indexing_view'] = duration
            log.bind(collection=result.get('item_type'))
            # log.info("Time for index-data", duration=duration, cat="indexing view")
        except SidException as e:
            duration = timer() - start
            log.warning('Invalid max sid. Resending...', duration=duration, cat=cat)
            # causes the item to be deferred by restarting worker
            # item will be re-sent (won't affect receive count)
            return {'error_message': 'defer_resend'}
        except MissingIndexItemException:
            # cannot find item. This could be due to it being purged.
            # if message is from create mapping, simply skip.
            # otherwise replace message and item will possibly make it to DLQ
            duration = timer() - start
            if cm_source:
                log.error('MissingIndexItemException encountered on resource %s'
                          ' from create_mapping. Skipping...' % index_data_query,
                          duration=duration, cat=cat)
                return
            else:
                log.warning('MissingIndexItemException encountered on resource '
                            '%s. No sid found. Replacing...' % index_data_query,
                            duration=duration, cat=cat)
                return {'error_message': 'defer_replace'}
        except Exception as e:
            duration = timer() - start
            log.error('Error rendering @@index-data', duration=duration, exc_info=True, cat=cat)
            return {'error_message': repr(e), 'time': curr_time, 'uuid': str(uuid)}

        # add found uuids that rev_link this item to be put in the secondary queue
        # find_and_queue_secondary_items() serves to find rev_linking items that
        # are currently in ES; this will pick up new rev links as well
        if add_to_secondary is not None:
            add_to_secondary.update(result['rev_linked_to_me'])

        last_exc = None
        for backoff in [0, 1, 2]:
            time.sleep(backoff)
            try:
                namespaced_index = get_namespaced_index(request, result['item_type'])
                self.es.index(
                    index=namespaced_index, doc_type=result['item_type'], body=result,
                    id=str(uuid), version=result['sid'], version_type='external_gte',
                    request_timeout=30
                )
            except ConflictError:
                # sid of found document is greater than sid of indexed document
                # this may be somewhat common and is not harmful
                # do not return an error so item is removed from queue
                duration = timer() - start
                log.warning('Conflict indexing', sid=result['sid'], duration=duration, cat=cat)
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
                # success! Do not return an error so item is removed from queue
                duration = timer() - start
                log.info('Time to index', duration=duration, cat=cat)
                return
        # returning an error message means item did not index
        return {'error_message': last_exc, 'time': curr_time, 'uuid': str(uuid)}
