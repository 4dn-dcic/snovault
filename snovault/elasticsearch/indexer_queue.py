"""
Class to manage the items for indexing
First round will use a standard SQS queue from AWS without Elasticache.
"""

import datetime
import json
import os
import socket
import time
from collections import OrderedDict

import boto3
import structlog
from dcicutils.env_utils import blue_green_mirror_env
from dcicutils.misc_utils import ignored, RateManager, LockoutManager
from pyramid.view import view_config

from .indexer_utils import get_uuids_for_types
from .interfaces import INDEXER_QUEUE, INDEXER_QUEUE_MIRROR
from ..util import debug_log

log = structlog.getLogger(__name__)


def includeme(config):
    config.add_route('queue_indexing', '/queue_indexing')
    config.add_route('indexing_status', '/indexing_status')
    config.add_route('dlq_to_primary', '/dlq_to_primary')
    env_name = config.registry.settings.get('env.name')
    sqs_url = os.environ.get('SQS_URL', None)
    config.registry[INDEXER_QUEUE] = QueueManager(config.registry, override_url=sqs_url)
    # INDEXER_QUEUE_MIRROR is used because blue and green share a DB
    mirror_env = blue_green_mirror_env(env_name) if env_name else None
    if mirror_env:
        mirror_queue = QueueManager(config.registry, mirror_env=mirror_env, override_url=sqs_url)
        if not mirror_queue.queue_url:
            log.error('INDEXING: Mirror queues %s are not available!' % mirror_queue.queue_name,
                      queue=mirror_queue.queue_name)
            raise Exception('INDEXING: Mirror queues %s are not available!' % mirror_queue.queue_name)
        config.registry[INDEXER_QUEUE_MIRROR] = mirror_queue
    else:
        config.registry[INDEXER_QUEUE_MIRROR] = None
    config.scan(__name__)


@view_config(route_name='queue_indexing', request_method='POST', permission="index")
@debug_log
def queue_indexing(context, request):
    """
    Endpoint to queue items for indexing. Takes a POST request with index
    priviliges which should contain either a list of uuids under "uuids" key
    or a list of collections under "collections" key of its body. Can also
    optionally take "strict" boolean and "target_queue" string.
    """
    ignored(context)
    req_uuids = request.json.get('uuids', None)
    req_collections = request.json.get('collections', None)
    # TODO: This variable is unused. Is it work-in-progress or something that should go away? -kmp 7-May-2020
    # queue_mode = None  # either queueing 'uuids' or 'collection'
    response = {
        'notification': 'Failure',
        'number_queued': 0,
        'detail': 'Nothing was queued. Make sure to past in a list of uuids in in "uuids" key'
                  ' OR list of collections in the "collections" key of request the POST request.'
    }
    telemetry_id = request.params.get('telemetry_id', None)

    if not req_uuids and not req_collections:
        return response
    if req_uuids and req_collections:
        response['detail'] = 'Nothing was queued. You cannot provide both uuids and a collection for queueing at once.'
        return response
    if req_uuids and not isinstance(req_uuids, list):
        response['detail'] = ('Nothing was queued. When queueing uuids,'
                              ' make to sure to put a list of string uuids in the POST request.')
        return response
    if req_collections and not isinstance(req_collections, list):
        response['detail'] = ('Nothing was queued. When queueing a collection,'
                              ' make sure to provide a list of string collection names in the POST request.')
        return response
    queue_indexer = request.registry[INDEXER_QUEUE]
    # strict mode means uuids should be indexed without finding associates
    strict = request.json.get('strict', False)
    # target queue can also be specified, e.g. 'primary', 'secondary'
    target = request.json.get('target_queue', 'primary')
    if req_uuids:
        # queue these as secondary
        queued, failed = queue_indexer.add_uuids(request.registry, req_uuids,
                                                 strict=strict, target_queue=target,
                                                 telemetry_id=telemetry_id)
        response['requested_uuids'] = req_uuids
    else:
        # queue these as secondary
        queued, failed = queue_indexer.add_collections(request.registry,
                                                       req_collections,
                                                       strict=strict,
                                                       target_queue=target,
                                                       telemetry_id=telemetry_id)
        response['requested_collections'] = req_collections
    response['notification'] = 'Success'
    response['number_queued'] = len(queued)
    response['detail'] = 'Successfuly queued items!'
    response['errors'] = failed
    response['strict'] = strict
    response['target_queue'] = target
    response['telemetry_id'] = telemetry_id
    return response


@view_config(route_name='indexing_status', request_method='GET')
@debug_log
def indexing_status(context, request):
    """
    Endpoint to check what is currently on the queue. Uses GET requests
    """
    ignored(context)
    queue_indexer = request.registry[INDEXER_QUEUE]
    response = {}
    try:
        numbers = queue_indexer.number_of_messages()
    except Exception as e:
        response['detail'] = str(e)
        response['status'] = 'Failure'
    else:
        for queue in numbers:
            response[queue] = numbers[queue]
        response['display_title'] = 'Indexing Status'
        response['status'] = 'Success'
    return response


@view_config(route_name='dlq_to_primary', request_method='GET', permission='index')
@debug_log
def dlq_to_primary(context, request):
    """
    Endpoint to move all uuids on the DLQ to the primary queue
    """
    ignored(context)
    queue_indexer = request.registry[INDEXER_QUEUE]
    # What comes out of .receive_messages() looks like:
    # [{
    #     "Body": "{\"uuid\": \"some-uuid\", \"sid\": null, \"strict\": false,"
    #             " \"timestamp\": \"2020-05-08T15:06:13.229594\"}",
    #     "Attributes": {
    #         "ApproximateFirstReceiveTimestamp": "1588950373247",
    #         "SenderId": "AIDAIT2UOQQ...",
    #         "ApproximateReceiveCount": "1",
    #         "SentTimestamp": "1588950373237"
    #     },
    #     "ReceiptHandle": "tvsvyhlhyaukymgulgjmchpnedkllhmckmheclfnrafnboqiflfjdjrwnwzwgxgomhxznpvgysgnr..."
    #     "MD5OfBody": "3e0e82e521a522...",
    #     "MessageId": "ca0886a7-9f95-..."
    # }, ...]
    # NOTES by kmp 8-May-2020:
    #   - If .send_messages() is going to take care of packing the body JSON into a string, then really
    #     .receive_messages() should do the inverse so that functions like this don't have to know anything
    #     about that storage form.
    #   - .send_messages() adds an envelope/wrapper that contains a 'MessageBody' key (with the JSON payload
    #     in string form as its value), and other keywords that are header metadata.
    #        {'Id': ..., 'MessageBody': json.dumps(msg)}
    #     Later, though, this wrapped structure shows up looking like:
    #        {'MessageId': ..., 'Body': ...}
    #     as if the key 'Id' had become 'MessageId' (a longer name) and 'MessageBody' had become 'Body'
    #     (a shorter name). What's up with that weird asymmetry??
    #   - I'm not sure why the Body/MessageBody is being stored as string at all and not just left as JSON,
    #     but maybe that's an SQS thing -- it seems to only have datatypes String, Number, and Binary.
    #   - TODO: Make .send_messages and .receive_messages more representationally symmetric so that if we switch
    #           to another queueing system that can store JSON directly, we don't have code doing weird coercions.
    dlq_messages_with_headers = queue_indexer.receive_messages(target_queue='dlq')
    dlq_messages = [msg['Body'] for msg in dlq_messages_with_headers]
    response = {}
    # .send_messages expects a list of items having the form:
    #   {"uuid": ..., "sid": ..., "strict": ..., "timestamp": ..., "detail": ...}
    failed = queue_indexer.send_messages(dlq_messages) if dlq_messages else []
    response['number_failed'] = len(failed)
    response['number_migrated'] = len(dlq_messages) - len(failed)
    return response


class QueueManager(object):
    """
    Class for handling the queues responsible for coordinating indexing.
    Contains methods to inititalize queues, add both uuids and collections of
    uuids to the queue, and also various helper methods to receive/delete/replace
    messages on the queue.
    Currently the set up uses 3 queues:
    1. Primary queue for items that are directly posted, patched, or added.
    2. Secondary queue for associated items of those in the primary queue.
    3. Dead letter queue (dlq) for handling items that have issues processing
       from either the primary or secondary queues.
    """

    USE_RATE_MANAGER = False

    def __init__(self, registry, mirror_env=None, override_url=None):
        """
        __init__ will build all three queues needed with the desired settings.
        batch_size parameters conntrol how many messages are batched together
        """
        # batch sizes of messages. __all of these should be 10 at maximum__
        self.send_batch_size = 10
        self.receive_batch_size = 10
        self.delete_batch_size = 10
        self.replace_batch_size = 10
        # Amazon says we shouldn't do anything for 60 seconds after a purge request.
        # Since we can't be sure they're counting from the same place as we are, we add 1 second margin for error.
        if self.USE_RATE_MANAGER:
            self.collision_manager = RateManager(action="purge_queue", interval_seconds=60, safety_seconds=1,
                                                 allowed_attempts=1, log=log)
        else:
            self.collision_manager = LockoutManager(lockout_seconds=60, safety_seconds=1, action="purge_queue", log=log)
        self.env_name = mirror_env if mirror_env else registry.settings.get('env.name')
        self.override_url = override_url
        # local development
        if not self.env_name:
            # make sure it's something aws likes
            backup = self.generate_clean_env_namespace()
            # last case scenario
            self.env_name = backup if backup else 'fourfront-backup'

        kwargs = {
            'region_name': 'us-east-1'
        }
        if self.override_url:
            kwargs['endpoint_url'] = self.override_url
        self.client = boto3.client('sqs', **kwargs)
        # primary queue name
        self.queue_name = self.env_name + '-indexer-queue'
        # secondary queue name
        self.second_queue_name = self.env_name + '-secondary-indexer-queue'
        self.dlq_name = self.queue_name + '-dlq'
        # dictionary storing attributes for each queue, keyed by name
        # set VisibilityTimeout high because messages are batched and some items are slow
        self.queue_attrs = {
            self.queue_name: {
                'DelaySeconds': '1',  # messages initially inivisble for 1 sec
                'VisibilityTimeout': '600',
                'MessageRetentionPeriod': '1209600',  # 14 days, in seconds
                'ReceiveMessageWaitTimeSeconds': '2',  # 2 seconds of long polling
            },
            self.second_queue_name: {
                'VisibilityTimeout': '600',
                'MessageRetentionPeriod': '1209600',  # 14 days, in seconds
                'ReceiveMessageWaitTimeSeconds': '2',  # 2 seconds of long polling
            },
            self.dlq_name: {
                'VisibilityTimeout': '600',  # increase if messages going to dlq
                'MessageRetentionPeriod': '1209600',  # 14 days, in seconds
                'ReceiveMessageWaitTimeSeconds': '2',  # 2 seconds of long polling
            },
        }
        # initialize the queue and dlq here, but not on mirror queue
        if not mirror_env:
            response_urls = self.initialize(dlq=True)
            self.queue_url = response_urls.get(self.queue_name)
            self.second_queue_url = response_urls.get(self.second_queue_name)
            self.dlq_url = response_urls.get(self.dlq_name)
        else:  # assume the urls exist
            self.queue_url = self.get_queue_url(self.queue_name)
            self.second_queue_url = self.get_queue_url(self.second_queue_name)
            self.dlq_url = self.get_queue_url(self.dlq_name)
        # short names for queues. Use OrderedDict to preserve order in Py < 3.6
        self.queue_targets = OrderedDict([
            ('primary', self.queue_url),
            ('secondary', self.second_queue_url),
            ('dlq', self.dlq_url),
        ])

    @staticmethod
    def generate_clean_env_namespace():
        """ Helper that ensures the env namespace for queues is short, does not contain
            spaces or punctuation typically seen in the host """
        return socket.gethostname()[:80].replace('.', '-').replace(' ', '').replace("’", '')

    def add_uuids(self, registry, uuids, strict=False, target_queue='primary',
                  sid=None, telemetry_id=None):
        """
        Takes a list of string uuids queues them up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        If strict, the uuids will be queued with info instructing associated
        uuids NOT to be queued. If the secondary queue is targeted, strict
        should be true (though this is not enforced).

        Can optionally take an sid to add to the bodies of all messages.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        ignored(registry)
        curr_time = datetime.datetime.utcnow().isoformat()
        items = []
        for uuid in uuids:
            temp = {'uuid': uuid, 'sid': sid, 'strict': strict, 'timestamp': curr_time}
            if telemetry_id:
                temp['telemetry_id'] = telemetry_id
            items.append(temp)
        failed = self.send_messages(items, target_queue=target_queue)
        return uuids, failed

    def add_collections(self, registry, collections, strict=False, target_queue='primary',
                        telemetry_id=None):
        """
        Takes a list of collection name and queues all uuids for them.
        Also requires a registry, which is passed in automatically when using
        the /queue_indexing route.

        If strict, the uuids will be queued with info instructing associated
        uuids NOT to be queued.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        curr_time = datetime.datetime.utcnow().isoformat()
        uuids = list(get_uuids_for_types(registry, collections))
        items = []
        for uuid in uuids:
            temp = {'uuid': uuid, 'sid': None, 'strict': strict, 'timestamp': curr_time}
            if telemetry_id:
                temp['telemetry_id'] = telemetry_id
            items.append(temp)
        failed = self.send_messages(items, target_queue=target_queue)
        return uuids, failed

    def get_queue_url(self, queue_name):
        """
        Simple function that returns url of associated queue name
        """
        try:
            response = self.client.get_queue_url(
                QueueName=queue_name
            )
        except Exception:
            response = {}
        return response.get('QueueUrl')

    def get_queue_arn(self, queue_url):
        """
        Get the ARN of the specified queue
        """
        response = self.client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        return response['Attributes']['QueueArn']

    def initialize(self, dlq=False):
        """
        Initialize the queue that is used by this manager.
        For now, this is an AWS SQS standard queue.
        Will use whatever attributes are defined within self.queue_attrs.
        If dlq arg is True, then the dead letter queue will be initialized
        as well.

        Returns a queue url that is guaranteed to link to the right queue.
        """
        # dlq MUST be initialized first if used
        if dlq:
            queue_names = [self.dlq_name, self.queue_name, self.second_queue_name]
        else:
            queue_names = [self.queue_name, self.second_queue_name]
        queue_urls = {}
        for queue_name in queue_names:
            queue_attrs = self.queue_attrs[queue_name]
            queue_url = self.get_queue_url(queue_name)
            should_set_attrs = False
            if queue_url:  # see if current settings are up to date
                curr_attrs = self.client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=list(queue_attrs.keys())
                ).get('Attributes', {})
                # must remove JSON formatting from redrivePolicy to compare
                compare_attrs = queue_attrs.copy()
                if 'RedrivePolicy' in compare_attrs:
                    compare_attrs['RedrivePolicy'] = json.loads(compare_attrs['RedrivePolicy'])
                if 'RedrivePolicy' in curr_attrs:
                    curr_attrs['RedrivePolicy'] = json.loads(curr_attrs['RedrivePolicy'])
                should_set_attrs = compare_attrs != curr_attrs
            else:  # queue needs to be created
                for backoff in [30, 30, 10, 20, 30, 60, 90, 120]:  # totally arbitrary
                    if self.override_url:  # if we are mocking, catch generic exceptions
                        try:
                            response = self.client.create_queue(
                                QueueName=queue_name,
                                Attributes=queue_attrs
                            )
                        except Exception as e:
                            log.warning('Got error %s while creating mocked queue' % str(e))
                            break
                        else:
                            log.warning('\n___CREATED QUEUE WITH NAME %s___\n' % queue_name)
                            queue_url = response['QueueUrl']
                            break
                    else:  # if we are not mocking boto, take advantage of exception API
                        try:
                            response = self.client.create_queue(
                                QueueName=queue_name,
                                Attributes=queue_attrs
                            )
                        except self.client.exceptions.QueueNameExists:
                            # try to get queue url again
                            queue_url = self.get_queue_url(queue_name)
                            if queue_url:
                                should_set_attrs = True
                                break
                        except self.client.exceptions.QueueDeletedRecently:
                            log.warning('\n___MUST WAIT TO CREATE QUEUE FOR %ss___\n' % str(backoff))
                            time.sleep(backoff)
                        else:
                            log.warning('\n___CREATED QUEUE WITH NAME %s___\n' % queue_name)
                            queue_url = response['QueueUrl']
                            break
            # update the queue attributes with dlq information, which can only
            # be obtained after the dlq is created
            if queue_name == self.dlq_name:
                dlq_arn = self.get_queue_arn(queue_url)
                redrive_policy = {  # maintain this order of settings
                    'deadLetterTargetArn': dlq_arn,
                    'maxReceiveCount': 4  # num of fails before sending to dlq
                }
                # set redrive policy for queues
                for redrive_queue in [self.queue_name, self.second_queue_name]:
                    self.queue_attrs[redrive_queue]['RedrivePolicy'] = json.dumps(redrive_policy)

            # set attributes on an existing queue. not hit if queue was just created
            if should_set_attrs:
                self.client.set_queue_attributes(
                    QueueUrl=queue_url,
                    Attributes=queue_attrs
                )
            queue_urls[queue_name] = queue_url
        return queue_urls

    def _wait_until_purge_queue_allowed(self):
        self.collision_manager.wait_if_needed()

    def purge_queue(self):
        """
        Clear out the queue and dlq completely. You can no longer retrieve these messages.
        AWS says this operation takes up to 60 seconds, that operations queued before will start to disappear,
        and that operations queued within 60 seconds after may also disappear.
        """
        self._wait_until_purge_queue_allowed()
        for queue_url in [self.queue_url, self.second_queue_url, self.dlq_url]:
            try:
                self.client.purge_queue(
                    QueueUrl=queue_url
                )
                # NOTE: It's possible that we should be again calling ._wait_Until_purge_queue_allowed() here, too.
                #       The reason for the two calls would be this:
                #       - A wait before would be because we could get an error if we needed to wait and didn't.
                #         If we waited after, the only case where the wait before would matter is if there was an
                #         aborted wait that didn't wait the relevant time after. That's a possible scenario in testing.
                #       - A wait after would be to protect other operations that might be unreliable if done too soon.
                #       For now I'm going to try the simpler strategy, but I wanted to identify this as a potential
                #       source of lingering trouble. -kmp 6-May-2020
            except self.client.exceptions.PurgeQueueInProgress:
                log.warning('\n___QUEUE IS ALREADY BEING PURGED: %s___\n' % queue_url,
                            queue_url=queue_url)

    def clear_queue(self):
        """
        Manually clears all queues by repeatedly calling receieve_messages then
        deleting those messages.
        """
        for target in self.queue_targets:
            msgs = self.receive_messages(target)
            while msgs:
                self.delete_messages(msgs, target)
                msgs = self.receive_messages(target)

    def delete_queue(self, queue_url):
        """
        Remove the SQS queue with given queue_url from AWS
        Should really only be needed for local development.
        """
        response = self.client.delete_queue(
            QueueUrl=queue_url
        )
        setattr(self, queue_url, None)
        return response

    @staticmethod
    def chunk_messages(messages, chunksize):
        """
        Chunk a given number of messages into chunks of given chunksize
        """
        for i in range(0, len(messages), chunksize):
            yield messages[i:i + chunksize]

    def choose_queue_url(self, name):
        """
        Simple utility function get queue url given a target name (e.g. 'primary')
        """
        return self.queue_targets.get(name.lower(), self.queue_url)

    def send_messages(self, items, target_queue='primary', retries=0):
        """
        Send any number of 'items' as messages to sqs.
        items is a list of dictionaries with the following format:
        {
            'uuid': string uuid,
            'sid': int sid from postgres or None for secondary items,
            'strict': boolean that controls if assciated uuids are found,
            'timestamp': datetime string, should be utc,
            'detail': string containing extra information, not always used
        }
        Can batch up to 10 messages, controlled by self.send_batch_size.

        strict is a boolean that determines whether or not associated uuids
        will be found for these uuids.

        Since sending messages is something we want to be fail-proof, retry
        failed messages automatically up to 4 times.
        Returns information on messages that failed to queue despite the retries
        """
        queue_url = self.choose_queue_url(target_queue)
        failed = []
        for msg_batch in self.chunk_messages(items, self.send_batch_size):
            entries = []
            for msg in msg_batch:
                # quick workaround to communicate with old style messages
                if isinstance(msg, dict):
                    entries.append({
                        'Id': str(int(time.time() * 1000000)),
                        'MessageBody': json.dumps(msg)
                    })
                else:
                    entries.append({
                        'Id': str(int(time.time() * 1000000)),
                        'MessageBody': msg
                    })
                time.sleep(0.001)  # ensure time-based Ids are not repeated
            response = self.client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            failed_messages = response.get('Failed', [])

            if failed_messages and retries < 4:
                to_retry = []
                for fail_message in failed_messages:
                    fail_id = fail_message.get('Id')
                    if not fail_id:
                        log.error('INDEXING: Non-retryable error sending message: %s' %
                                  str(fail_message), target_queue=target_queue)
                        continue  # cannot retry this message without an Id
                    to_retry.extend([json.loads(ent['MessageBody']) for ent in entries if ent['Id'] == fail_id])
                if to_retry:
                    failed_messages = self.send_messages(to_retry, target_queue, retries=retries+1)
            failed.extend(failed_messages)
        return failed

    def receive_messages(self, target_queue='primary'):
        """
        Recieves up to self.receive_batch_size number of messages from the queue.
        Fewer (even 0) messages may be returned on any given run.

        Returns a list of messages with message metadata
        """
        # TODO: Consider whether "long polling" would be useful here to avoid some useless re-polling when queue empty.
        #  Ref: https://stackoverflow.com/questions/50558084/how-to-long-poll-amazon-sqs-service-using-boto
        #  Ref: https://aws.amazon.com/sqs/faqs/
        queue_url = self.choose_queue_url(target_queue)
        response = self.client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=self.receive_batch_size
        )
        # messages in response include ReceiptHandle and Body, most importantly
        return response.get('Messages', [])

    def delete_messages(self, messages, target_queue='primary'):
        """
        Called after a message has been successfully received and processed.
        Removes message from the queue.
        Splits messages into a batch size given by self.delete_batch_size.
        Input should be the messages directly from receive messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.

        Returns a list with any failed attempts.
        """
        queue_url = self.choose_queue_url(target_queue)
        failed = []
        for batch in self.chunk_messages(messages, self.delete_batch_size):
            # need to change message format, since deleting takes slightly
            # different fields what's return from receiving
            for i in range(len(batch)):
                to_delete = {
                    'Id': batch[i]['MessageId'],
                    'ReceiptHandle': batch[i]['ReceiptHandle']
                }
                batch[i] = to_delete
            response = self.client.delete_message_batch(
                QueueUrl=queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed

    def replace_messages(self, messages, target_queue='primary', vis_timeout=5):
        """
        Called using received messages to place them back on the queue.
        Using a VisibilityTimeout of 0 means these messages are instantly
        available to consumers.
        Number of messages in a batch is controlled by self.replace_batch_size
        Input should be the messages directly from receive messages. At the
        very least, needs a list of messages with 'Id' and 'ReceiptHandle'.

        Returns a list with any failed attempts.
        """
        queue_url = self.choose_queue_url(target_queue)
        failed = []
        for batch in self.chunk_messages(messages, self.replace_batch_size):
            for i in range(len(batch)):
                to_replace = {
                    'Id': batch[i]['MessageId'],
                    'ReceiptHandle': batch[i]['ReceiptHandle'],
                    'VisibilityTimeout': vis_timeout
                }
                batch[i] = to_replace
            response = self.client.change_message_visibility_batch(
                QueueUrl=queue_url,
                Entries=batch
            )
            failed.extend(response.get('Failed', []))
        return failed

    def number_of_messages(self):
        """
        Returns a dict with number of waiting messages in the queue and
        number of inflight (i.e. not currently visible) messages.
        Also returns info on items in the dlq.
        """
        responses = []
        for queue_url in [self.queue_url, self.second_queue_url, self.dlq_url]:
            response = self.client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible'
                ]
            )
            responses.append(response)
        formatted = {
            'primary_waiting': responses[0].get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'primary_inflight': responses[0].get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible'),
            'secondary_waiting': responses[1].get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'secondary_inflight': responses[1].get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible'),
            'dlq_waiting': responses[2].get('Attributes', {}).get('ApproximateNumberOfMessages'),
            'dlq_inflight': responses[2].get('Attributes', {}).get('ApproximateNumberOfMessagesNotVisible')
        }
        # transform in integers
        for entry in formatted:
            try:
                formatted[entry] = int(formatted[entry])
            except ValueError:
                formatted[entry] = None
        return formatted

    def queue_is_empty(self, secondary_only=True, include_inflight=False):
        """
        Returns True if the queue is empty - by default will only inspect secondary queue, otherwise all will be
        checked for any messages
        """
        message_counts = self.number_of_messages()

        # helper from Kent that will compute the count
        def get_count(kind):
            return (message_counts[kind + "_waiting"]
                    + (message_counts[kind + "_inflight"] if include_inflight else 0))

        count = 0 if secondary_only else get_count('primary')
        count += get_count('secondary')
        count += 0 if secondary_only else get_count('dlq')
        return count == 0
