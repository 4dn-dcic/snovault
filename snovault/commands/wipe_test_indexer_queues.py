import sys
import logging

import boto3

from ..elasticsearch.indexer_queue import QueueManager


logger = logging.getLogger(__name__)


def main():
    logging.basicConfig()
    if len(sys.argv) < 2:
        logger.error('Usage: python wipe_test_indexer_queues.py <TEST_JOB_ID>\n')
        exit(1)

    jid = sys.argv[1]
    # Queue names are built from the same sanitized namespace QueueManager uses for
    # env.name (periods etc replaced), so the list-queues prefix has to match that,
    # not the raw TEST_JOB_ID (which is otherwise usable as-is for ES index names).
    prefix = QueueManager.clean_env_namespace(jid)
    logger.info('Wiping SQS queues on us-east-1 with prefix %s\n' % prefix)
    try:
        client = boto3.client('sqs', region_name='us-east-1')
        queue_urls = client.list_queues(QueueNamePrefix=prefix).get('QueueUrls', [])
    except Exception as exc:
        logger.error('Failed to list queues with exception: %s\n' % str(exc))
        exit(1)

    if not queue_urls:
        logger.info('No SQS queues found with prefix %s' % prefix)
        return

    failures = False
    for queue_url in queue_urls:
        try:
            client.delete_queue(QueueUrl=queue_url)
            logger.info('Deleted queue %s' % queue_url)
        except Exception as exc:
            failures = True
            logger.error('Failed to delete queue %s with exception: %s\n' % (queue_url, str(exc)))
    if failures:
        exit(1)


if __name__ == '__main__':
    main()
