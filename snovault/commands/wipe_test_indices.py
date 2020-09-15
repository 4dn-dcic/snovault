import sys
import logging
from dcicutils.es_utils import create_es_client


logger = logging.getLogger(__name__)


def main():
    logging.basicConfig()
    if len(sys.argv) < 3:
        logger.error('Usage: python wipe_test_indices.py <TRAVIS_JOB_ID> <REMOTE_ES>\n')
        exit(1)

    jid, r_es = sys.argv[1], sys.argv[2]
    logger.info('Wiping ES instances on %s with prefix %s\n' % (r_es, jid))
    try:
        client = create_es_client(r_es, use_aws_auth=True)
    except:
        logger.error('Failed to get ES client')
        exit(1)

    try:
        client.indices.delete(index=jid+'*')
        logger.info('Successfully deleted indices with prefix %s' % jid)
    except Exception as exc:
        logger.error('Failed to delete indices with exception: %s\n' % str(exc))
        exit(1)

if __name__ == '__main__':
    main()
