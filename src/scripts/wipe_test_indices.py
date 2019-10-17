import sys
from dcicutils import es_utils


if len(sys.argv) < 3:
    print('Usage: python wipe_test_indices.py <TRAVIS_JOB_ID> <REMOTE_ES>')
    exit(1)

jid, r_es = sys.argv[1], sys.argv[2]
print('Wiping ES instances on %s with prefix %s' % (r_es, jid))
try:
    client = es_utils.create_es_client(r_es, use_aws_auth=True)
except:
    print('Failed to get ES client')
    exit(1)

client.indices.delete(index=jid+'*')
