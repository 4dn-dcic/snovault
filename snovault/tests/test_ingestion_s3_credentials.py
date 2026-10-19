"""
Regression coverage for the credential source of the metadata bundle ingestion S3 client.

Background: ``submit_for_ingestion`` and both ``process_submission`` implementations used to build
their S3 client from the legacy static S3 IAM *user* keys in the global application configuration
(``S3_AWS_ACCESS_KEY_ID`` / ``S3_AWS_SECRET_ACCESS_KEY``). Those keys are not provisioned in newer
environments, so every metadata bundle PutObject/GetObject failed with ``InvalidAccessKeyId``.
They now use the boto3 default credential provider chain, which inside a task resolves the ECS task
role's temporary, self-refreshing credentials.

These tests drive the real view / real handler, the real client factory and real boto3 SigV4
signing. Only the Pyramid plumbing and the HTTP transport are synthetic: the transport answers on
the basis of the access key id that actually appears in the signed ``Authorization`` header, so a
test can tell exactly which identity went out on the wire. No network, no AWS, no SQS.
"""

import boto3
import cgi
import io
import json
import os
import pytest
import re
import threading

from botocore.awsrequest import AWSResponse
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

from ..ingestion import ingestion_listener as il
from ..ingestion import ingestion_message_handler_default as handler_module
from ..ingestion.ingestion_listener_base import IngestionListenerBase
from ..ingestion.ingestion_message import IngestionMessage
from ..util import make_s3_client, make_s3_client_for_ingestion, build_s3_presigned_get_url


pytestmark = [pytest.mark.unit]


SUBMISSION_ID = '2a6e1c3e-0000-4000-8000-00000000beef'
BUNDLES_BUCKET = 'some-env-metadata-bundles'
KMS_KEY_ID = 'synthetic-kms-key-id'

STALE_USER_KEY = 'AKIASYNTHETICSTALE00'
TASK_ROLE_KEY = 'ASIASYNTHETICTASKRL0'
TASK_ROLE_TOKEN = 'FwoGZXIvYXdzEJr--synthetic-session-token'

LEGACY_IDENTITY = {
    'S3_AWS_ACCESS_KEY_ID': STALE_USER_KEY,
    'S3_AWS_SECRET_ACCESS_KEY': 'synthetic-secret',
    'ENCODED_S3_ENCRYPT_KEY_ID': KMS_KEY_ID,
}

IDENTITY_WITHOUT_STATIC_KEYS = {'ENCODED_S3_ENCRYPT_KEY_ID': KMS_KEY_ID}

TASK_ROLE_ENV = {
    'AWS_ACCESS_KEY_ID': TASK_ROLE_KEY,
    'AWS_SECRET_ACCESS_KEY': 'synthetic-secret',
    'AWS_SESSION_TOKEN': TASK_ROLE_TOKEN,
    'AWS_DEFAULT_REGION': 'us-east-1',
}


# ---------------------------------------------------------------------------------------------
# Local fake AWS transport: real signing, synthetic answers.
# ---------------------------------------------------------------------------------------------

ERROR_BODY = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<Error><Code>{code}</Code><Message>{message}</Message>'
    '<AWSAccessKeyId>{akid}</AWSAccessKeyId><RequestId>synthetic</RequestId></Error>'
)

INVALID_KEY_MESSAGE = 'The AWS Access Key Id you provided does not exist in our records.'


class _RawBody:
    """Minimal urllib3-HTTPResponse-ish shim botocore can read from."""

    def __init__(self, data):
        self._buf = io.BytesIO(data)

    def stream(self, amt=1024, **kwargs):
        while True:
            chunk = self._buf.read(amt)
            if not chunk:
                break
            yield chunk

    def read(self, amt=None):
        return self._buf.read() if amt is None else self._buf.read(amt)

    def close(self):
        self._buf.close()


def _decode(value):
    return value.decode() if isinstance(value, bytes) else value


class FakeS3Wire:
    """Records every signed S3 request and answers it the way S3 would."""

    def __init__(self, live_key_ids, *, get_body=None, deny=False):
        self.live_key_ids = set(live_key_ids)
        self.get_body = get_body
        self.deny = deny
        self.requests = []

    def install(self, client):
        client.meta.events.register('before-send.s3.*', self._before_send)
        return client

    @property
    def access_key_ids(self):
        return [record['access_key_id'] for record in self.requests]

    def _before_send(self, request, **kwargs):
        auth = _decode(request.headers.get('Authorization')) or ''
        matched = re.search(r'Credential=([^/]+)/', auth)
        akid = matched.group(1) if matched else None
        self.requests.append({
            'method': request.method,
            'url': request.url,
            'access_key_id': akid,
            'session_token': _decode(request.headers.get('X-Amz-Security-Token')),
            'sse': _decode(request.headers.get('x-amz-server-side-encryption')),
            'sse_kms_key_id': _decode(request.headers.get('x-amz-server-side-encryption-aws-kms-key-id')),
        })
        if akid not in self.live_key_ids:
            return self._error(request, 403, 'InvalidAccessKeyId', INVALID_KEY_MESSAGE, akid)
        if self.deny:
            return self._error(request, 403, 'AccessDenied', 'Access Denied', akid)
        body = self.get_body if request.method == 'GET' and self.get_body is not None else b''
        return AWSResponse(request.url, 200,
                           {'ETag': '"synthetic"', 'Content-Length': str(len(body))},
                           _RawBody(body))

    @staticmethod
    def _error(request, status, code, message, akid):
        body = ERROR_BODY.format(code=code, message=message, akid=akid).encode()
        return AWSResponse(request.url, status,
                           {'Content-Type': 'application/xml', 'Content-Length': str(len(body))},
                           _RawBody(body))


# ---------------------------------------------------------------------------------------------
# Minimal Pyramid-shaped plumbing for the view.
# ---------------------------------------------------------------------------------------------

class FakeRegistry(dict):
    def __init__(self, settings):
        super().__init__()
        self.settings = settings


class FakeRequest:
    def __init__(self, registry, post):
        self.registry = registry
        self.POST = post
        self.content_type = 'multipart/form-data'
        self.path_info = f'/ingestion-submissions/{SUBMISSION_ID}/submit_for_ingestion'
        self.authenticated_userid = 'synthetic-user'


class FakeQueueManager:
    def __init__(self):
        self.added = []

    def add_uuids(self, uuids, ingestion_type=None):
        self.added.append((uuids, ingestion_type))
        return uuids, []


def _datafile(content=b'synthetic\tmetadata\tbundle\n'):
    storage = cgi.FieldStorage()
    storage.filename = 'synthetic_submission.tsv'
    storage.file = io.BytesIO(content)
    return storage


@pytest.fixture(autouse=True)
def isolated_credential_environment(tmp_path, monkeypatch):
    """ Keeps the default credential chain away from this developer's or runner's real credentials.

        Without this the chain happily reads ~/.aws or probes instance metadata, which would make
        these tests both non-hermetic and slow.
    """
    monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(tmp_path / 'no-such-credentials'))
    monkeypatch.setenv('AWS_CONFIG_FILE', str(tmp_path / 'no-such-config'))
    monkeypatch.setenv('AWS_EC2_METADATA_DISABLED', 'true')
    for name in ('AWS_PROFILE', 'AWS_DEFAULT_PROFILE', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                 'AWS_SESSION_TOKEN', 'AWS_SECURITY_TOKEN', 'AWS_CONTAINER_CREDENTIALS_FULL_URI',
                 'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI', 'AWS_WEB_IDENTITY_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    # boto3 caches resolved credentials on its default session, which would leak between tests.
    saved_session = boto3.DEFAULT_SESSION
    boto3.DEFAULT_SESSION = None
    yield
    boto3.DEFAULT_SESSION = saved_session


def run_submission(*, identity, env_credentials, wire, datafile=True, encrypt=True):
    """ Drives the real ``submit_for_ingestion`` view once.

        Returns (outcome, queue_manager). ``outcome`` is 'SUCCESS' or '<ExceptionName>: <message>'.
    """
    real_boto3_client = boto3.client

    def client_factory(*args, **kwargs):
        return wire.install(real_boto3_client(*args, **kwargs))

    settings = {'metadata_bundles_bucket': BUNDLES_BUCKET, 'env.name': 'some-env'}
    if encrypt:
        settings['s3_encrypt_key_id'] = KMS_KEY_ID
    registry = FakeRegistry(settings)
    queue_manager = FakeQueueManager()
    registry[il.INGESTION_QUEUE] = queue_manager

    post = {'ingestion_type': 'metadata_bundle'}
    if datafile:
        post['datafile'] = _datafile()
    request = FakeRequest(registry, post)
    instance = {'ingestion_type': 'metadata_bundle', '@id': f'/ingestion-submissions/{SUBMISSION_ID}/'}

    env_patch = dict(env_credentials)
    env_patch['IDENTITY'] = 'synthetic-identity'

    with mock.patch.dict(os.environ, env_patch, clear=False), \
            mock.patch.object(boto3, 'client', client_factory), \
            mock.patch('snovault.util.assume_identity', return_value=identity), \
            mock.patch.object(il, 'app_project', return_value=mock.MagicMock()), \
            mock.patch.object(il, 'subrequest_object', return_value=instance), \
            mock.patch.object(il, 'beanstalk_env_from_request', return_value='some-env'), \
            mock.patch.object(il, 'get_trusted_email', return_value='submitter@example.org'), \
            mock.patch.object(il, 'is_stg_or_prd_env', return_value=False):
        try:
            il.submit_for_ingestion(None, request)
            outcome = 'SUCCESS'
        except Exception as e:  # noQA - the identity of whatever comes out is the point
            outcome = f'{type(e).__name__}: {e}'
    return outcome, queue_manager


def run_process_submission(module, *, identity, env_credentials, wire, manifest=None):
    """ Drives the real ``process_submission`` of either module far enough to read manifest.json.

        The handler body past the S3 read needs a live VirtualApp, so ``vapp_for_email``, the
        ``SubmissionFolio`` and the ingestion processor are stubbed. The credential selection, the
        client construction and the signed GetObject are all real.
    """
    real_boto3_client = boto3.client

    def client_factory(*args, **kwargs):
        return wire.install(real_boto3_client(*args, **kwargs))

    manifest_body = json.dumps(manifest or {'email': 'submitter@example.org'}).encode('utf-8')
    wire.get_body = manifest_body

    env_patch = dict(env_credentials)
    env_patch['IDENTITY'] = 'synthetic-identity'

    vapp = mock.MagicMock()
    vapp_cm = mock.MagicMock()
    vapp_cm.__enter__ = mock.Mock(return_value=vapp)
    vapp_cm.__exit__ = mock.Mock(return_value=False)

    with mock.patch.dict(os.environ, env_patch, clear=False), \
            mock.patch.object(boto3, 'client', client_factory), \
            mock.patch('snovault.util.assume_identity', return_value=identity), \
            mock.patch.object(module, 'vapp_for_email', return_value=vapp_cm), \
            mock.patch.object(module, 'SubmissionFolio', mock.MagicMock()), \
            mock.patch.object(module, 'get_ingestion_processor',
                              return_value=mock.Mock(return_value='synthetic-result')):
        try:
            result = module.process_submission(submission_id=SUBMISSION_ID,
                                               ingestion_type='metadata_bundle',
                                               app=mock.MagicMock(),
                                               bundles_bucket=BUNDLES_BUCKET)
            outcome = 'SUCCESS'
        except Exception as e:  # noQA
            result = None
            outcome = f'{type(e).__name__}: {e}'
    return outcome, result


# ---------------------------------------------------------------------------------------------
# submit_for_ingestion
# ---------------------------------------------------------------------------------------------

def test_submit_for_ingestion_signs_with_default_credential_chain():
    """ The reported failure, turned around: the stale static key is the one S3 rejects. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
    outcome, queue_manager = run_submission(identity=LEGACY_IDENTITY, env_credentials=TASK_ROLE_ENV,
                                            wire=wire)
    assert outcome == 'SUCCESS'
    # Both the datafile and manifest.json went up.
    assert [record['method'] for record in wire.requests] == ['PUT', 'PUT']
    assert wire.requests[0]['url'].endswith(f'/{SUBMISSION_ID}/datafile.tsv')
    assert wire.requests[1]['url'].endswith(f'/{SUBMISSION_ID}/manifest.json')
    # Signed by the task role, carrying its session token -- never by the stale static user key.
    assert wire.access_key_ids == [TASK_ROLE_KEY, TASK_ROLE_KEY]
    assert STALE_USER_KEY not in wire.access_key_ids
    for record in wire.requests:
        assert record['session_token'] == TASK_ROLE_TOKEN
        # SSE-KMS is unaffected by the credential change.
        assert record['sse'] == 'aws:kms'
        assert record['sse_kms_key_id'] == KMS_KEY_ID
    assert queue_manager.added == [([SUBMISSION_ID], 'metadata_bundle')]


def test_submit_for_ingestion_ignores_static_keys_even_when_they_are_valid():
    """ The static user key is no longer consulted at all, not merely preferred less. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY, STALE_USER_KEY})
    outcome, _ = run_submission(identity=LEGACY_IDENTITY, env_credentials=TASK_ROLE_ENV, wire=wire)
    assert outcome == 'SUCCESS'
    assert set(wire.access_key_ids) == {TASK_ROLE_KEY}


def test_submit_for_ingestion_without_static_keys_in_identity():
    """ An identity carrying no S3_AWS_ACCESS_KEY_ID used to raise TypeError before signing. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
    outcome, queue_manager = run_submission(identity=IDENTITY_WITHOUT_STATIC_KEYS,
                                            env_credentials=TASK_ROLE_ENV, wire=wire)
    assert outcome == 'SUCCESS'
    assert set(wire.access_key_ids) == {TASK_ROLE_KEY}
    assert queue_manager.added == [([SUBMISSION_ID], 'metadata_bundle')]


def test_submit_for_ingestion_permission_failure_surfaces_without_fallback():
    """ A denial must surface, not quietly retry through the static user or the upload role. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY}, deny=True)
    outcome, queue_manager = run_submission(identity=LEGACY_IDENTITY, env_credentials=TASK_ROLE_ENV,
                                            wire=wire)
    assert outcome.startswith('SubmissionFailure:')
    assert 'AccessDenied' in outcome
    # Exactly one attempt, with the task role: no second hop and no static-key retry.
    assert wire.access_key_ids == [TASK_ROLE_KEY]
    assert queue_manager.added == []


def test_submit_for_ingestion_missing_credentials_surfaces_without_fallback():
    """ With nothing in the chain the request is never signed; it does not fall back to the keys. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
    outcome, queue_manager = run_submission(identity=LEGACY_IDENTITY,
                                            env_credentials={'AWS_DEFAULT_REGION': 'us-east-1'},
                                            wire=wire)
    assert outcome.startswith('NoCredentialsError:')
    assert wire.requests == []
    assert queue_manager.added == []


# ---------------------------------------------------------------------------------------------
# Container (ECS task role) credential provider: refresh and failure
# ---------------------------------------------------------------------------------------------

class _CredentialEndpoint:
    """ A loopback HTTP server that answers like the ECS container credentials endpoint.

        Used to exercise botocore's real ``ContainerProvider`` -- the provider a task actually
        uses -- including its self-refresh, rather than the environment-variable provider.
    """

    def __init__(self, *, status=200, expiry_seconds=300):
        self.status = status
        self.expiry_seconds = expiry_seconds
        self.issued = []
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noQA - BaseHTTPRequestHandler naming
                if endpoint.status != 200:
                    self.send_response(endpoint.status)
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                import datetime
                token = f'synthetic-container-token-{len(endpoint.issued)}'
                endpoint.issued.append(token)
                expiration = (datetime.datetime.now(datetime.timezone.utc)
                              + datetime.timedelta(seconds=endpoint.expiry_seconds))
                body = json.dumps({
                    'AccessKeyId': TASK_ROLE_KEY,
                    'SecretAccessKey': 'synthetic-secret',
                    'Token': token,
                    'Expiration': expiration.strftime('%Y-%m-%dT%H:%M:%SZ'),
                }).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # noQA - silence the default stderr logging
                pass

        self._server = HTTPServer(('127.0.0.1', 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def uri(self):
        host, port = self._server.server_address
        return f'http://{host}:{port}/creds'

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
        return False


def test_submit_for_ingestion_refreshes_container_credentials():
    """ Successive submissions pick up refreshed task-role credentials with no caching logic here.

        The endpoint hands out credentials expiring inside botocore's mandatory-refresh window, so
        a second submission must fetch again and go out under a different session token.
    """
    with _CredentialEndpoint(expiry_seconds=300) as endpoint:
        env = {'AWS_CONTAINER_CREDENTIALS_FULL_URI': endpoint.uri, 'AWS_DEFAULT_REGION': 'us-east-1'}
        first_wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
        first_outcome, _ = run_submission(identity=LEGACY_IDENTITY, env_credentials=env,
                                          wire=first_wire)
        second_wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
        second_outcome, _ = run_submission(identity=LEGACY_IDENTITY, env_credentials=env,
                                           wire=second_wire)

    assert first_outcome == 'SUCCESS'
    assert second_outcome == 'SUCCESS'
    assert set(first_wire.access_key_ids) == {TASK_ROLE_KEY}
    assert set(second_wire.access_key_ids) == {TASK_ROLE_KEY}
    assert len(endpoint.issued) >= 2, "credentials were never refreshed"
    assert first_wire.requests[0]['session_token'] != second_wire.requests[0]['session_token']


def test_submit_for_ingestion_credential_provider_failure_surfaces():
    """ A task role that cannot be resolved is an error, not a reason to use the static keys. """
    with _CredentialEndpoint(status=500) as endpoint:
        env = {'AWS_CONTAINER_CREDENTIALS_FULL_URI': endpoint.uri, 'AWS_DEFAULT_REGION': 'us-east-1'}
        wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
        outcome, queue_manager = run_submission(identity=LEGACY_IDENTITY, env_credentials=env,
                                                wire=wire)
    assert 'CredentialRetrievalError' in outcome or 'NoCredentialsError' in outcome
    assert STALE_USER_KEY not in wire.access_key_ids
    assert queue_manager.added == []


# ---------------------------------------------------------------------------------------------
# process_submission (ingester side), both copies
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize('module', [il, handler_module],
                         ids=['ingestion_listener', 'ingestion_message_handler_default'])
def test_process_submission_reads_manifest_with_default_credential_chain(module):
    """ Fixing only the portal-side PutObject would move the failure here, so cover both copies. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
    outcome, result = run_process_submission(module, identity=LEGACY_IDENTITY,
                                             env_credentials=TASK_ROLE_ENV, wire=wire)
    assert outcome == 'SUCCESS'
    assert result['submission_id'] == SUBMISSION_ID
    assert [record['method'] for record in wire.requests] == ['GET']
    assert wire.requests[0]['url'].endswith(f'/{SUBMISSION_ID}/manifest.json')
    assert wire.access_key_ids == [TASK_ROLE_KEY]
    assert wire.requests[0]['session_token'] == TASK_ROLE_TOKEN


@pytest.mark.parametrize('module', [il, handler_module],
                         ids=['ingestion_listener', 'ingestion_message_handler_default'])
def test_process_submission_denied_read_surfaces_without_fallback(module):
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY}, deny=True)
    outcome, _ = run_process_submission(module, identity=LEGACY_IDENTITY,
                                        env_credentials=TASK_ROLE_ENV, wire=wire)
    assert 'AccessDenied' in outcome
    assert wire.access_key_ids == [TASK_ROLE_KEY]


def test_ingestion_message_handler_default_uses_default_credential_chain():
    """ The listener's own entry point, with INGEST_AS_USER left at its default of True. """
    wire = FakeS3Wire(live_key_ids={TASK_ROLE_KEY})
    real_boto3_client = boto3.client

    def client_factory(*args, **kwargs):
        return wire.install(real_boto3_client(*args, **kwargs))

    wire.get_body = json.dumps({'email': 'submitter@example.org'}).encode('utf-8')

    message = IngestionMessage({'Body': json.dumps({'uuid': SUBMISSION_ID,
                                                    'ingestion_type': 'metadata_bundle'})})

    class FakeListener(IngestionListenerBase):
        INGEST_AS_USER = True
        vapp = mock.MagicMock()

    listener = FakeListener()

    vapp = mock.MagicMock()
    vapp_cm = mock.MagicMock()
    vapp_cm.__enter__ = mock.Mock(return_value=vapp)
    vapp_cm.__exit__ = mock.Mock(return_value=False)

    env_patch = dict(TASK_ROLE_ENV)
    env_patch['IDENTITY'] = 'synthetic-identity'

    with mock.patch.dict(os.environ, env_patch, clear=False), \
            mock.patch.object(boto3, 'client', client_factory), \
            mock.patch('snovault.util.assume_identity', return_value=LEGACY_IDENTITY), \
            mock.patch.object(handler_module, 'metadata_bundles_bucket', return_value=BUNDLES_BUCKET), \
            mock.patch.object(handler_module, 'vapp_for_email', return_value=vapp_cm), \
            mock.patch.object(handler_module, 'SubmissionFolio', mock.MagicMock()), \
            mock.patch.object(handler_module, 'get_ingestion_processor',
                              return_value=mock.Mock(return_value='synthetic-result')):
        assert handler_module.ingestion_message_handler_default(message, listener) is True

    assert wire.access_key_ids == [TASK_ROLE_KEY]
    assert wire.requests[0]['url'].endswith(f'/{SUBMISSION_ID}/manifest.json')


# ---------------------------------------------------------------------------------------------
# Client factories in isolation
# ---------------------------------------------------------------------------------------------

def test_make_s3_client_for_ingestion_preserves_region_and_signature():
    captured = {}

    def client_factory(*args, **kwargs):
        captured.update(kwargs)
        return mock.MagicMock()

    with mock.patch.dict(os.environ, {'IDENTITY': 'synthetic-identity'}, clear=False), \
            mock.patch.object(boto3, 'client', client_factory), \
            mock.patch('snovault.util.assume_identity', return_value=LEGACY_IDENTITY):
        make_s3_client_for_ingestion()

    assert 'aws_access_key_id' not in captured
    assert 'aws_secret_access_key' not in captured
    assert captured['region_name'] == 'us-east-1'
    assert captured['config'].signature_version == 's3v4'


def test_make_s3_client_for_ingestion_without_identity():
    """ Local/non-orchestrated use is unchanged: whatever the developer's own chain resolves. """
    captured = {}

    def client_factory(*args, **kwargs):
        captured.update(kwargs)
        return mock.MagicMock()

    assume_identity = mock.Mock()
    with mock.patch.dict(os.environ, {}, clear=False), \
            mock.patch.object(boto3, 'client', client_factory), \
            mock.patch('snovault.util.assume_identity', assume_identity):
        os.environ.pop('IDENTITY', None)
        make_s3_client_for_ingestion()

    assert captured == {}
    assume_identity.assert_not_called()


def test_make_s3_client_still_uses_static_keys_for_presigned_downloads():
    """ Presigned GET lifetimes depend on long-lived credentials, so make_s3_client is untouched. """
    captured = {}

    def client_factory(*args, **kwargs):
        captured.update(kwargs)
        return mock.MagicMock()

    with mock.patch.dict(os.environ, {'IDENTITY': 'synthetic-identity'}, clear=False), \
            mock.patch.object(boto3, 'client', client_factory), \
            mock.patch('snovault.util.assume_identity', return_value=LEGACY_IDENTITY):
        make_s3_client()

    assert captured['aws_access_key_id'] == STALE_USER_KEY
    assert captured['aws_secret_access_key'] == LEGACY_IDENTITY['S3_AWS_SECRET_ACCESS_KEY']
    assert captured['region_name'] == 'us-east-1'


def test_build_s3_presigned_get_url_unchanged():
    client = mock.MagicMock()
    with mock.patch('snovault.util.make_s3_client', return_value=client):
        build_s3_presigned_get_url(params={'Bucket': 'some-bucket', 'Key': 'some-key'})
    client.generate_presigned_url.assert_called_once_with(
        ClientMethod='get_object',
        Params={'Bucket': 'some-bucket', 'Key': 'some-key'},
        ExpiresIn=36 * 60 * 60,
    )


def test_ingestion_modules_do_not_import_the_static_key_client():
    """ Guards against a future edit quietly routing ingestion back through the static keys. """
    assert not hasattr(il, 'make_s3_client')
    assert not hasattr(handler_module, 'make_s3_client')
    assert il.make_s3_client_for_ingestion is make_s3_client_for_ingestion
    assert handler_module.make_s3_client_for_ingestion is make_s3_client_for_ingestion
