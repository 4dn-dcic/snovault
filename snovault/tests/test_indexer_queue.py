"""Pure-logic unit tests for QueueManager and the receive_n_messages test helper.

These deliberately avoid the live-ES/SQS ``app`` fixture (and its autouse
``setup_and_teardown`` in test_indexing.py) so they can run without a live AWS/ES
environment - see the INDEXING CI flakiness investigation that motivated the
queue-namespacing/long-polling fixes these tests cover.
"""
import pytest
from unittest import mock

from ..elasticsearch.indexer_queue import QueueManager
from .test_indexing import receive_n_messages


def make_registry(env_name=None, indexer_namespace=None):
    class MockRegistry:
        settings = {'env.name': env_name, 'indexer.namespace': indexer_namespace}
    return MockRegistry()


def make_queue_manager(env_name=None, indexer_namespace=None):
    """ Builds a QueueManager with boto3/initialize mocked out so no real AWS calls occur. """
    expected_env = env_name or indexer_namespace
    queue_name = expected_env + '-indexer-queue'
    second_queue_name = expected_env + '-secondary-indexer-queue'
    dlq_name = queue_name + '-dlq'
    queue_urls = {
        queue_name: 'http://primary',
        second_queue_name: 'http://secondary',
        dlq_name: 'http://dlq',
    }
    registry = make_registry(env_name=env_name, indexer_namespace=indexer_namespace)
    with mock.patch("boto3.client") as mock_boto3_client:
        with mock.patch.object(QueueManager, "initialize", return_value=queue_urls):
            manager = QueueManager(registry)
    return manager, mock_boto3_client


def test_clean_env_namespace_strips_dots_spaces_and_smart_quotes():
    dirty = "sno-test-indexing-456-3.11- host’s name"
    cleaned = QueueManager.clean_env_namespace(dirty)
    assert '.' not in cleaned
    assert ' ' not in cleaned
    assert '’' not in cleaned


def test_clean_env_namespace_truncates_to_80_chars():
    long_namespace = "a" * 200
    assert len(QueueManager.clean_env_namespace(long_namespace)) == 80


def test_queue_manager_sanitizes_explicit_env_name_with_python_version():
    """ A test-run identifier like INDEXER_NAMESPACE_FOR_TESTING can embed a python
    version (e.g. "3.11"), which SQS queue names can't contain periods for. QueueManager
    must sanitize an explicitly-provided env.name the same way it already sanitizes the
    hostname-derived fallback used when env.name is unset. """
    manager, _ = make_queue_manager(env_name="sno-test-indexing-456-3.11-")
    assert manager.env_name == "sno-test-indexing-456-3-11-"
    assert manager.queue_name == "sno-test-indexing-456-3-11--indexer-queue"
    assert '.' not in manager.queue_name


def test_queue_manager_falls_back_to_indexer_namespace_when_env_name_unset():
    """ test_indexing.py namespaces SQS queues per CI run via settings['indexer.namespace']
    (INDEXER_NAMESPACE_FOR_TESTING) rather than settings['env.name'], specifically so that
    settings['env.name'] stays falsy - see test_env_name_must_stay_unset_for_blue_green_safety
    below for why. QueueManager must still pick up indexer.namespace (sanitized) in that case. """
    manager, _ = make_queue_manager(indexer_namespace="sno-test-indexing-456-3.11-")
    assert manager.env_name == "sno-test-indexing-456-3-11-"
    assert manager.queue_name == "sno-test-indexing-456-3-11--indexer-queue"


def test_queue_manager_prefers_env_name_over_indexer_namespace():
    """ indexer.namespace is only a substitute for a real deployment env.name, not an
    override of one. """
    manager, _ = make_queue_manager(env_name="some-env", indexer_namespace="sno-test-indexing-456-")
    assert manager.env_name == "some-env"


def test_env_name_must_stay_unset_for_blue_green_safety():
    """ snovault.elasticsearch's includeme() does
    `mirror_env = blue_green_mirror_env(env_name) if env_name else None`
    using settings['env.name'] directly (not QueueManager.env_name). In an
    orchestrated-but-unconfigured environment (e.g. CI, no IDENTITY available),
    blue_green_mirror_env(...) raises rather than returning None for a falsy env_name -
    it's only skipped because `env_name` up front is falsy. So test settings must
    namespace SQS queues via settings['indexer.namespace'] (see the fixture in
    test_indexing.py's app_settings), never by making settings['env.name'] truthy,
    or app construction would crash before any test runs. """
    registry = make_registry(indexer_namespace="sno-test-indexing-456-3.11-")
    assert not registry.settings.get('env.name')


def test_receive_messages_passes_wait_time_seconds():
    """ receive_messages should pass WaitTimeSeconds explicitly (resolving a longstanding
    TODO), matching the queue's own configured 2-second ReceiveMessageWaitTimeSeconds by
    default - see the docstring/comment on receive_messages for why this intentionally
    isn't raised higher (measured live-CI regression from a version of this fix that did). """
    manager, mock_boto3_client = make_queue_manager(env_name="some-env")
    mock_client = mock_boto3_client.return_value
    mock_client.receive_message.return_value = {'Messages': []}

    manager.receive_messages()
    mock_client.receive_message.assert_called_once_with(
        QueueUrl=manager.queue_url,
        MaxNumberOfMessages=manager.receive_batch_size,
        WaitTimeSeconds=2,
    )

    mock_client.receive_message.reset_mock()
    manager.receive_messages(wait_time_seconds=20)
    mock_client.receive_message.assert_called_once_with(
        QueueUrl=manager.queue_url,
        MaxNumberOfMessages=manager.receive_batch_size,
        WaitTimeSeconds=20,
    )


class FakeQueue:
    """ Minimal stand-in exposing just receive_messages/delete_messages, for testing the
    receive_n_messages test helper's surplus-handling logic in isolation from real SQS. """

    def __init__(self, batches):
        self._batches = list(batches)
        self.deleted = []

    def receive_messages(self, target_queue='primary'):
        if self._batches:
            return self._batches.pop(0)
        return []

    def delete_messages(self, messages, target_queue='primary'):
        self.deleted.extend(messages)
        return []


def test_receive_n_messages_returns_exact_count():
    queue = FakeQueue([[{'MessageId': '1', 'ReceiptHandle': 'r1'}]])
    received = receive_n_messages(queue=queue, n=1, tries=1, wait_seconds=0)
    assert received == [{'MessageId': '1', 'ReceiptHandle': 'r1'}]
    assert queue.deleted == []


def test_receive_n_messages_discards_surplus_instead_of_raising():
    """ A queue polluted with stale/leftover messages (e.g. left behind by a prior test)
    used to make this helper burn its whole retry budget and raise a misleading "only
    received N, but wanted n" error even though N was actually greater than n. It should
    instead succeed immediately, return exactly n messages, and delete the surplus off
    the queue so it doesn't resurface and confuse a later test. """
    surplus_message = {'MessageId': '2', 'ReceiptHandle': 'r2'}
    queue = FakeQueue([[{'MessageId': '1', 'ReceiptHandle': 'r1'}, surplus_message]])
    received = receive_n_messages(queue=queue, n=1, tries=1, wait_seconds=0)
    assert received == [{'MessageId': '1', 'ReceiptHandle': 'r1'}]
    assert queue.deleted == [surplus_message]


def test_receive_n_messages_still_raises_when_short():
    queue = FakeQueue([[{'MessageId': '1', 'ReceiptHandle': 'r1'}]])
    with pytest.raises(AssertionError):
        receive_n_messages(queue=queue, n=2, tries=1, wait_seconds=0)
