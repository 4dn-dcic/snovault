from unittest import mock

import pytest

from ..commands import wipe_test_indexer_queues


def test_main_requires_test_job_id(capsys):
    with mock.patch("sys.argv", ["wipe-test-indexer-queues"]):
        with pytest.raises(SystemExit):
            wipe_test_indexer_queues.main()


def test_main_sanitizes_prefix_and_deletes_matching_queues():
    """ TEST_JOB_ID (e.g. from the matrix python version) can contain periods, which
    SQS queue names can't, so the list-queues prefix must be sanitized the same way
    QueueManager sanitizes env.name when building queue names. """
    with mock.patch("sys.argv", ["wipe-test-indexer-queues", "sno-test-indexing-456-3.11-"]):
        with mock.patch("boto3.client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.list_queues.return_value = {
                'QueueUrls': ['http://queue/1', 'http://queue/2']
            }
            wipe_test_indexer_queues.main()

    mock_client.list_queues.assert_called_once_with(QueueNamePrefix="sno-test-indexing-456-3-11-")
    assert mock_client.delete_queue.call_count == 2
    mock_client.delete_queue.assert_any_call(QueueUrl='http://queue/1')
    mock_client.delete_queue.assert_any_call(QueueUrl='http://queue/2')


def test_main_no_matching_queues_is_a_no_op():
    with mock.patch("sys.argv", ["wipe-test-indexer-queues", "sno-test-unit-456-3.11-"]):
        with mock.patch("boto3.client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.list_queues.return_value = {'QueueUrls': []}
            wipe_test_indexer_queues.main()

    assert mock_client.delete_queue.call_count == 0


def test_main_list_queues_access_denied_does_not_raise_or_exit_nonzero():
    """ The CI IAM role is not (yet) authorized for sqs:ListQueues - a missing cleanup
    permission must not fail the whole CI job, just leave the queues in place. """
    with mock.patch("sys.argv", ["wipe-test-indexer-queues", "sno-test-indexing-456-3.11-"]):
        with mock.patch("boto3.client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.list_queues.side_effect = Exception(
                "An error occurred (AccessDenied) when calling the ListQueues operation"
            )
            wipe_test_indexer_queues.main()  # must not raise or call sys.exit

    assert mock_client.delete_queue.call_count == 0


def test_main_delete_queue_access_denied_does_not_raise_or_exit_nonzero():
    with mock.patch("sys.argv", ["wipe-test-indexer-queues", "sno-test-indexing-456-3.11-"]):
        with mock.patch("boto3.client") as mock_boto3_client:
            mock_client = mock_boto3_client.return_value
            mock_client.list_queues.return_value = {'QueueUrls': ['http://queue/1']}
            mock_client.delete_queue.side_effect = Exception(
                "An error occurred (AccessDenied) when calling the DeleteQueue operation"
            )
            wipe_test_indexer_queues.main()  # must not raise or call sys.exit
