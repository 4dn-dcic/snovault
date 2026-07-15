"""Pure mocked tests for ingestion queue deletion and listener retries."""

from unittest import mock

from ..ingestion import ingestion_listener as ingestion_listener_module
from ..ingestion import queue_utils
from ..ingestion.ingestion_listener import IngestionListener
from ..ingestion.queue_utils import IngestionQueueManager


QUEUE_URL = "https://sqs.example.test/ingestion"


def make_message(message_id, receipt_handle):
    return {
        "MessageId": message_id,
        "ReceiptHandle": receipt_handle,
        "Body": '{"uuid": "some-uuid"}',
    }


def make_queue_manager(responses, batch_size=1):
    """Build a queue manager without initializing boto3 or contacting SQS."""
    manager = object.__new__(IngestionQueueManager)
    manager.batch_size = batch_size
    manager.queue_url = QUEUE_URL
    manager.client = mock.Mock()
    manager.client.delete_message_batch.side_effect = responses
    return manager


def make_listener(manager):
    return IngestionListener(object(), _queue_manager=manager)


def test_delete_messages_builds_delete_entries_without_mutating_received_messages():
    message = make_message("logical-message-id", "current-receipt-handle")
    messages = [message]
    manager = make_queue_manager([{}])

    failed = manager.delete_messages(messages)

    assert failed == []
    assert messages == [message]
    assert messages[0] is message
    manager.client.delete_message_batch.assert_called_once_with(
        QueueUrl=QUEUE_URL,
        Entries=[{
            "Id": "logical-message-id",
            "ReceiptHandle": "current-receipt-handle",
        }],
    )
    entry = manager.client.delete_message_batch.call_args.kwargs["Entries"][0]
    assert entry["Id"] != entry["ReceiptHandle"]


def test_delete_messages_correlates_aws_failure_to_original_received_message():
    message = make_message("logical-message-id", "current-receipt-handle")
    aws_failure = {
        "Id": "logical-message-id",
        "SenderFault": False,
        "Code": "InternalError",
    }
    manager = make_queue_manager([{"Failed": [aws_failure]}])

    with mock.patch.object(queue_utils.log, "warning") as mock_warning:
        failed = manager.delete_messages([message])

    assert failed == [message]
    assert failed[0] is message
    assert failed[0]["ReceiptHandle"] == "current-receipt-handle"
    assert "MessageId" not in aws_failure
    assert "ReceiptHandle" not in aws_failure
    mock_warning.assert_called_once_with(
        "SQS delete_message_batch entry failed",
        batch_id="logical-message-id",
        sender_fault=False,
        code="InternalError",
        message=None,
    )


def test_listener_retries_with_original_receipt_handle_and_succeeds():
    message = make_message("logical-message-id", "current-receipt-handle")
    manager = make_queue_manager([
        {
            "Failed": [{
                "Id": "logical-message-id",
                "SenderFault": False,
                "Code": "InternalError",
                "Message": "transient failure",
            }],
        },
        {},
    ])

    make_listener(manager).delete_messages([message])

    assert manager.client.delete_message_batch.call_count == 2
    retry_entry = manager.client.delete_message_batch.call_args_list[1].kwargs["Entries"][0]
    assert retry_entry == {
        "Id": "logical-message-id",
        "ReceiptHandle": "current-receipt-handle",
    }


def test_listener_retries_only_failed_message_from_multi_message_batch():
    messages = [
        make_message("message-a", "receipt-a"),
        make_message("message-b", "receipt-b"),
        make_message("message-c", "receipt-c"),
    ]
    manager = make_queue_manager([
        {
            "Successful": [{"Id": "message-a"}, {"Id": "message-c"}],
            "Failed": [{
                "Id": "message-b",
                "SenderFault": False,
                "Code": "InternalError",
            }],
        },
        {"Successful": [{"Id": "message-b"}]},
    ], batch_size=10)

    make_listener(manager).delete_messages(messages)

    assert manager.client.delete_message_batch.call_count == 2
    initial_entries = manager.client.delete_message_batch.call_args_list[0].kwargs["Entries"]
    retry_entries = manager.client.delete_message_batch.call_args_list[1].kwargs["Entries"]
    assert initial_entries == [
        {"Id": "message-a", "ReceiptHandle": "receipt-a"},
        {"Id": "message-b", "ReceiptHandle": "receipt-b"},
        {"Id": "message-c", "ReceiptHandle": "receipt-c"},
    ]
    assert retry_entries == [{"Id": "message-b", "ReceiptHandle": "receipt-b"}]


def test_listener_stops_after_three_delete_retries():
    message = make_message("logical-message-id", "current-receipt-handle")
    persistent_failure = {
        "Failed": [{
            "Id": "logical-message-id",
            "SenderFault": False,
            "Code": "InternalError",
            "Message": "still failing",
        }],
    }
    manager = make_queue_manager([persistent_failure] * 4)

    with mock.patch.object(ingestion_listener_module.log, "error") as mock_error:
        make_listener(manager).delete_messages([message])

    assert manager.client.delete_message_batch.call_count == 4
    for call in manager.client.delete_message_batch.call_args_list:
        assert call.kwargs["Entries"] == [{
            "Id": "logical-message-id",
            "ReceiptHandle": "current-receipt-handle",
        }]
    mock_error.assert_called_once_with(
        "Failed to delete messages from SQS after 3 retries: %s" % [message]
    )
