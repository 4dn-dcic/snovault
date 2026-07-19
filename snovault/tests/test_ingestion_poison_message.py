"""
Offline (no live SQS/DB/ES) regression tests for the ingestion listener's poison-message
isolation (audit finding T8).

Before the fix, `IngestionListener.run()` called the message handler with no exception guard.
A single malformed SQS message -- non-JSON body, or a body missing the required ``uuid`` --
raised out of the per-message loop (`IngestionMessage.__init__` parses the body BEFORE any
handler runs). In composite/WSGI mode that tripped `ErrorHandlingThread`'s `SIGINT`
self-restart; combined with the ingestion queue's 3h visibility timeout and no dead-letter
queue, the poison message redelivered and crashed the listener every 3h indefinitely, blocking
ALL submissions rather than just the bad one.

The fix routes every message through `IngestionListener.handle_one_message`, which isolates
parse and handler failures and returns a disposition telling `run()` whether to delete
(ack) the message, leave it for redelivery, or discard it as poison. These tests assert that
contract without any live services.
"""

from snovault.ingestion import ingestion_listener as il
from snovault.ingestion.ingestion_listener import IngestionListener, summarize_message_for_log


def _raw(body, **extra):
    """ Build a raw SQS-style message dict. `body` is used verbatim as the ``Body`` string. """
    msg = {'MessageId': 'msg-abc-123', 'MD5OfBody': 'deadbeef', 'ReceiptHandle': 'rh-xyz', 'Body': body}
    msg.update(extra)
    return msg


def _listener():
    """ A bare IngestionListener with no __init__ side effects (no vapp/registry/SQS). Only
        handle_one_message is exercised, which needs no instance state of its own. """
    return object.__new__(IngestionListener)


# --- poison (unparseable) messages: real IngestionMessage parsing must be guarded ---------

def test_handle_one_message_invalid_json_is_poison():
    listener = _listener()
    disposition = listener.handle_one_message(_raw('this is not json{{'))
    assert disposition == IngestionListener.MESSAGE_POISON


def test_handle_one_message_missing_uuid_is_poison():
    listener = _listener()
    # Valid JSON, but no "uuid" key -> IngestionMessage.__init__ raises KeyError.
    disposition = listener.handle_one_message(_raw('{"ingestion_type": "vcf"}'))
    assert disposition == IngestionListener.MESSAGE_POISON


def test_handle_one_message_missing_body_is_poison():
    listener = _listener()
    # No "Body" key at all -> IngestionMessage.__init__ raises KeyError on raw_message["Body"].
    disposition = listener.handle_one_message({'MessageId': 'msg-no-body'})
    assert disposition == IngestionListener.MESSAGE_POISON


def test_handle_one_message_never_raises_on_poison():
    """ The whole point: a poison message must NOT propagate an exception out of
        handle_one_message (which is what previously crash-looped the listener). """
    listener = _listener()
    for bad in ['', '{', 'null', '[]', '{"no_uuid": 1}']:
        # Must return a disposition rather than raise.
        assert listener.handle_one_message(_raw(bad)) in (
            IngestionListener.MESSAGE_POISON, IngestionListener.MESSAGE_HANDLED,
            IngestionListener.MESSAGE_UNHANDLED, IngestionListener.MESSAGE_DEFERRED,
        )


# --- valid messages: disposition follows the handler's outcome ---------------------------

def _valid_body():
    return '{"uuid": "1111-2222", "ingestion_type": "vcf"}'


def test_handle_one_message_handler_success_is_handled(monkeypatch):
    listener = _listener()
    monkeypatch.setattr(il, 'call_ingestion_message_handler', lambda message, self_: True)
    assert listener.handle_one_message(_raw(_valid_body())) == IngestionListener.MESSAGE_HANDLED


def test_handle_one_message_handler_falsy_is_unhandled(monkeypatch):
    listener = _listener()
    monkeypatch.setattr(il, 'call_ingestion_message_handler', lambda message, self_: False)
    # A handler that runs but declines the message keeps the prior fallback-ack behavior.
    assert listener.handle_one_message(_raw(_valid_body())) == IngestionListener.MESSAGE_UNHANDLED


def test_handle_one_message_handler_raises_is_deferred(monkeypatch):
    listener = _listener()

    def boom(message, self_):
        raise RuntimeError("handler blew up")

    monkeypatch.setattr(il, 'call_ingestion_message_handler', boom)
    # A valid message whose handler raised must NOT be acked; it is deferred for redelivery,
    # and the exception must be swallowed (no crash).
    assert listener.handle_one_message(_raw(_valid_body())) == IngestionListener.MESSAGE_DEFERRED


# --- safe diagnostic logging identity ----------------------------------------------------

def test_summarize_message_for_log_includes_id_excludes_body():
    summary = summarize_message_for_log(_raw('{"uuid": "secret-sensitive-payload"}'))
    assert 'msg-abc-123' in summary          # MessageId is logged
    assert 'deadbeef' in summary             # MD5OfBody is logged
    assert 'secret-sensitive-payload' not in summary  # the Body is NEVER logged


def test_summarize_message_for_log_handles_missing_fields_and_non_dict():
    assert '<unknown>' in summarize_message_for_log({})
    assert summarize_message_for_log('not-a-dict').startswith('<non-dict message')
