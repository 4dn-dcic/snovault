import logging
import json
from unittest import mock

import structlog

from snovault import stats
from snovault.elasticsearch import indexer as indexer_module
from snovault.elasticsearch.indexer import Indexer
from snovault.elasticsearch.interfaces import ELASTIC_SEARCH, INDEXER_QUEUE
from snovault.interfaces import STORAGE


class _RecordingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _configured_logger(name, level):
    """Return a real structlog logger backed by a controllable stdlib threshold."""
    handler = _RecordingHandler()
    logger = logging.getLogger(name)
    logger.handlers[:] = [handler]
    logger.propagate = False
    logger.setLevel(level)
    bound_logger = structlog.stdlib.BoundLogger(
        logger,
        processors=[structlog.stdlib.filter_by_level, structlog.processors.KeyValueRenderer()],
        context={},
    )
    return logger, handler, bound_logger


def _event_records(handler, event):
    return [record for record in handler.records if "event='%s'" % event in record.getMessage()]


class _Registry(dict):
    def __init__(self, es, queue=None):
        super().__init__({ELASTIC_SEARCH: es, INDEXER_QUEUE: queue or object()})
        self.settings = {'indexer.namespace': 'test-'}


class _Storage:
    class write:
        @staticmethod
        def get_max_sid():
            return 1


class _Queue:
    queue_targets = ('primary', 'secondary', 'deferred')
    delete_batch_size = 10

    def __init__(self, message):
        self.message = message
        self.receive_count = 0
        self.sent = []
        self.deleted = []

    def receive_messages(self, target_queue):
        if target_queue == 'primary' and self.receive_count == 0:
            self.receive_count += 1
            return [self.message]
        return []

    def send_messages(self, messages, target_queue):
        self.sent.append((messages, target_queue))

    def delete_messages(self, messages, target_queue):
        self.deleted.append((messages, target_queue))


class _Response:
    def __init__(self):
        self.headers = {}


class _Request:
    def __init__(self, registry):
        self.registry = registry
        self.params = {}
        self.path = '/index'
        self.query_string = ''
        self.host = 'localhost'
        self.environ = {}
        self.embed_calls = []

    def embed(self, path, as_user):
        self.embed_calls.append((path, as_user))
        return {
            'item_type': 'Thing',
            'sid': 1,
            'indexing_stats': {},
            'rev_linked_to_me': [],
        }


class _ES:
    def __init__(self, error=None):
        self.error = error
        self.index_calls = []

    def index(self, **kwargs):
        self.index_calls.append(kwargs)
        if self.error:
            raise self.error


def _run_stats_tween(request):
    tween = stats.stats_tween_factory(lambda _request: _Response(), request.registry)
    return tween(request)


def test_request_timings_is_quiet_at_info_but_available_at_debug(monkeypatch):
    logger, handler, bound_logger = _configured_logger(
        'snovault.tests.request_timings', logging.INFO
    )
    monkeypatch.setattr(stats, 'log', bound_logger)

    request = _Request(_Registry(_ES()))
    response = _run_stats_tween(request)

    assert not _event_records(handler, 'Request timings')
    assert 'wsgi_time=' in response.headers['X-Stats']

    logger.setLevel(logging.DEBUG)
    _run_stats_tween(_Request(_Registry(_ES())))
    debug_records = _event_records(handler, 'Request timings')
    assert len(debug_records) == 1
    assert debug_records[0].levelno == logging.DEBUG


def test_invalid_max_sid_is_a_quiet_safe_defer_at_info(monkeypatch):
    es = _ES()
    registry = _Registry(es)
    request = _Request(registry)
    indexer = Indexer(registry)
    logger, handler, bound_logger = _configured_logger(
        'snovault.tests.invalid_max_sid', logging.INFO
    )
    monkeypatch.setattr(indexer_module, 'log', bound_logger)

    result = indexer.update_object(request, 'uuid-1', sid=2, max_sid=1)

    assert result == {'error_message': 'defer_resend'}
    assert request.embed_calls == []
    assert es.index_calls == []
    assert not _event_records(handler, 'Invalid max sid. Resending...')

    logger.setLevel(logging.DEBUG)
    indexer.update_object(_Request(registry), 'uuid-1', sid=2, max_sid=1)
    debug_records = _event_records(handler, 'Invalid max sid. Resending...')
    assert len(debug_records) == 1
    assert debug_records[0].levelno == logging.DEBUG


def test_invalid_max_sid_requeues_the_message_without_indexing(monkeypatch):
    message_body = {
        'uuid': 'uuid-1',
        'sid': 2,
        'strict': False,
        'timestamp': '2026-01-01T00:00:00',
    }
    message = {'Body': json.dumps(message_body)}
    queue = _Queue(message)
    es = _ES()
    registry = _Registry(es, queue=queue)
    registry[STORAGE] = _Storage()
    indexer = Indexer(registry)
    _, handler, bound_logger = _configured_logger(
        'snovault.tests.invalid_max_sid_queue', logging.INFO
    )
    monkeypatch.setattr(indexer_module, 'log', bound_logger)

    errors, deferred = indexer.update_objects_queue(_Request(registry), [0])

    assert errors == []
    assert deferred is True
    assert queue.sent == [([message_body], 'primary')]
    assert queue.deleted == [([message], 'primary')]
    assert es.index_calls == []
    assert not _event_records(handler, 'Invalid max sid. Resending...')


def test_actionable_indexing_failure_remains_error(monkeypatch):
    es = _ES(error=RuntimeError('elasticsearch unavailable'))
    registry = _Registry(es)
    indexer = Indexer(registry)
    _, handler, bound_logger = _configured_logger(
        'snovault.tests.actionable_indexing_failure', logging.INFO
    )

    with mock.patch.object(indexer_module, 'log', bound_logger), \
            mock.patch.object(indexer_module.time, 'sleep'):
        result = indexer.update_object(_Request(registry), 'uuid-1', sid=1, max_sid=1)

    assert result['error_message'] == "RuntimeError('elasticsearch unavailable')"
    error_records = _event_records(handler, 'Error indexing')
    assert len(error_records) == 1
    assert error_records[0].levelno == logging.ERROR
