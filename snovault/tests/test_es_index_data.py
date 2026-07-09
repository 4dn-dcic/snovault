"""Unit tests for es_index_data.run's early-exit behavior (Finding 13):

`run()` used to unconditionally POST to /index INDEXING_RUN_ITERATIONS (100)
times even after the queue had drained, burning ~6s of long-polling per wasted
iteration. It should now break out once the queue is empty and the response
reports no indexing work, requiring two consecutive such iterations to guard
against SQS's approximate/eventually-consistent message counts.

webtest.TestApp is mocked out entirely so no real WSGI app / DB / ES is
needed - only the response.json shape and registry[INDEXER_QUEUE] interface
are exercised.
"""
from unittest import mock

from ..commands.es_index_data import run, INDEXING_RUN_ITERATIONS


def make_app(queue_is_empty_side_effect):
    indexer_queue = mock.MagicMock()
    indexer_queue.queue_is_empty.side_effect = queue_is_empty_side_effect
    app = mock.MagicMock()
    app.registry = {'indexer_queue': indexer_queue}
    return app, indexer_queue


def make_testapp_with_indexing_counts(counts):
    responses = []
    for count in counts:
        response = mock.MagicMock()
        response.json = {'indexing_count': count}
        responses.append(response)
    testapp = mock.MagicMock()
    testapp.post_json.side_effect = responses
    return testapp


def test_run_breaks_after_two_consecutive_empty_iterations():
    # every iteration after the first reports an empty queue and 0 indexed
    counts = [5, 0, 0] + [0] * INDEXING_RUN_ITERATIONS
    testapp = make_testapp_with_indexing_counts(counts)
    app, indexer_queue = make_app(queue_is_empty_side_effect=lambda secondary_only: True)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app)

    # 1st iteration: indexing_count=5 (not empty) -> consecutive_empty resets to 0
    # 2nd iteration: indexing_count=0 and queue empty -> consecutive_empty=1
    # 3rd iteration: indexing_count=0 and queue empty -> consecutive_empty=2 -> break
    assert testapp.post_json.call_count == 3


def test_run_uses_full_iteration_cap_when_queue_never_empties():
    counts = [1] * INDEXING_RUN_ITERATIONS
    testapp = make_testapp_with_indexing_counts(counts)
    app, indexer_queue = make_app(queue_is_empty_side_effect=lambda secondary_only: False)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app)

    assert testapp.post_json.call_count == INDEXING_RUN_ITERATIONS


def test_run_with_uuids_posts_once_regardless_of_queue_state():
    testapp = make_testapp_with_indexing_counts([1])
    app, indexer_queue = make_app(queue_is_empty_side_effect=lambda secondary_only: True)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app, uuids=['abc-123'])

    assert testapp.post_json.call_count == 1
    indexer_queue.queue_is_empty.assert_not_called()
