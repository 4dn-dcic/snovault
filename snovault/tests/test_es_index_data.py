"""Unit tests for es_index_data.run's early-exit behavior (Finding 13):

`run()` used to unconditionally POST to /index INDEXING_RUN_ITERATIONS (100)
times even after the queue had drained, burning ~6s of long-polling per wasted
iteration. It should now break out once the queue is empty (including
in-flight/invisible retryable messages - see
test_run_does_not_break_while_inflight_messages_remain) and the response
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
    # every iteration after the first reports an empty queue (including no in-flight
    # work) and 0 indexed
    counts = [5, 0, 0] + [0] * INDEXING_RUN_ITERATIONS
    testapp = make_testapp_with_indexing_counts(counts)
    app, indexer_queue = make_app(
        queue_is_empty_side_effect=lambda secondary_only, include_inflight: True)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app)

    # 1st iteration: indexing_count=5 (not empty) -> consecutive_empty resets to 0
    # 2nd iteration: indexing_count=0 and queue empty -> consecutive_empty=1
    # 3rd iteration: indexing_count=0 and queue empty -> consecutive_empty=2 -> break
    assert testapp.post_json.call_count == 3


def test_run_uses_full_iteration_cap_when_queue_never_empties():
    counts = [1] * INDEXING_RUN_ITERATIONS
    testapp = make_testapp_with_indexing_counts(counts)
    app, indexer_queue = make_app(
        queue_is_empty_side_effect=lambda secondary_only, include_inflight: False)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app)

    assert testapp.post_json.call_count == INDEXING_RUN_ITERATIONS


def test_run_checks_queue_emptiness_including_inflight_messages():
    """ Regression test: queue_is_empty defaults include_inflight=False, which only
    reflects visible primary/secondary/DLQ counts. A retryable indexing failure puts a
    message in flight (invisible) for up to its configured visibility timeout
    (indexer.py's replace_messages uses vis_timeout=180) - if the emptiness check omits
    in-flight messages, the command can stop while that message is still scheduled to
    reappear and be retried, leaving it unindexed. run() must pass
    include_inflight=True. """
    testapp = make_testapp_with_indexing_counts([0, 0, 0])
    app, indexer_queue = make_app(
        queue_is_empty_side_effect=lambda secondary_only, include_inflight: True)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app)

    for call in indexer_queue.queue_is_empty.call_args_list:
        assert call.kwargs == {'secondary_only': False, 'include_inflight': True}


def test_run_does_not_break_while_inflight_messages_remain():
    """ Models a queue whose visible counts are empty throughout (so the old,
    include_inflight=False check would consider every iteration "empty"), but which
    still reports in-flight (invisible/retryable) work for the first several
    iterations. The command must keep polling until the in-flight work clears, rather
    than exiting after two consecutive visible-empty iterations. """
    inflight_clears_after = 4  # first 4 calls report in-flight work remaining
    calls_made = {'n': 0}

    def queue_is_empty_side_effect(secondary_only, include_inflight):
        calls_made['n'] += 1
        assert secondary_only is False
        assert include_inflight is True
        # while in-flight work remains, the queue is not considered empty at all
        return calls_made['n'] > inflight_clears_after

    # indexing_count is 0 throughout - only in-flight state should gate the break
    counts = [0] * (inflight_clears_after + 2)
    testapp = make_testapp_with_indexing_counts(counts)
    app, indexer_queue = make_app(queue_is_empty_side_effect=queue_is_empty_side_effect)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app)

    # iterations 1-4: queue_is_empty() is False (in-flight remains) -> consecutive_empty
    #   stays 0 each time
    # iteration 5: queue_is_empty() becomes True -> consecutive_empty=1
    # iteration 6: queue_is_empty() True again -> consecutive_empty=2 -> break
    assert testapp.post_json.call_count == inflight_clears_after + 2


def test_run_with_uuids_posts_once_regardless_of_queue_state():
    testapp = make_testapp_with_indexing_counts([1])
    app, indexer_queue = make_app(
        queue_is_empty_side_effect=lambda secondary_only, include_inflight: True)

    with mock.patch('snovault.commands.es_index_data.webtest.TestApp', return_value=testapp):
        run(app, uuids=['abc-123'])

    assert testapp.post_json.call_count == 1
    indexer_queue.queue_is_empty.assert_not_called()
