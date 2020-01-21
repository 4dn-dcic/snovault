""" Test full indexing setup

The fixtures in this module setup a full system with postgresql and
elasticsearch running as subprocesses.
Does not include data dependent tests
"""

import pytest
import time
import json
import uuid
import yaml
import os
from datetime import datetime
from snovault.elasticsearch.interfaces import (
    ELASTIC_SEARCH,
    INDEXER_QUEUE,
    INDEXER_QUEUE_MIRROR,
)
from snovault import (
    COLLECTIONS,
    TYPES,
    DBSESSION,
    STORAGE
)
from snovault.commands.es_index_data import run as run_index_data
from snovault.elasticsearch import create_mapping, indexer_utils
from elasticsearch.exceptions import NotFoundError
from snovault.elasticsearch.create_mapping import (
    run,
    type_mapping,
    create_mapping_by_type,
    build_index_record,
    check_if_index_exists,
    confirm_mapping,
    compare_against_existing_mapping
)
from snovault.elasticsearch.indexer import (
    check_sid,
    SidException
)
from pyramid.paster import get_appsettings
from snovault.tests.pyramidfixtures import dummy_request
from snovault.tests.toolfixtures import registry, root, elasticsearch

pytestmark = [pytest.mark.indexing]
TEST_COLL = '/testing-post-put-patch-sno/'
TEST_TYPE = 'testing_post_put_patch_sno'  # use one collection for testing

# we just need single shard for these tests
create_mapping.NUM_SHARDS = 1


@pytest.fixture(scope='session')
def app_settings(wsgi_server_host_port, elasticsearch_server, postgresql_server, aws_auth):
    from .testappfixtures import _app_settings
    settings = _app_settings.copy()
    settings['create_tables'] = True
    settings['elasticsearch.server'] = elasticsearch_server
    settings['sqlalchemy.url'] = postgresql_server
    settings['collection_datastore'] = 'elasticsearch'
    settings['item_datastore'] = 'elasticsearch'
    settings['indexer'] = True
    settings['indexer.namespace'] = os.environ.get('TRAVIS_JOB_ID', '')

    # use aws auth to access elasticsearch
    if aws_auth:
        settings['elasticsearch.aws_auth'] = aws_auth

    return settings


@pytest.yield_fixture(scope='session', params=[True, False])
def app(app_settings, request):
    from snovault import main
    if request.param: # run tests both with and without mpindexer
        app_settings['mpindexer'] = True
    app = main({}, **app_settings)

    yield app

    DBSession = app.registry[DBSESSION]
    # Dispose connections so postgres can tear down.
    DBSession.bind.pool.dispose()


@pytest.yield_fixture(autouse=True)
def setup_and_teardown(app):
    """
    Run create mapping and purge queue before tests and clear out the
    DB tables after the test
    """
    import transaction
    from sqlalchemy import MetaData
    from zope.sqlalchemy import mark_changed
    # BEFORE THE TEST - just run CM for the TEST_TYPE by default
    create_mapping.run(app, collections=[TEST_TYPE], skip_indexing=True)
    app.registry[INDEXER_QUEUE].clear_queue()

    yield  # run the test

    # AFTER THE TEST
    session = app.registry[DBSESSION]
    connection = session.connection().connect()
    meta = MetaData(bind=session.connection(), reflect=True)
    for table in meta.sorted_tables:
        print('Clear table %s' % table)
        print('Count before -->', str(connection.scalar("SELECT COUNT(*) FROM %s" % table)))
        connection.execute(table.delete())
        print('Count after -->', str(connection.scalar("SELECT COUNT(*) FROM %s" % table)), '\n')
    session.flush()
    mark_changed(session())
    transaction.commit()


def test_indexer_namespacing(app, testapp, indexer_testapp):
    """
    Tests that namespacing indexes works as expected. This test has no real
    effect on local but does on Travis
    """
    import os
    jid = os.environ.get('TRAVIS_JOB_ID')
    idx = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    testapp.post_json(TEST_COLL, {'required': ''})
    indexer_testapp.post_json('/index', {'record': True})
    es = app.registry[ELASTIC_SEARCH]
    assert idx in es.indices.get(index=idx)
    if jid:
        assert jid in idx
    app.registry.settings['indexer.namespace'] = '' # unset namespace, check raw is given
    raw_idx = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    star_idx = indexer_utils.get_namespaced_index(app.registry, '*') # registry should work as well
    assert raw_idx == TEST_TYPE
    assert star_idx == '*'
    app.registry.settings['indexer.namespace'] = jid # reset jid


@pytest.mark.es
def test_indexer_queue_adds_telemetry_id(app):
    indexer_queue = app.registry[INDEXER_QUEUE]
    indexer_queue.clear_queue()
    test_message = 'abc123'
    telem = 'test_telemetry_id'
    to_index, failed = indexer_queue.add_uuids(app.registry, [test_message], strict=True,
                                               telemetry_id=telem)
    assert to_index == [test_message]
    assert not failed
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    msg_body = json.loads(received[0]['Body'])
    assert isinstance(msg_body, dict)
    assert msg_body['uuid'] == test_message
    assert msg_body['strict'] is True
    assert msg_body['telemetry_id'] == telem

    # finally, delete
    indexer_queue.delete_messages(received)


@pytest.mark.es
@pytest.mark.flaky
def test_indexer_queue(app):
    indexer_queue_mirror = app.registry[INDEXER_QUEUE_MIRROR]
    # this is only set up for webprod/webprod2
    assert indexer_queue_mirror is None

    indexer_queue = app.registry[INDEXER_QUEUE]
    indexer_queue.clear_queue()
    # unittesting the QueueManager
    assert indexer_queue.queue_url is not None
    assert indexer_queue.dlq_url is not None
    assert indexer_queue.second_queue_url is not None
    test_message = 'abc123'
    to_index, failed = indexer_queue.add_uuids(app.registry, [test_message], strict=True)
    assert to_index == [test_message]
    assert not failed
    time.sleep(5) # make sure all msgs are received
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    msg_body = json.loads(received[0]['Body'])
    assert isinstance(msg_body, dict)
    assert msg_body['uuid'] == test_message
    assert msg_body['strict'] is True
    # try to receive again (should be empty)
    received_2 = indexer_queue.receive_messages()
    assert len(received_2) == 0
    # replace into queue
    indexer_queue.replace_messages(received, vis_timeout=0)
    # replace sets vis timeout to 5 seconds... so wait a bit first
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    # finally, delete
    indexer_queue.delete_messages(received)
    # make sure the queue eventually sorts itself out
    tries_left = 5
    while tries_left > 0:
        msg_count = indexer_queue.number_of_messages()
        if (msg_count['primary_waiting'] == 0 and
            msg_count['primary_inflight'] == 0):
            break
        tries_left -= 0
        time.sleep(3)
    assert tries_left > 0


@pytest.mark.flaky
def test_queue_indexing_telemetry_id(app, testapp):
    indexer_queue = app.registry[INDEXER_QUEUE]
    ordered_queue_targets = [targ for targ in indexer_queue.queue_targets]
    assert ordered_queue_targets == ['primary', 'secondary', 'dlq']
    indexer_queue.clear_queue()
    testapp.post_json(TEST_COLL + '?telemetry_id=test_telem', {'required': ''})
    time.sleep(2)
    secondary_body = {
        'uuids': ['12345', '23456'],
        'strict': True,
        'target_queue': 'secondary',
    }
    testapp.post_json('/queue_indexing?telemetry_id=test_telem', secondary_body)
    time.sleep(2)
    # make sure the queue eventually sorts itself out
    tries_left = 5
    while tries_left > 0:
        msg_count = indexer_queue.number_of_messages()
        if (msg_count['primary_waiting'] == 1 and
            msg_count['secondary_waiting'] == 2):
            break
        tries_left -= 1
        time.sleep(3)
    assert tries_left > 0
    # delete the messages
    for target in indexer_queue.queue_targets:
        if 'dlq' in target:  # skip if dlq
            continue
        received = indexer_queue.receive_messages(target_queue=target)
        assert len(received) > 0
        for msg in received:
            # ensure we are passing telemetry_id through queue_indexing
            print(msg)
            msg_body = json.loads(msg['Body'])
            assert msg_body['telemetry_id'] == 'test_telem'
        indexer_queue.delete_messages(received, target_queue=target)
    # make sure the queue eventually sorts itself out
    tries_left = 5
    while tries_left > 0:
        msg_count = indexer_queue.number_of_messages()
        if (msg_count['primary_waiting'] == 0 and
            msg_count['secondary_waiting'] == 0):
            break
        tries_left -= 1
        time.sleep(3)
    assert tries_left > 0


@pytest.mark.flaky
def test_queue_indexing_after_post_patch(app, testapp):
    # make sure that the right stuff gets queued up on a post or a patch
    indexer_queue = app.registry[INDEXER_QUEUE]
    # POST
    post_res = testapp.post_json(TEST_COLL, {'required': ''})
    post_uuid = post_res.json['@graph'][0]['uuid']
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    msg_body = json.loads(received[0]['Body'])
    assert isinstance(msg_body, dict)
    assert msg_body['uuid'] == post_uuid
    assert msg_body['strict'] is False
    assert msg_body['method'] == 'POST'
    assert 'timestamp' in msg_body
    assert 'sid' in msg_body
    post_sid = msg_body['sid']
    indexer_queue.delete_messages(received)
    time.sleep(3)
    # PATCH
    testapp.patch_json(TEST_COLL + post_uuid, {'required': 'meh'})
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    msg_body = json.loads(received[0]['Body'])
    assert isinstance(msg_body, dict)
    assert msg_body['uuid'] == post_uuid
    assert msg_body['strict'] is False
    assert msg_body['method'] == 'PATCH'
    assert 'timestamp' in msg_body
    assert 'sid' in msg_body
    assert msg_body['sid'] > post_sid
    indexer_queue.delete_messages(received)


@pytest.mark.flaky
def test_dlq_to_primary(app, anontestapp, indexer_testapp):
    """
    Tests the dlq_to_primary route
    Post some messages to the DLQ, hit the route, receive
    those same messages from the primary queue
    """
    indexer_queue = app.registry[INDEXER_QUEUE]
    indexer_queue.clear_queue()
    test_bodies = ["destined for primary!", "i am also destined!"]
    success, failed = indexer_queue.add_uuids(app.registry, test_bodies,
                                        target_queue='dlq')
    assert not failed
    assert len(success) == 2
    res = indexer_testapp.get('/dlq_to_primary').json
    assert res['number_migrated'] == 2
    assert res['number_failed'] == 0
    msgs = indexer_queue.receive_messages()  # receive from primary
    assert len(msgs) == 2
    for msg in msgs:
        body = msg["Body"]
        assert body in test_bodies

    # hit route with no messages, should see 0 migrated
    res = indexer_testapp.get('/dlq_to_primary').json
    assert res['number_migrated'] == 0
    assert res['number_failed'] == 0

    # hit route from unauthenticated testapp, should fail
    anontestapp.get('/dlq_to_primary', status=403)


@pytest.mark.flaky
def test_indexing_simple(app, testapp, indexer_testapp):
    # First post a single item so that subsequent indexing is incremental
    testapp.post_json(TEST_COLL, {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    assert res.json['indexing_status'] == 'finished'
    assert res.json['errors'] == []
    res = testapp.post_json(TEST_COLL, {'required': ''})
    uuid = res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    res = testapp.get('/search/?type=%s' % TEST_TYPE).follow()
    uuids = [indv_res['uuid'] for indv_res in res.json['@graph']]
    count = 0
    while uuid not in uuids and count < 20:
        time.sleep(1)
        res = testapp.get('/search/?type=%s' % TEST_TYPE)
        uuids = [indv_res['uuid'] for indv_res in res.json['@graph']]
        count += 1
    assert res.json['total'] >= 2
    assert uuid in uuids

    es = app.registry[ELASTIC_SEARCH]
    namespaced_index = indexer_utils.get_namespaced_index(app, 'indexing')
    indexing_doc = es.get(index=namespaced_index, doc_type='indexing', id='latest_indexing')
    indexing_source = indexing_doc['_source']
    assert 'indexing_finished' in indexing_source
    assert 'indexing_content' in indexing_source
    assert indexing_source['indexing_status'] == 'finished'
    assert indexing_source['indexing_count'] > 0
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    testing_ppp_mappings = es.indices.get_mapping(index=namespaced_index)[namespaced_index]
    assert 'mappings' in testing_ppp_mappings
    testing_ppp_settings = es.indices.get_settings(index=namespaced_index)[namespaced_index]
    assert 'settings' in testing_ppp_settings
    # ensure we only have 1 shard for tests
    assert testing_ppp_settings['settings']['index']['number_of_shards'] == '1'


def test_indexing_logging(app, testapp, indexer_testapp, capfd):
    """
    This test is meant to do 2 things.
    - Test correct logging contents to stdout/stderr using capfd fixture
    - Test that logs are correctly shipped to Elasticsearch when `es_server`
      setting (along with `in_prod=True`) and used in set_logging
    HOWEVER, logging cannot be reset to NOT use ES (which is what is currently
    desired), so for the time being, the second part of the test is disabled
    """
    ### PART OF ES LOGGING TEST (DISABLED)
    # import logging
    # import structlog
    # from dcicutils.log_utils import calculate_log_index, set_logging
    # es = app.registry[ELASTIC_SEARCH]
    # set_logging(es_server=app.registry.settings.get('elasticsearch.server'),
    #             in_prod=True, level=logging.INFO, log_name='snovault')
    # logger = structlog.get_logger('snovault')
    # logger.info('testing testing')
    # log_index_name = calculate_log_index()
    # exists = check_if_index_exists(es, log_index_name)
    # assert exists

    # Can't get log msgs when MPIndexer is running, so skip in this case
    if app.registry.settings['mpindexer'] == True:
        return

    # index an item and make sure logging to stdout occurs
    post_res = testapp.post_json(TEST_COLL, {'required': ''})
    post_uuid = post_res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    assert res.json['indexing_status'] == 'finished'
    check_logs = capfd.readouterr()[-1].split('\n')
    item_idx_record = None
    for record in check_logs:
        if not record:
            continue
        try:
            proc_record = yaml.load('{' + record.strip().split('{', 1)[1])
        except:
            continue
        if not isinstance(proc_record, dict):
            continue
        if proc_record.get('item_uuid') == post_uuid:
            item_idx_record = proc_record
            break
    assert item_idx_record is not None
    assert item_idx_record['collection'] == TEST_TYPE
    assert 'uo_start_time' in item_idx_record
    assert isinstance(item_idx_record['sid'], int)
    assert 'log_uuid' in item_idx_record
    assert 'level' in item_idx_record

    # On local, the below line works
    # but on travis it fails
    # assert item_idx_record['url_path'] == '/index'

    ### PART OF ES LOGGING TEST (DISABLED)
    # # now get the log from ES
    # log_uuid = item_idx_record['log_uuid']
    # log_doc = es.get(index=log_index_name, doc_type='log', id=log_uuid)
    # log_source = log_doc['_source']
    # assert log_source['item_uuid'] == post_uuid
    # assert log_source['url_path'] == '/index'
    # assert 'level' in log_source
    #
    # # remove the log index and reset logging
    # es.indices.delete(index=log_index_name)
    # exists = check_if_index_exists(es, log_index_name)
    # assert not exists


@pytest.mark.flaky
def test_indexing_queue_records(app, testapp, indexer_testapp):
    """
    Do a full test using different forms of create mapping and both sync
    and queued indexing.
    """
    es = app.registry[ELASTIC_SEARCH]
    indexer_queue = app.registry[INDEXER_QUEUE]
    namespaced_indexing = indexer_utils.get_namespaced_index(app, 'indexing')
    # first clear out the indexing records
    es.indices.delete(index=namespaced_indexing)
    # no documents added yet
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert doc_count == 0
    # post a document but do not yet index
    res = testapp.post_json(TEST_COLL, {'required': ''})
    doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert doc_count == 0
    # indexing record should not yet exist (expect error)
    with pytest.raises(NotFoundError):
        es.get(index=namespaced_indexing, doc_type='indexing', id='latest_indexing')
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    assert res.json['indexing_content']['type'] == 'queue'
    time.sleep(4)
    doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert doc_count == 1
    # make sure latest_indexing doc matches
    indexing_doc = es.get(index=namespaced_indexing, doc_type='indexing', id='latest_indexing')
    indexing_doc_source = indexing_doc.get('_source', {})
    # cannot always rely on this number with the shared test ES setup
    assert indexing_doc_source.get('indexing_count') > 0
    # test timing in indexing doc
    assert indexing_doc_source.get('indexing_elapsed')
    indexing_start = indexing_doc_source.get('indexing_started')
    indexing_end = indexing_doc_source.get('indexing_finished')
    assert indexing_start and indexing_end
    time_start =  datetime.strptime(indexing_start, '%Y-%m-%dT%H:%M:%S.%f')
    time_done = datetime.strptime(indexing_end, '%Y-%m-%dT%H:%M:%S.%f')
    assert time_start < time_done
    # get indexing record by start_time
    indexing_record = es.get(index=namespaced_indexing, doc_type='indexing', id=indexing_start)
    assert indexing_record.get('_source', {}).get('indexing_status') == 'finished'
    assert indexing_record.get('_source') == indexing_doc_source


@pytest.mark.flaky
def test_sync_and_queue_indexing(app, testapp, indexer_testapp):
    es = app.registry[ELASTIC_SEARCH]
    indexer_queue = app.registry[INDEXER_QUEUE]
    # clear queue before starting this one
    indexer_queue.clear_queue()
    # queued on post - total of one item queued
    res = testapp.post_json(TEST_COLL, {'required': ''})
    # synchronously index
    create_mapping.run(app, collections=[TEST_TYPE], sync_index=True)
    #time.sleep(6)
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    doc_count = tries = 0
    while(tries < 6):
        doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
        if doc_count != 0:
            break
        time.sleep(1)
        tries += 1
    assert doc_count == 1
    # post second item to database but do not index (don't load into es)
    # queued on post - total of two items queued
    res = testapp.post_json(TEST_COLL, {'required': ''})
    #time.sleep(2)
    doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    # doc_count has not yet updated
    assert doc_count == 1
    # clear the queue by indexing and then run create mapping to queue the all items
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 2
    create_mapping.run(app, collections=[TEST_TYPE])
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 2
    time.sleep(4)
    doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert doc_count == 2


@pytest.mark.flaky
def test_queue_indexing_with_linked(app, testapp, indexer_testapp, dummy_request):
    """
    Test a whole bunch of things here:
    - posting/patching invalidates rev linked items
    - check linked_uuids/rev_link_names/rev_linked_to_me fields in ES
    - test indexer_utils.find_uuids_for_indexing fxn
    - test check_es_and_cache_linked_sids & validate_es_content
    - test purge functionality before and after removing links to an item
    """
    import webtest
    from snovault import util
    from pyramid.traversal import traverse
    from snovault.tests.testing_views import TestingLinkSourceSno
    es = app.registry[ELASTIC_SEARCH]
    indexer_queue = app.registry[INDEXER_QUEUE]
    # first, run create mapping with the indices we will use
    create_mapping.run(
        app,
        collections=[TEST_TYPE, 'testing_link_target_sno', 'testing_link_source_sno'],
        skip_indexing=True
    )
    ppp_res = testapp.post_json(TEST_COLL, {'required': ''})
    ppp_uuid = ppp_res.json['@graph'][0]['uuid']
    target  = {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'}
    source = {
        'name': 'A',
        'target': '775795d3-4410-4114-836b-8eeecf1d0c2f',
        'ppp': ppp_uuid,
        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd',
        'status': 'current',
    }
    target_res = testapp.post_json('/testing-link-targets-sno/', target, status=201)
    res = indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    # wait for the first item to index
    namespaced_link_target = indexer_utils.get_namespaced_index(app, 'testing_link_target_sno')
    namespaced_link_source = indexer_utils.get_namespaced_index(app, 'testing_link_source_sno')
    namespaced_test_type = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    doc_count_target = es.count(index=namespaced_link_target, doc_type='testing_link_target_sno').get('count')
    doc_count_ppp = es.count(index=namespaced_test_type, doc_type=TEST_TYPE).get('count')
    tries = 0
    while (doc_count_target < 1 or doc_count_ppp < 1) and tries < 5:
        time.sleep(4)
        doc_count_target = es.count(index=namespaced_link_target, doc_type='testing_link_target_sno').get('count')
        doc_count_ppp = es.count(index=namespaced_test_type, doc_type=TEST_TYPE).get('count')
        tries += 1
    assert doc_count_target == 1
    assert doc_count_ppp == 1
    # indexing the source will also reindex the target and ppp, due to rev links
    source_res = testapp.post_json('/testing-link-sources-sno/', source, status=201)
    source_uuid = source_res.json['@graph'][0]['uuid']
    time.sleep(2)
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 2
    time.sleep(2)
    # wait for them to index
    doc_count = es.count(index=namespaced_link_source, doc_type='testing_link_source_sno').get('count')
    tries = 0
    while doc_count < 1 and tries < 5:
        time.sleep(4)
        doc_count = es.count(index=namespaced_link_source, doc_type='testing_link_source_sno').get('count')
    assert doc_count == 1
    # patching json will not queue the embedded ppp
    # the target will be indexed though, since it has a linkTo back to the source
    patch_source_name = 'ABC'
    testapp.patch_json('/testing-link-sources-sno/' + source_uuid, {'name': patch_source_name})
    time.sleep(2)
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 2

    time.sleep(3)
    # check some stuff on the es results for source and target
    es_source = es.get(index=namespaced_link_source, doc_type='testing_link_source_sno', id=source['uuid'])
    uuids_linked_emb = [link['uuid'] for link in es_source['_source']['linked_uuids_embedded']]
    uuids_linked_obj = [link['uuid'] for link in es_source['_source']['linked_uuids_object']]
    assert set(uuids_linked_emb) == {target['uuid'], source['uuid'], ppp_uuid}
    assert uuids_linked_obj == [source['uuid']]
    assert es_source['_source']['rev_link_names'] == {}
    assert es_source['_source']['rev_linked_to_me'] == [target['uuid']]

    es_target = es.get(index=namespaced_link_target, doc_type='testing_link_target_sno', id=target['uuid'])
    # just the source uuid itself in the linked uuids for the object view
    uuids_linked_emb2 = [link['uuid'] for link in es_target['_source']['linked_uuids_embedded']]
    uuids_linked_obj2 = [link['uuid'] for link in es_target['_source']['linked_uuids_object']]
    assert set(uuids_linked_emb2) == {target['uuid'], source['uuid']}
    assert uuids_linked_obj2 == [target['uuid']]
    assert es_target['_source']['rev_link_names'] == {'reverse': [source['uuid']]}
    assert es_target['_source']['rev_linked_to_me'] == []
    # this specific field was embedded from the rev link
    assert es_target['_source']['embedded']['reverse'][0]['name'] == patch_source_name

    # test find_uuids_for_indexing
    to_index = indexer_utils.find_uuids_for_indexing(app.registry, {target['uuid']})
    assert to_index == {target['uuid'], source['uuid']}
    to_index = indexer_utils.find_uuids_for_indexing(app.registry, {ppp_uuid})
    assert to_index == {ppp_uuid, source['uuid']}
    # this will return the target uuid, since it has an indexed rev link
    to_index = indexer_utils.find_uuids_for_indexing(app.registry, {source['uuid']})
    assert to_index == {target['uuid'], source['uuid']}
    # now use a made-up uuid; only result should be itself
    fake_uuid = str(uuid.uuid4())
    to_index = indexer_utils.find_uuids_for_indexing(app.registry, {fake_uuid})
    assert to_index == {fake_uuid}

    # test @@links functionality
    source_links_res = testapp.get('/' + source['uuid'] + '/@@links', status=200)
    linking_uuids = source_links_res.json.get('uuids_linking_to')
    assert linking_uuids and len(linking_uuids) == 1
    assert linking_uuids[0]['uuid'] == target['uuid']  # rev_link from target

    # test check_es_and_cache_linked_sids and validate_es_content
    # must get the context object through request traversal
    dummy_request.datastore = 'database'
    assert dummy_request._sid_cache == {}
    source_ctxt = traverse(dummy_request.root, source_res.json['@graph'][0]['@id'])['context']
    target_ctxt = traverse(dummy_request.root, target_res.json['@graph'][0]['@id'])['context']
    # first check frame=object for target
    tar_es_res_obj = util.check_es_and_cache_linked_sids(target_ctxt, dummy_request, 'object')
    assert tar_es_res_obj['uuid'] == target['uuid']
    assert set(uuids_linked_obj2) == set(dummy_request._sid_cache)
    # frame=embedded for source
    src_es_res_emb = util.check_es_and_cache_linked_sids(source_ctxt, dummy_request, 'embedded')
    assert src_es_res_emb['uuid'] == source['uuid']
    assert set(uuids_linked_emb) == set(dummy_request._sid_cache)
    # make everything in _sid_cache is present and up to date
    for rid in dummy_request._sid_cache:
        found_sid = dummy_request.registry[STORAGE].write.get_by_uuid(rid).sid
        assert dummy_request._sid_cache.get(rid) == found_sid
    # test validate_es_content with the correct sids and then an incorrect one
    valid = util.validate_es_content(source_ctxt, dummy_request, src_es_res_emb, 'embedded')
    assert valid is True

    # lastly, test purge_uuid and delete functionality
    with pytest.raises(webtest.AppError) as excinfo:
        del_res0 = testapp.delete_json('/' + source['uuid'] + '/?purge=True')
    assert 'Item status must equal deleted before purging' in str(excinfo.value)
    del_res1 = testapp.delete_json('/' + source['uuid'])
    assert del_res1.json['status'] == 'success'
    # this item will still have items linking to it indexing occurs
    with pytest.raises(webtest.AppError) as excinfo:
        del_res2 = testapp.delete_json('/' + source['uuid'] + '/?purge=True')
    assert 'Cannot purge item as other items still link to it' in str(excinfo.value)
    # the source should fail due to outdated sids
    # must manually update _sid_cache on dummy_request for source
    src_sid = dummy_request.registry[STORAGE].write.get_by_uuid(source['uuid']).sid
    dummy_request._sid_cache[source['uuid']] = src_sid
    valid2 = util.validate_es_content(source_ctxt, dummy_request, src_es_res_emb, 'embedded')
    assert valid2 is False
    # the target should fail due to outdated rev_links (at least frame=object)
    # need to get a new the target context again, otherwise get a sqlalchemy error
    target_ctxt2 = traverse(dummy_request.root, target_res.json['@graph'][0]['@id'])['context']
    valid3 = util.validate_es_content(target_ctxt2, dummy_request, tar_es_res_obj, 'object')
    assert valid3 is False
    res = indexer_testapp.post_json('/index', {'record': True})
    del_res3 = testapp.delete_json('/' + source['uuid'] + '/?purge=True')
    assert del_res3.json['status'] == 'success'
    assert del_res3.json['notification'] == 'Permanently deleted ' + source['uuid']
    time.sleep(3)
    # make sure everything has updated on ES
    check_es_source = es.get(index=namespaced_link_source, doc_type='testing_link_source_sno',
                             id=source['uuid'], ignore=[404])
    assert check_es_source['found'] == False
    # source uuid removed from the target uuid
    check_es_target = es.get(index=namespaced_link_target, doc_type='testing_link_target_sno',
                             id=target['uuid'])
    uuids_linked_emb2 = [link['uuid'] for link in check_es_target['_source']['linked_uuids_embedded']]
    assert source['uuid'] not in uuids_linked_emb2
    # the source is now purged
    testapp.get('/' + source['uuid'], status=404)
    # make sure check_es_and_cache_linked_sids fails for the purged item
    es_res_emb2 = util.check_es_and_cache_linked_sids(source_ctxt, dummy_request, 'embedded')
    assert es_res_emb2 is None


@pytest.mark.flaky
def test_indexing_invalid_sid(app, testapp, indexer_testapp):
    indexer_queue = app.registry[INDEXER_QUEUE]
    es = app.registry[ELASTIC_SEARCH]
    # post an item, index, then find version (sid)
    res = testapp.post_json(TEST_COLL, {'required': ''})
    test_uuid = res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    time.sleep(4)
    namespaced_test_type = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    es_item = es.get(index=namespaced_test_type, doc_type=TEST_TYPE, id=test_uuid)
    initial_version = es_item['_version']  # same as sid
    assert es_item['_source']['max_sid'] == initial_version

    # now increment the version and check it
    res = testapp.patch_json(TEST_COLL + test_uuid, {'required': 'meh'})
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    time.sleep(4)
    es_item = es.get(index=namespaced_test_type, doc_type=TEST_TYPE, id=test_uuid)
    assert es_item['_version'] == initial_version + 1
    assert es_item['_source']['max_sid'] == initial_version + 1

    # manually cause SidException
    max_sid = app.registry[STORAGE].write.get_max_sid()
    with pytest.raises(SidException):
        check_sid(initial_version + 2, max_sid)


@pytest.mark.flaky
def test_indexing_invalid_sid_linked_items(app, testapp, indexer_testapp):
    """
    Make sure that when an item is deferred due to invalid sid, it does not
    add any items to the secondary queue
    """
    # invalid sid causes infinite loop in MPIndexer, so skip this test if enabled
    # res_vals[2] is continuously True with invalid sid, see MPIndexer L228
    if app.registry.settings['mpindexer'] == True:
        return
    indexer_queue = app.registry[INDEXER_QUEUE]
    es = app.registry[ELASTIC_SEARCH]
    create_mapping.run(
        app,
        collections=['testing_link_target_sno', 'testing_link_source_sno'],
        skip_indexing=True
    )
    target1 = {'name': 't_one', 'uuid': str(uuid.uuid4())}
    source = {
        'name': 'idx_source',
        'target': target1['uuid'],
        'uuid': str(uuid.uuid4()),
        'status': 'current',
    }
    testapp.post_json('/testing-link-targets-sno/', target1, status=201)
    testapp.post_json('/testing-link-sources-sno/', source, status=201)
    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    namespaced_link_target = indexer_utils.get_namespaced_index(app, 'testing_link_target_sno')
    es_item = es.get(index=namespaced_link_target, doc_type='testing_link_target_sno',
                     id=target1['uuid'])
    initial_version = es_item['_version']

    # now try to manually bump an invalid version for the queued item
    # expect it to be recycled to the primary queue and not cause any
    # secondary indexing
    to_queue = {
        'uuid': target1['uuid'],
        'sid': initial_version + 2,
        'strict': False,
        'timestamp': datetime.utcnow().isoformat()
    }
    indexer_queue.send_messages([to_queue], target_queue='primary')
    received_secondary = indexer_queue.receive_messages(target_queue='secondary')
    assert len(received_secondary) == 0
    res = indexer_testapp.post_json('/index', {'record': True})
    time.sleep(4)
    assert res.json['indexing_count'] == 0
    # make sure nothing is in secondary queue after calling /index
    received_secondary = indexer_queue.receive_messages(target_queue='secondary')
    assert len(received_secondary) == 0
    # remove the message with invalid sid
    received_deferred = indexer_queue.receive_messages(target_queue='primary')
    assert len(received_deferred) == 1
    indexer_queue.delete_messages(received_deferred, target_queue='primary')


@pytest.mark.flaky
def test_queue_indexing_endpoint(app, testapp, indexer_testapp):
    es = app.registry[ELASTIC_SEARCH]
    # post a couple things
    testapp.post_json(TEST_COLL, {'required': ''})
    testapp.post_json(TEST_COLL, {'required': ''})
    # index these initial bad boys to get them out of the queue
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 2
    # now use the queue_indexing endpoint to reindex them
    post_body = {
        'collections': [TEST_TYPE],
        'strict': True
    }
    res = indexer_testapp.post_json('/queue_indexing?telemetry_id=test', post_body)
    assert res.json['notification'] == 'Success'
    assert res.json['number_queued'] == 2
    assert res.json['telemetry_id'] == 'test'
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 2
    time.sleep(4)
    namespaced_test_type = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    doc_count = es.count(index=namespaced_test_type, doc_type=TEST_TYPE).get('count')
    assert doc_count == 2

    # here are some failure situations
    post_body = {
        'collections': TEST_TYPE,
        'strict': False
    }
    res = indexer_testapp.post_json('/queue_indexing', post_body)
    assert res.json['notification'] == 'Failure'
    assert res.json['number_queued'] == 0

    post_body = {
        'uuids': ['abc123'],
        'collections': [TEST_TYPE],
        'strict': False
    }
    res = indexer_testapp.post_json('/queue_indexing', post_body)
    assert res.json['notification'] == 'Failure'
    assert res.json['number_queued'] == 0


@pytest.mark.flaky
def test_es_indices(app, elasticsearch):
    """
    Test overall create_mapping functionality using app.
    Do this by checking es directly before and after running mapping.
    Delete an index directly, run again to see if it recovers.
    """
    es = app.registry[ELASTIC_SEARCH]
    item_types = app.registry[TYPES].by_item_type
    test_collections = [TEST_TYPE]
    # run create mapping for all types, but no need to index
    run(app, collections=test_collections, skip_indexing=True)
    # check that mappings and settings are in index
    for item_type in test_collections:
        item_mapping = type_mapping(app.registry[TYPES], item_type)
        try:
            namespaced_index = indexer_utils.get_namespaced_index(app, item_type)
            item_index = es.indices.get(index=namespaced_index)
        except:
            assert False
        found_index_mapping = item_index.get(namespaced_index, {}).get('mappings').get(item_type, {}).get('properties', {}).get('embedded')
        found_index_settings = item_index.get(namespaced_index, {}).get('settings')
        assert found_index_mapping
        assert found_index_settings


@pytest.mark.flaky
def test_index_settings(app, testapp, indexer_testapp):
    from snovault.elasticsearch.create_mapping import index_settings
    es_settings = index_settings()
    max_result_window = es_settings['index']['max_result_window']
    # preform some initial indexing to build meta
    res = testapp.post_json(TEST_COLL, {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    # need to make sure an xmin was generated for the following to work
    assert 'indexing_finished' in res.json
    es = app.registry[ELASTIC_SEARCH]
    namespaced_test_type = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    curr_settings = es.indices.get_settings(index=namespaced_test_type)
    found_max_window = curr_settings.get(namespaced_test_type, {}).get('settings', {}).get('index', {}).get('max_result_window', None)
    # test one important setting
    assert int(found_max_window) == max_result_window


# some unit tests associated with build_index in create_mapping
@pytest.mark.flaky
def test_check_if_index_exists(app):
    es = app.registry[ELASTIC_SEARCH]
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    exists = check_if_index_exists(es, namespaced_index)
    assert exists
    # delete index
    es.indices.delete(index=namespaced_index)
    exists = check_if_index_exists(es, namespaced_index)
    assert not exists


@pytest.mark.flaky
def test_confirm_mapping(app, testapp, indexer_testapp):
    es = app.registry[ELASTIC_SEARCH]
    # make a dynamic mapping
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    es.indices.delete(index=namespaced_index)
    time.sleep(2)
    testapp.post_json(TEST_COLL, {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    time.sleep(2)
    mapping = create_mapping_by_type(TEST_TYPE, app.registry)
    index_record = build_index_record(mapping, TEST_TYPE)
    tries_taken = confirm_mapping(es, namespaced_index, TEST_TYPE, index_record)
    # 3 tries means it failed to correct, 0 means it was unneeded
    assert tries_taken > 0 and tries_taken < 3
    # test against a live mapping to ensure handling of dynamic mapping works
    run(app, collections=[TEST_TYPE], skip_indexing=True)
    # compare_against_existing_mapping is used under the hood in confirm_mapping
    assert compare_against_existing_mapping(es, namespaced_index, TEST_TYPE, index_record, True) is True


@pytest.mark.flaky
def test_dynamic_mapping_check_first(app, testapp, indexer_testapp):
    """
    create_mapping with --check-first option must be able to properly compare
    mappings that have been affected by dynamic mapping to those freshly
    generated from schemas. One case of this being challenging is with items
    with additionalProperties=True in their schemas...
    """
    es = app.registry[ELASTIC_SEARCH]
    ppp_body = {
        'required': '',
        'custom_object': {'mapped_property': 'hey', 'unmap1': 1, 'unmap2': '2'}
    }
    testapp.post_json(TEST_COLL, ppp_body)
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    time.sleep(2)
    mapping = create_mapping_by_type(TEST_TYPE, app.registry)
    index_record = build_index_record(mapping, TEST_TYPE)
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    assert compare_against_existing_mapping(es, namespaced_index, TEST_TYPE, index_record, True) is True


@pytest.mark.flaky
def test_check_and_reindex_existing(app, testapp):
    from snovault.elasticsearch.create_mapping import check_and_reindex_existing
    es = app.registry[ELASTIC_SEARCH]
    # post an item but don't reindex
    # this will cause the testing-ppp index to queue reindexing when we call
    # check_and_reindex_existing
    res = testapp.post_json(TEST_COLL, {'required': ''})
    time.sleep(2)
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    doc_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    # doc_count has not yet updated
    assert doc_count == 0
    test_uuids = {TEST_TYPE: set()}
    check_and_reindex_existing(app, es, TEST_TYPE, test_uuids)
    assert(len(test_uuids)) == 1


@pytest.mark.flaky
def test_es_purge_uuid(app, testapp, indexer_testapp, session):
    indexer_queue = app.registry[INDEXER_QUEUE]
    es = app.registry[ELASTIC_SEARCH]
    ## Adding new test resource to DB
    storage = app.registry[STORAGE]
    test_body = {'required': '', 'simple1' : 'foo', 'simple2' : 'bar' }
    res = testapp.post_json(TEST_COLL, test_body)
    test_uuid = res.json['@graph'][0]['uuid']
    check = storage.get_by_uuid(test_uuid)

    assert str(check.uuid) == test_uuid

    # Then index it:
    create_mapping.run(app, collections=[TEST_TYPE], sync_index=True)
    indexer_queue.clear_queue()
    time.sleep(4)

    ## Now ensure that we do have it in ES:
    try:
        namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
        es_item = es.get(index=namespaced_index, doc_type=TEST_TYPE, id=test_uuid)
    except:
        assert False
    item_uuid = es_item.get('_source', {}).get('uuid')
    assert item_uuid == test_uuid

    check_post_from_rdb = storage.write.get_by_uuid(test_uuid)
    assert check_post_from_rdb is not None

    assert es_item['_source']['embedded']['simple1'] == test_body['simple1']
    assert es_item['_source']['embedded']['simple2'] == test_body['simple2']

    # The actual delete
    storage.purge_uuid(test_uuid, namespaced_index) # We can optionally pass in TEST_TYPE as well for better performance.

    check_post_from_rdb_2 = storage.write.get_by_uuid(test_uuid)

    assert check_post_from_rdb_2 is None

    time.sleep(5) # Allow time for ES API to send network request to ES server to perform delete.
    check_post_from_es_2 = es.get(index=namespaced_index, doc_type=TEST_TYPE, id=test_uuid, ignore=[404])
    assert check_post_from_es_2['found'] == False


@pytest.mark.flaky
def test_create_mapping_check_first(app, testapp, indexer_testapp):
    es = app.registry[ELASTIC_SEARCH]
    # get the initial mapping
    mapping = create_mapping_by_type(TEST_TYPE, app.registry)
    index_record = build_index_record(mapping, TEST_TYPE)
    # ensure the dynamic mapping matches the manually created one
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    assert compare_against_existing_mapping(es, namespaced_index, TEST_TYPE, index_record, True) is True

    # post an item and then index it
    testapp.post_json(TEST_COLL, {'required': ''})
    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    initial_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    # run with check_first but skip indexing. counts should still match because
    # the index wasn't removed
    run(app, check_first=True, collections=[TEST_TYPE], skip_indexing=True)
    time.sleep(2)
    second_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    counter = 0
    while (second_count != initial_count and counter < 10):
        time.sleep(2)
        second_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
        counter +=1
    assert second_count == initial_count

    # remove the index manually and do not index
    # should cause create_mapping w/ check_first to recreate
    es.indices.delete(index=namespaced_index)
    run(app, collections=[TEST_TYPE], check_first=True, skip_indexing=True)
    time.sleep(2)
    third_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert third_count == 0
    # ensure the re-created dynamic mapping still matches the original one
    assert compare_against_existing_mapping(es, namespaced_index, TEST_TYPE, index_record, True) is True


def delay_rerun(*args):
    """ Rerun function for flaky """
    time.sleep(30)
    return True


@pytest.mark.flaky(max_runs=2, rerun_filter=delay_rerun)
def test_create_mapping_index_diff(app, testapp, indexer_testapp):
    es = app.registry[ELASTIC_SEARCH]
    # post a couple items, index, then remove one
    res = testapp.post_json(TEST_COLL, {'required': ''})
    test_uuid = res.json['@graph'][0]['uuid']
    testapp.post_json(TEST_COLL, {'required': ''})  # second item
    create_mapping.run(app, collections=[TEST_TYPE])
    indexer_queue = app.registry[INDEXER_QUEUE]
    indexer_testapp.post_json('/index', {'record': True})
    indexer_queue.clear_queue()
    time.sleep(4)
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    initial_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert initial_count == 2

    # remove one item
    es.delete(index=namespaced_index, doc_type=TEST_TYPE, id=test_uuid)
    time.sleep(8)
    second_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert second_count == 1

    # patch the item to increment version
    res = testapp.patch_json(TEST_COLL + test_uuid, {'required': 'meh'})
    # index with index_diff to ensure the item is reindexed
    create_mapping.run(app, collections=[TEST_TYPE], index_diff=True)
    res = indexer_testapp.post_json('/index', {'record': True})
    time.sleep(4)
    third_count = es.count(index=namespaced_index, doc_type=TEST_TYPE).get('count')
    assert third_count == initial_count


@pytest.mark.flaky(max_runs=2, rerun_filter=delay_rerun)
def test_indexing_esstorage(app, testapp, indexer_testapp):
    """
    Test some esstorage methods (a.k.a. registry[STORAGE].read)
    """
    indexer_queue = app.registry[INDEXER_QUEUE]
    es = app.registry[ELASTIC_SEARCH]
    esstorage = app.registry[STORAGE].read
    # post an item, index, then find version (sid)
    res = testapp.post_json(TEST_COLL, {'required': 'some_value'})
    test_uuid = res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    time.sleep(4)
    namespaced_test_type = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    es_res = es.get(index=namespaced_test_type, doc_type=TEST_TYPE, id=test_uuid)['_source']
    # test the following methods:
    es_res_by_uuid = esstorage.get_by_uuid(test_uuid)
    es_res_by_json = esstorage.get_by_json('required', 'some_value', TEST_TYPE)
    es_res_direct = esstorage.get_by_uuid_direct(test_uuid, namespaced_test_type, TEST_TYPE)
    assert es_res == es_res_by_uuid.source
    assert es_res == es_res_by_json.source
    assert es_res == es_res_direct['_source']
    # make sure indexing_stats and certain timings are included
    assert 'indexing_stats' in es_res_direct['_source']
    assert 'embedded_view' in es_res_direct['_source']['indexing_stats']
    assert 'total_indexing_view' in es_res_direct['_source']['indexing_stats']
    # db get_by_uuid direct returns None by design
    db_res_direct = app.registry[STORAGE].write.get_by_uuid_direct(test_uuid, namespaced_test_type, TEST_TYPE)
    assert db_res_direct == None


@pytest.mark.flaky(max_runs=2, rerun_filter=delay_rerun)
def test_aggregated_items(app, testapp, indexer_testapp):
    """
    Test that the item aggregation works, which only occurs when indexing
    is actually run. This test does the following:
    - Post a TestingLinkAggregateSno, which links to 2 TestingLinkSourceSno
    - Check aggregated-items view for the item; should be empty before indexing
    - Index and retrieve the TestingLinkAggregateSno from ES
    - Check that the aggregations worked correctly
    - Patch the TestingLinkAggregateSno to only 1 TestingLinkSourceSno, index
    - Ensure that the aggregated_items changed, checking ES
    - Ensure that duplicate aggregated_items are deduplicated
    - Check aggregated-items view; should now match ES results
    """
    import webtest
    es = app.registry[ELASTIC_SEARCH]
    indexer_queue = app.registry[INDEXER_QUEUE]
    # first, run create mapping with the indices we will use
    namespaced_aggregate = indexer_utils.get_namespaced_index(app, 'testing_link_aggregate_sno')
    create_mapping.run(
        app,
        collections=['testing_link_target_sno', 'testing_link_aggregate_sno'],
        skip_indexing=True
    )
    # generate a uuid for the aggregate item
    agg_res_uuid = str(uuid.uuid4())
    target1  = {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'}
    target2  = {'name': 'two', 'uuid': '775795d3-4410-4114-836b-8eeecf1daabc'}
    aggregated = {
        'name': 'A',
        'targets': [
            {
                'test_description': 'target one',
                'target': '775795d3-4410-4114-836b-8eeecf1d0c2f'
            },
            {
                'test_description': 'target two',
                'target': '775795d3-4410-4114-836b-8eeecf1daabc'
            }
        ],
        'uuid': agg_res_uuid,
        'status': 'current'
    }
    # you can do stuff like this and it will take effect
    # app.registry['types']['testing_link_aggregate_sno'].aggregated_items['targets'] = ['target.name', 'test_description']
    target1_res = testapp.post_json('/testing-link-targets-sno/', target1, status=201)
    target2_res = testapp.post_json('/testing-link-targets-sno/', target2, status=201)
    agg_res = testapp.post_json('/testing-link-aggregates-sno/', aggregated, status=201)
    agg_res_atid = agg_res.json['@graph'][0]['@id']
    # ensure that aggregated-items view shows nothing before indexing
    pre_agg_view = testapp.get(agg_res_atid + '@@aggregated-items', status=200).json
    assert pre_agg_view['@id'] == agg_res_atid
    assert pre_agg_view['aggregated_items'] == {}
    # wait for the items to index
    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    # wait for test-link-aggregated item to index
    doc_count = es.count(index=namespaced_aggregate, doc_type='testing_link_aggregate_sno').get('count')
    tries = 0
    while doc_count < 1 and tries < 5:
        time.sleep(2)
        doc_count = es.count(index=namespaced_aggregate, doc_type='testing_link_aggregate_sno').get('count')
        tries += 1
    assert doc_count == 1
    es_agg_res = es.get(index=namespaced_aggregate,
                        doc_type='testing_link_aggregate_sno', id=agg_res_uuid)
    assert 'aggregated_items' in es_agg_res['_source']
    es_agg_items = es_agg_res['_source']['aggregated_items']
    assert 'targets' in es_agg_items
    assert len(es_agg_items['targets']) == 2
    for idx, target_agg in enumerate(es_agg_items['targets']):
        # order of targets should be maintained
        assert target_agg['parent'] == agg_res.json['@graph'][0]['@id']
        assert target_agg['embedded_path'] == 'targets'
        if idx == 0:
            assert target_agg['item']['test_description'] == 'target one'
            assert target_agg['item']['target']['uuid'] == target1['uuid']
        else:
            assert target_agg['item']['test_description'] == 'target two'
            assert target_agg['item']['target']['uuid'] == target2['uuid']
    # now make sure they get updated on a patch
    # use duplicate items, which should be deduplicated if all aggregated
    # content (including parent) is exactly the same
    testapp.patch_json(
        '/testing-link-aggregates-sno/' + aggregated['uuid'],
        {'targets': [
            {'test_description': 'target one revised',
            'target': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
            {'test_description': 'target one revised',
            'target': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
            {'test_description': 'target one revised2',
             'target': '775795d3-4410-4114-836b-8eeecf1d0c2f'}
        ]}
    )
    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(10)  # be lazy and just wait a bit
    es_agg_res = es.get(index=namespaced_aggregate, doc_type='testing_link_aggregate_sno', id=agg_res_uuid)
    assert 'aggregated_items' in es_agg_res['_source']
    es_agg_items = es_agg_res['_source']['aggregated_items']
    assert 'targets' in es_agg_items
    assert len(es_agg_items['targets']) == 2
    assert es_agg_items['targets'][0]['item']['test_description'] == 'target one revised'
    assert es_agg_items['targets'][1]['item']['test_description'] == 'target one revised2'
    # check that the aggregated-items view now works
    post_agg_view = testapp.get(agg_res_atid + '@@aggregated-items', status=200).json
    assert post_agg_view['@id'] == agg_res_atid
    assert post_agg_view['aggregated_items'] == es_agg_res['_source']['aggregated_items']
    # clean up the test items
    testapp.patch_json('/testing-link-aggregates-sno/' + aggregated['uuid'],
                       {'targets': []})
    indexer_testapp.post_json('/index', {'record': True})


@pytest.mark.flaky(max_runs=2, rerun_filter=delay_rerun)
def test_indexing_info(app, testapp, indexer_testapp):
    """
    Test the information on indexing-info for a given uuid and make sure that
    it updates properly following indexing
    """
    # first, run create mapping with the indices we will use
    create_mapping.run(
        app,
        collections=['testing_link_target_sno', 'testing_link_source_sno'],
        skip_indexing=True
    )
    target1 = {'name': 't_one', 'uuid': str(uuid.uuid4())}
    target2 = {'name': 't_two', 'uuid': str(uuid.uuid4())}
    source = {
        'name': 'idx_source',
        'target': target1['uuid'],
        'uuid': str(uuid.uuid4()),
        'status': 'current',
    }
    testapp.post_json('/testing-link-targets-sno/', target1, status=201)
    testapp.post_json('/testing-link-targets-sno/', target2, status=201)
    testapp.post_json('/testing-link-sources-sno/', source, status=201)
    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    # indexing-info fails without uuid query param
    idx_info_err = testapp.get('/indexing-info')
    assert idx_info_err.json['status'] == 'error'
    src_idx_info = testapp.get('/indexing-info?uuid=%s' % source['uuid'])
    assert src_idx_info.json['status'] == 'success'
    assert 'indexing_stats' in src_idx_info.json
    assert 'embedded_view' in src_idx_info.json['indexing_stats']
    # up to date
    assert src_idx_info.json['sid_es'] == src_idx_info.json['sid_db']
    assert set(src_idx_info.json['uuids_invalidated']) == set([target1['uuid'], source['uuid']])
    # update without indexing; view should capture the changes but sid_es will not change
    testapp.patch_json('/testing-link-sources-sno/' + source['uuid'], {'target': target2['uuid']})
    src_idx_info2 = testapp.get('/indexing-info?uuid=%s' % source['uuid'])
    assert src_idx_info2.json['status'] == 'success'
    assert 'indexing_stats' in src_idx_info2.json
    assert 'embedded_view' in src_idx_info2.json['indexing_stats']
    # es is now out of date, since not indexed yet
    assert src_idx_info2.json['sid_es'] < src_idx_info2.json['sid_db']
    # target1 will still be in invalidated uuids, since es has not updated
    assert set(src_idx_info2.json['uuids_invalidated']) == set([target1['uuid'], target2['uuid'], source['uuid']])
    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    # after indexing, make sure sid_es is updated
    src_idx_info3 = testapp.get('/indexing-info?uuid=%s' % source['uuid'])
    assert src_idx_info3.json['status'] == 'success'
    assert src_idx_info3.json['sid_es'] == src_idx_info3.json['sid_db']
    assert 'indexing_stats' in src_idx_info3.json
    assert 'embedded_view' in src_idx_info3.json['indexing_stats']
    # target1 has now been updated and removed from invalidated uuids
    assert set(src_idx_info3.json['uuids_invalidated']) == set([target2['uuid'], source['uuid']])
    # try the view without calculated embedded view
    src_idx_info4 = testapp.get('/indexing-info?uuid=%s&run=False' % source['uuid'])
    assert src_idx_info4.json['status'] == 'success'
    assert 'uuids_invalidated' not in src_idx_info4.json
    assert 'indexing_stats' not in src_idx_info4.json


@pytest.mark.flaky(max_runs=2, rerun_filter=delay_rerun)
def test_validators_on_indexing(app, testapp, indexer_testapp):
    """
    We now run PATCH validators for an indexed item using check_only=True
    query param (so data isn't actually changed)
    """
    es = app.registry[ELASTIC_SEARCH]
    # make an item with a validation error (`simple1` should be str)
    res = testapp.post_json(TEST_COLL + '?validate=false&upgrade=False',
                            {'required': '', 'simple1': 1}, status=201)
    ppp_id = res.json['@graph'][0]['@id']
    # validation-errors view should be empty before indexing
    val_err_view = testapp.get(ppp_id + '@@validation-errors', status=200).json
    assert val_err_view['@id'] == ppp_id
    assert val_err_view['validation_errors'] == []

    indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    namespaced_index = indexer_utils.get_namespaced_index(app, TEST_TYPE)
    es_res = es.get(index=namespaced_index, doc_type=TEST_TYPE, id=res.json['@graph'][0]['uuid'])
    assert len(es_res['_source'].get('validation_errors', [])) == 1
    assert es_res['_source']['validation_errors'][0]['name'] == 'Schema: simple1'
    # check that validation-errors view works
    val_err_view = testapp.get(ppp_id + '@@validation-errors', status=200).json
    assert val_err_view['@id'] == ppp_id
    assert val_err_view['validation_errors'] == es_res['_source']['validation_errors']
