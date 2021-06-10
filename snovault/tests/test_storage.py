import pytest
import re
import transaction as transaction_management
import uuid

from pyramid.threadlocal import manager
from sqlalchemy import func
from sqlalchemy.orm.exc import FlushError
from ..interfaces import DBSESSION, STORAGE
from ..storage import (
    Blob,
    CurrentPropertySheet,
    Key,
    Link,
    PickStorage,
    PropertySheet,
    RDBStorage,
    register_storage,
    Resource,
    S3BlobStorage,
)


pytestmark = pytest.mark.storage


POSTGRES_MAJOR_VERSION_EXPECTED = 11

def test_postgres_version(session):

    (version_info,) = session.query(func.version()).one()
    print("version_info=", version_info)
    assert isinstance(version_info, str)
    assert re.match("PostgreSQL %s([.][0-9]+)? " % POSTGRES_MAJOR_VERSION_EXPECTED, version_info)


def test_storage_creation(session):
    assert session.query(PropertySheet).count() == 0
    assert session.query(CurrentPropertySheet).count() == 0
    assert session.query(Blob).count() == 0
    assert session.query(Key).count() == 0
    assert session.query(Link).count() == 0


def test_transaction_record_rollback(session):
    rid = uuid.uuid4()
    resource = Resource('test_item', {'': {}}, rid=rid)
    session.add(resource)
    transaction_management.commit()
    transaction_management.begin()
    sp = session.begin_nested()
    resource = Resource('test_item', {'': {}}, rid=rid)
    session.add(resource)
    with pytest.raises(Exception):
        sp.commit()
    sp.rollback()
    resource = Resource('test_item', {'': {}})
    session.add(resource)
    transaction_management.commit()


def test_current_propsheet(session):
    name = 'testdata'
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {name: props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    assert resource.rid
    assert resource[name] == props1
    propsheet = session.query(PropertySheet).one()
    assert propsheet.sid
    assert propsheet.rid == resource.rid
    current = session.query(CurrentPropertySheet).one()
    assert current.sid == propsheet.sid
    assert current.rid == resource.rid
    # tid is removed
    assert not hasattr(current, 'tid')


def test_current_propsheet_update(session):
    name = 'testdata'
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {name: props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    props2 = {'foo': 'baz'}
    resource[name] = props2
    session.flush()
    resource = session.query(Resource).one()
    session.flush()
    assert resource[name] == props2
    assert session.query(PropertySheet).count() == 2
    assert [propsheet.properties for propsheet in resource.data[name].history] == [props1, props2]
    current = session.query(CurrentPropertySheet).one()
    assert current.sid
    # tid is removed
    assert not hasattr(current, 'tid')


def test_get_by_json(session):
    name = 'testdata'
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {name: props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    props2 = {'foo': 'baz'}
    resource[name] = props2
    session.flush()
    resource = session.query(Resource).one()
    session.flush()
    query = (session.query(CurrentPropertySheet)
             .join(CurrentPropertySheet.propsheet, CurrentPropertySheet.resource)
             .filter(PropertySheet.properties['foo'].astext == 'baz')
             )

    data = query.one()
    assert data.propsheet.properties == props2


def test_purge_uuid(session, storage):
    """ Tests full purge of metadata (including revision history). """
    name = 'testdata'
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {name: props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    check = storage.get_by_uuid(str(resource.rid))
    assert check[name] == props1
    # add a key
    testname = 'foo'
    key = Key(rid=resource.rid, name=testname, value=props1[testname])
    session.add(key)
    session.flush()
    assert session.query(Key).count() == 1

    propsheet = session.query(PropertySheet).one()
    assert propsheet.sid
    assert propsheet.rid == resource.rid
    current = session.query(CurrentPropertySheet).one()
    assert current.sid == propsheet.sid
    assert current.rid == resource.rid

    assert len(storage.revision_history(uuid=str(resource.rid))) > 0
    storage.purge_uuid(str(resource.rid))
    check_post = storage.get_by_uuid(str(resource.rid))
    assert not check_post
    assert session.query(Key).count() == 0
    assert session.query(PropertySheet).count() == 0
    assert session.query(CurrentPropertySheet).count() == 0
    assert storage.revision_history(uuid=str(resource.rid)) == []


def test_delete_compound(session, storage):
    name = 'testdata'
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {name: props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    check = storage.get_by_uuid(str(resource.rid))
    assert check[name] == props1
    # add a key
    testname = 'foo'
    key = Key(rid=resource.rid, name=testname, value=props1[testname])
    session.add(key)
    session.flush()
    assert session.query(Key).count() == 1

    props2 = {'foo': 'baz'}
    resource[name] = props2
    session.flush()
    resource = session.query(Resource).one()
    session.flush()
    assert resource[name] == props2
    assert session.query(PropertySheet).count() == 2
    assert [propsheet.properties for propsheet in resource.data[name].history] == [props1, props2]
    current = session.query(CurrentPropertySheet).one()
    assert current.sid

    assert len(storage.revision_history(uuid=str(resource.rid))) > 0
    storage.purge_uuid(str(resource.rid))
    check_post = storage.get_by_uuid(str(resource.rid))
    assert not check_post
    assert session.query(Key).count() == 0
    assert session.query(PropertySheet).count() == 0
    assert session.query(CurrentPropertySheet).count() == 0
    assert storage.revision_history(uuid=str(resource.rid)) == []


def test_keys(session):
    name = 'testdata'
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {name: props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()

    testname = 'foo'
    key = Key(rid=resource.rid, name=testname, value=props1[testname])
    session.add(key)
    session.flush()
    assert session.query(Key).count() == 1
    othertest = 'foofoo'
    othervalue = 'barbar'
    key2 = Key(rid=resource.rid, name=othertest, value=othervalue)
    session.add(key2)
    session.flush()
    assert session.query(Key).count() == 2
    props2 = {'foofoo': 'barbar'}
    resource2 = Resource('test_item', {name: props2})
    session.add(resource2)
    session.flush()
    key3 = Key(rid=resource2.rid, name=testname, value=props1[testname])
    session.add(key3)
    with pytest.raises(FlushError):
        session.flush()


def test_get_sids_by_uuids(session, storage):
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {'': props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    sids = storage.get_sids_by_uuids([str(resource.rid)])
    assert set(sids) == {str(resource.rid)}


def test_S3BlobStorage():
    blob_bucket = 'encoded-4dn-blobs'
    storage = S3BlobStorage(blob_bucket)
    assert storage.bucket == blob_bucket

    download_meta = {'download': 'test.txt'}
    storage.store_blob('data', download_meta)
    assert download_meta['bucket'] == blob_bucket
    assert 'key' in download_meta

    data = storage.get_blob(download_meta)
    assert data == 'data'

    url = storage.get_blob_url(download_meta)
    assert url
    assert blob_bucket in url
    assert 'Signature' in url


def test_S3BlobStorage_get_blob_url_for_non_s3_file():
    blob_bucket = 'encoded-4dn-blobs'
    storage = S3BlobStorage(blob_bucket)
    assert storage.bucket == blob_bucket
    download_meta = {'blob_id': 'blob_id'}
    url = storage.get_blob_url(download_meta)
    assert url


def test_pick_storage(registry, dummy_request):
    # use a dummy value for ElasticSearchStorage
    storage = PickStorage(RDBStorage(registry[DBSESSION]), 'dummy_es', registry)
    assert isinstance(storage.write, RDBStorage)
    assert storage.read == 'dummy_es'
    # test storage selection logic
    assert storage.storage('database') is storage.write
    assert storage.storage('elasticsearch') == 'dummy_es'
    with pytest.raises(Exception) as exec_info:
        storage.storage('not_a_db')
    assert 'Invalid forced datastore not_a_db' in str(exec_info)
    assert storage.storage() is storage.write

    dummy_request.datastore = 'elasticsearch'
    manager.push({'request': dummy_request, 'registry': registry})
    assert storage.storage() == 'dummy_es'
    manager.pop()


def test_register_storage(registry):
    # test storage.register_storage, used to configure registry[STORAGE]
    storage = PickStorage('dummy_db', 'dummy_es', registry)
    # store previous storage and use a dummy one for testing
    prev_storage = registry[STORAGE]
    registry[STORAGE] = storage
    # expect existing values to be used
    register_storage(registry)
    assert registry[STORAGE].write == 'dummy_db'
    assert registry[STORAGE].read == 'dummy_es'
    # expect overrides to be used
    register_storage(registry, write_override='override_db',
                     read_override='override_es')
    assert registry[STORAGE].write == 'override_db'
    assert registry[STORAGE].read == 'override_es'
    # reset storage
    registry[STORAGE] = prev_storage
