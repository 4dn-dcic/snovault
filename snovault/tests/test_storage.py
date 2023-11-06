from unittest import mock
import pytest
import re
import uuid
import boto3

from dcicutils.misc_utils import filtered_warnings
from pyramid.threadlocal import manager
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from ..interfaces import DBSESSION, STORAGE
from ..storage import (
    POSTGRES_COMPATIBLE_MAJOR_VERSIONS,
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
from moto import mock_s3

pytestmark = pytest.mark.storage


def test_postgres_version(session):
    """ Tests that the local postgres is running one of the compatible versions """
    (version_info,) = session.query(func.version()).one()
    print("version_info=", version_info)
    assert isinstance(version_info, str)
    assert re.match("PostgreSQL (%s)([.][0-9]+)? " % '|'.join(POSTGRES_COMPATIBLE_MAJOR_VERSIONS), version_info)


def test_storage_creation(session):
    assert session.query(PropertySheet).count() == 0
    assert session.query(CurrentPropertySheet).count() == 0
    assert session.query(Blob).count() == 0
    assert session.query(Key).count() == 0
    assert session.query(Link).count() == 0


def test_transaction_record_rollback(session):
    """ Tests that committing and rolling back an invalid transactions works as expected """
    rid = uuid.uuid4()
    sp1 = session.begin_nested()
    resource = Resource('test_item', {'': {}}, rid=rid)
    session.add(resource)
    sp1.commit()

    # test rollback
    sp2 = session.begin_nested()
    resource = Resource('test_item', {'': {}}, rid=rid)
    session.add(resource)
    with pytest.raises(Exception):
        sp2.commit()
    sp2.rollback()
    resource = Resource('test_item', {'': {}})
    session.add(resource)


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
             # Rewrittent to use two separate joins per SQLAlchemy 2.0 requirements. -kmp 10-Apr-2023
             .join(CurrentPropertySheet.propsheet)
             .join(CurrentPropertySheet.resource)
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
    # try to insert a duplicate unique key, previously threw FlushError
    with pytest.raises(IntegrityError):  # in newer sqlalchemy versions IntegrityError is more accurate
        session.flush()


def test_get_sids_by_uuids(session, storage):
    props1 = {'foo': 'bar'}
    resource = Resource('test_item', {'': props1})
    session.add(resource)
    session.flush()
    resource = session.query(Resource).one()
    sids = storage.get_sids_by_uuids([str(resource.rid)])
    assert set(sids) == {str(resource.rid)}


@pytest.mark.parametrize(
    's3_encrypt_key_id,kms_args_expected',
    [(None, False), ("", False), (str(uuid.uuid4()), True)],
)
def test_S3BlobStorage(s3_encrypt_key_id, kms_args_expected):
    # NOTE: I have separated the call to _test_S3BlobStorage into a separate function so I can wrap it with
    #       @mock_s3 after establishing this context manager to suppress a warning. (It could have been an
    #       internal function, but it shows up better in the patch diffs if I do it this way, and will be
    #       easier to simplify later. The warning is due to a call made by moto 1.3.x that uses deprecated
    #       support complained about in responses 0.17.0. Hopefully if we ever get higher than that version
    #       of moto, we can reconsider this. However, moto 2.0 requires some change in configuration that we'd
    #       have to take time to learn about. -kmp 5-Feb-2022
    with filtered_warnings('ignore', category=DeprecationWarning):
        # The warning being suppressed (which comes from moto 1.3.x) looks like:
        #   ...env/lib/python3.6/site-packages/responses/__init__.py:484:
        #   DeprecationWarning: stream argument is deprecated. Use stream parameter in request directly
        # HOPEFULLY that's the only deprecation warning that would come from this test, which is why it
        # would be good to remove these warnings when we are able. -kmp 5-Feb-2022
        _test_S3BlobStorage(s3_encrypt_key_id, kms_args_expected)


@mock_s3
def _test_S3BlobStorage(s3_encrypt_key_id, kms_args_expected):

    blob_bucket = 'encoded-4dn-blobs'  # note that this bucket exists but is mocked out here
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=blob_bucket)

    storage = S3BlobStorage(blob_bucket, kms_key_id=s3_encrypt_key_id)
    assert storage.bucket == blob_bucket
    if s3_encrypt_key_id:
        assert storage.kms_key_id == s3_encrypt_key_id

    download_meta = {'download': 'test.txt'}
    with mock.patch.object(
        storage.s3, 'put_object', side_effect=storage.s3.put_object
    ) as mocked_s3_put_object:  # To obtain calls while retaining function
        storage.store_blob('data', download_meta)
        assert download_meta['bucket'] == blob_bucket
        assert 'key' in download_meta
        mocked_s3_put_object.assert_called_once()
        call_kwargs = mocked_s3_put_object.call_args.kwargs
        if kms_args_expected:
            assert call_kwargs.get("ServerSideEncryption") == "aws:kms"
            assert call_kwargs.get("SSEKMSKeyId") == s3_encrypt_key_id
        else:
            assert "ServerSideEncryption" not in call_kwargs
            assert "SSEKMSKeyId" not in call_kwargs

    data = storage.get_blob(download_meta)
    assert data == 'data'

    url = storage.get_blob_url(download_meta)
    assert url
    assert blob_bucket in url
    assert 'Signature' in url


def test_S3BlobStorage_get_blob_url_for_non_s3_file():
    # NOTE: See test_S3BlobStorage for note explaining this filtering of warning and when/how it can go away.
    #       -kmp 5-Feb-2022
    with filtered_warnings('ignore', category=DeprecationWarning):
        # The warning being suppressed (which comes from moto 1.3.x) looks like:
        #   ...env/lib/python3.6/site-packages/responses/__init__.py:484:
        #   DeprecationWarning: stream argument is deprecated. Use stream parameter in request directly
        _test_S3BlobStorage_get_blob_url_for_non_s3_file()


@mock_s3
def _test_S3BlobStorage_get_blob_url_for_non_s3_file():

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
    assert 'Invalid forced datastore not_a_db' in str(exec_info.value)
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
