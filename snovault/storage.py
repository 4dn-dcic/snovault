import boto3
import structlog
import uuid

from botocore.client import Config
from dcicutils.misc_utils import ignored, get_error_message
from pyramid.httpexceptions import HTTPConflict, HTTPLocked, HTTPInternalServerError
from pyramid.threadlocal import get_current_request
from sqlalchemy import Column, ForeignKey, bindparam, func, orm, schema, types
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB as JSON
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext import baked
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, collections
from sqlalchemy.orm.exc import FlushError, MultipleResultsFound, NoResultFound
from .interfaces import BLOBS, DBSESSION, STORAGE


log = structlog.getLogger(__name__)

_DBSESSION = None


def includeme(config):
    registry = config.registry
    # add `datastore` attribute to request
    config.add_request_method(datastore, 'datastore', reify=True)
    # register PickStorage initialized with write storage
    write_stg = RDBStorage(registry[DBSESSION]) if registry[DBSESSION] else None
    register_storage(registry, write_override=write_stg)


def datastore(request):
    """
    Function that is reified as `request.datastore`. Used with PickStorage
    to determine whether to use RDBStorage or ElasticSearchStorage. Can be
    overriden by storage argument to `PickStorage.storage` directly
    """
    if request.__parent__ is not None:
        return request.__parent__.datastore
    datastore = 'database'
    if request.method in ('HEAD', 'GET'):
        datastore = request.params.get('datastore') or \
            request.headers.get('X-Datastore') or \
            request.registry.settings.get('collection_datastore', 'elasticsearch')
    return datastore


def register_storage(registry, write_override=None, read_override=None):
    """
    Wrapper function to register a PickStorage as registry[STORAGE].
    Attempts to reuse an existing PickStorage if possible, including read/write
    connections. If `write_override` or `read_override` are provided, will
    override PickStorage attributes with the given values

    Also sets up global DBSESSION config for this file and initializes
    blob storage
    """
    write_storage = None
    read_storage = None
    if isinstance(registry.get(STORAGE), PickStorage):
        write_storage = registry[STORAGE].write
        read_storage = registry[STORAGE].read
    if write_override is not None:
        write_storage = write_override
    if read_override is not None:
        read_storage = read_override
    # set PickStorage with correct write/read
    registry[STORAGE] = PickStorage(write_storage, read_storage, registry)

    # global config needed for some storage-related properties
    global _DBSESSION
    _DBSESSION = registry[DBSESSION]

    # set up blob storage if not configured already
    blob_bucket = registry.settings.get('blob_bucket', None)
    s3_encrypt_key_id = registry.settings.get('s3_encrypt_key_id', None)  # TODO: refactor SettingsKey
    registry[BLOBS] = (S3BlobStorage(blob_bucket, kms_key_id=s3_encrypt_key_id)
                       if blob_bucket
                       else RDBBlobStorage(registry[DBSESSION]))


# These 3 versions are known to be compatible, older versions should not be
# used, odds are 14 can be used as well - Will Sept 13 2022
POSTGRES_COMPATIBLE_MAJOR_VERSIONS = ['11', '12', '13', '14']

Base = declarative_base()

# baked queries allow for caching of query construction to save Python overhead
bakery = baked.bakery()
baked_query_resource = bakery(lambda session: session.query(Resource))
baked_query_unique_key = bakery(
    lambda session: session.query(Key).options(
        # This formerly called orm.joinedload_all, but that function has been deprecated since sqlalchemy 0.9.
        # The advice in the documentation was to just use orm.joinedload in apparently the same way. -kmp 11-May-2020
        # Ref: https://docs.sqlalchemy.org/en/13/orm/loading_relationships.html#sqlalchemy.orm.joinedload_all
        # OK, well, I had misread the doc, which agrees with the release notes for sqlalchemy 0.9 (when the change
        # was made), both say to use a chain. No advice is given about the keyword arguments (innerjoin=True),
        # but I assume they must be distributed to each such call. It's possible the right result was happening
        # anyway, but that it took more queries to get that result when not chained properly.
        # -kmp 14-May-2020
        # Ref: https://docs.sqlalchemy.org/en/13/changelog/migration_09.html
        orm.joinedload(Key.resource, innerjoin=True)
           .joinedload(Resource.data, innerjoin=True)
           .joinedload(CurrentPropertySheet.propsheet, innerjoin=True)
    ).filter(Key.name == bindparam('name'), Key.value == bindparam('value'))
)
# Baked queries can be used with expanding params (lists)
# https://docs.sqlalchemy.org/en/latest/orm/extensions/baked.html#baked-in
baked_query_sids = bakery(lambda session: session.query(CurrentPropertySheet))
baked_query_sids += lambda q: q.filter(CurrentPropertySheet.rid.in_(bindparam('rids', expanding=True)))


class PickStorage(object):
    """
    Class that directs storage methods to write storage (RDBStorage) or read
    storage (ElasticSearchStorage)
    """
    def __init__(self, write, read, registry):
        self.write = write
        self.read = read
        self.registry = registry
        self.used_datastores = {
            'database': self.write,
            'elasticsearch': self.read
        }

    def storage(self, datastore=None):
        """
        Choose which storage to use.
        1. First check `request.datastore` to see if using self.read
        2. Check `datastore` parameter to see if a certain storage is forced
        3. If neither 1. or 2. result in a storage, use self.write
        """
        # usually check the datastore attribute on request (set on GET/HEAD)
        request = get_current_request()
        if self.read is not None and request and request.datastore == 'elasticsearch':
            return self.read

        # check the datastore specified by Connection (not always used)
        if datastore is not None:
            if datastore in self.used_datastores:
                if self.used_datastores[datastore] is None:
                    raise HTTPInternalServerError('Forced datastore %s is not'
                                                  ' configured' % datastore)
                return self.used_datastores[datastore]
            else:
                raise HTTPInternalServerError('Invalid forced datastore %s. Must be one of: %s'
                                              % (datastore, list(self.used_datastores.keys())))
        # return write as a fallback
        return self.write

    def get_by_uuid(self, uuid, datastore=None):
        """
        Get write/read model by uuid
        """
        storage = self.storage(datastore)
        model = storage.get_by_uuid(uuid)
        # unless forcing ES datastore, check write storage if not found in read
        # if datastore == 'database' and storage is self.read:
        # Old is above - See C4-30
        # if not specifically specifying datastore=elasticsearch, always fall back to DB
        if not datastore == 'elasticsearch':
            if model is None:
                return self.write.get_by_uuid(uuid)
        return model

    def get_by_unique_key(self, unique_key, name, datastore=None, item_type=None):
        """
        Get write/read model by given unique key with value (name)
        """
        storage = self.storage(datastore)
        model = storage.get_by_unique_key(unique_key, name, item_type=item_type)
        # unless forcing ES datastore, check write storage if not found in read
        # if datastore == 'database' and storage is self.read:
        # Old is above - See C4-30
        # if not specifically specifying datastore=elasticsearch, always fall back to DB
        if not datastore == 'elasticsearch':
            if model is None:
                return self.write.get_by_unique_key(unique_key, name)  # no need to pass item_type since it's write
        return model

    def get_by_json(self, key, value, item_type, default=None, datastore=None):
        """
        Get write/read model by given key:value
        """
        ignored(default)  # TODO: Is is right that this should be ignored? -kmp 7-Aug-2022
        storage = self.storage(datastore)
        model = storage.get_by_json(key, value, item_type)  # TODO: Should a default not be passed here? -kmp 7-Aug-2022
        # unless forcing ES datastore, check write storage if not found in read
        # if datastore == 'database' and storage is self.read:
        # Old is above - See C4-30
        # if not specifically specifying datastore=elasticsearch, always fall back to DB
        if not datastore == 'elasticsearch':
            if model is None:
                return self.write.get_by_json(key, value, item_type)
        return model

    def purge_uuid(self, rid, item_type=None):
        """
        Attempt to purge an item by given resource id (rid), completely
        removing all propsheets, links, and keys from the DB. If read storage
        is configured, will check to ensure no items link to the given item
        and also remove the given item from Elasticsearch
        """
        log.warning('PURGE: purging %s' % rid)
        proceed = True
        # requires ES for searching item links
        if self.read is not None:
            # model and max_sid are used later, in second `if self.read` block
            model = self.get_by_uuid(rid)
            if not item_type:
                item_type = model.item_type
            max_sid = model.max_sid
            links_to_item = self.find_uuids_linked_to_item(rid)
            if len(links_to_item) > 0:
                raise HTTPLocked(
                    detail="Cannot purge item as other items still link to it",
                    comment=links_to_item
                )

            # delete item from ES and mirrored ES, and queue reindexing
            proceed = self.read.purge_uuid(rid, item_type, max_sid)

        # delete the item from DB
        if proceed:
            self.write.purge_uuid(rid)
        else:
            raise HTTPInternalServerError('Deletion of rid %s unsuccessful in Elasticsearch, aborting deletion' % rid)

    def get_rev_links(self, model, rel, *item_types):
        """
        Return a list of reverse links for the given model and item types using
        a certain reverse link type (rel)
        """
        return self.storage().get_rev_links(model, rel, *item_types)

    def __iter__(self, *item_types):
        """
        Return a generator that yields all uuids for given item types
        """
        return self.storage().__iter__(*item_types)

    def __len__(self, *item_types):
        """
        Return an integer count of number of items among given item types
        """
        return self.storage().__len__(*item_types)

    def create(self, item_type, uuid):
        """
        Always use self.write to create Resource model. Responsible for
        generating the initial DB entries for the item, regardless of storage
        used
        """
        return self.write.create(item_type, uuid)

    def update(self, model, properties=None, sheets=None, unique_keys=None,
               links=None, datastore=None):
        """
        model should always be write storage.Resource. If storage used is read,
        then this will both update DB tables and the item properties in ES
        """
        storage = self.storage(datastore)
        if storage is self.read:
            # must update links and such in write RDS. However, don't update
            # properties and sheets, as those are exclusively stored in ES.
            # Still call `storage.update` below to update contents of ES doc
            self.write.update(model, {}, None, unique_keys, links)

        return storage.update(model, properties, sheets, unique_keys, links)

    def get_sids_by_uuids(self, uuids):
        """
        Return a dict containing current sids keyed by uuid for given uuids
        Only functional with self.write
        """
        return self.write.get_sids_by_uuids(uuids)

    def get_by_uuid_direct(self, uuid, item_type, default=None):
        """
        Get the ES document by uuid directly for Elasticsearch
        Only functional with self.read
        """
        if self.read is not None:
            # must pass registry for access to settings
            return self.read.get_by_uuid_direct(uuid, item_type)

        return self.write.get_by_uuid_direct(uuid, item_type, default)

    def find_uuids_linked_to_item(self, uuid):
        """
        Returns a list of info about other items linked to item with given uuid.
        See esstorage.ElasticSearchStorage.find_uuids_linked_to_item
        Only functional with self.read
        """
        if self.read is not None:
            return self.read.find_uuids_linked_to_item(uuid)

        return self.write.find_uuids_linked_to_item(uuid)

    def revision_history(self, uuid):
        """
        Gets the revision history for the given uuid from postgres.
        """
        return self.write.revision_history(rid=uuid)


class RDBStorage(object):
    """
    Storage class used to interface with the relational database.
    Corresponds to PickStorage.write
    """
    batchsize = 1000

    def __init__(self, DBSession):
        self.DBSession = DBSession

    @property
    def write(self):
        return self

    @property
    def read(self):
        return self

    def get_by_uuid(self, rid, default=None):
        session = self.DBSession()
        model = baked_query_resource(session).get(uuid.UUID(rid))
        if model is None:
            return default
        return model

    def get_by_uuid_direct(self, rid, item_type, default=None):
        """
        This method is meant to only work with ES, so return None (default)
        for the DB implementation

        Args:
            rid (str): item rid (uuid)
            item_type (str): item_type of the target resource (Item.item_type)
            default: View to return on a failure. Defaults to None.

        Returns:
            default
        """
        ignored(rid)
        ignored(item_type)
        return default

    @classmethod
    def find_uuids_linked_to_item(cls, rid):
        """
        This method is meant to only work with ES, so return empty list for
        DB implementation. See ElasticSearchStorage.find_uuids_linked_to_item.
        """
        ignored(rid)
        return []

    def get_by_unique_key(self, unique_key, name, default=None, item_type=None):
        """ Postgres implementation of get_by_unique_key - Item type arg is not used here """
        ignored(item_type)  # TODO: unique keys are globally unique - could modify baked_query_unique_key to change this
        session = self.DBSession()
        try:
            key = baked_query_unique_key(session).params(name=unique_key, value=name).one()
        except NoResultFound:
            return default
        else:
            return key.resource

    def get_by_json(self, key, value, item_type, default=None):
        """ Postgres implementation of get_by_json (used for lookup keys) """
        session = self.DBSession()
        try:
            # baked query seem to not work with json
            query = (session.query(CurrentPropertySheet)
                     # Rewrittent to use two separate joins per SQLAlchemy 2.0 requirements. -kmp 10-Apr-2023
                     .join(CurrentPropertySheet.propsheet)
                     .join(CurrentPropertySheet.resource)
                     .filter(Resource.item_type == item_type,
                             PropertySheet.properties[key].astext == value)
                     )
            data = query.one()
            return data.resource
        except (NoResultFound, MultipleResultsFound):
            return default

    def get_rev_links(self, model, rel, *item_types):
        if item_types:
            return [
                link.source_rid for link in model.revs
                if link.rel == rel and link.source.item_type in item_types]
        else:
            return [link.source_rid for link in model.revs if link.rel == rel]

    def get_sids_by_uuids(self, rids):
        """
        Take a list of rids and return the sids from all of them using the
        CurrentPropertySheet table. This follows the convention of only using
        Resources with the default '' name.

        Args:
            rids (list): list of string rids (uuids)

        Returns:
            dict keyed by rid with integer sid values
        """
        if not rids:
            return []
        session = self.DBSession()
        results = baked_query_sids(session).params(rids=rids).all()
        # check res.name to skip sids for supplementary rows, like 'downloads'
        data = {str(res.rid): res.sid for res in results if res.name == ''}
        return data

    def get_max_sid(self):
        """
        Return the current max sid from the `current_propsheet` table.
        Not specific to a given uuid (i.e. rid). If no sid found, return 0

        Returns:
            int: maximum sid found
        """
        session = self.DBSession()
        # first element of the first result or None if no rows present.
        # If multiple rows are returned, raises MultipleResultsFound.
        data = session.query(func.max(CurrentPropertySheet.sid)).scalar() or 0
        return data

    def __iter__(self, *item_types):
        session = self.DBSession()
        query = session.query(Resource.rid)

        if item_types:
            query = query.filter(
                Resource.item_type.in_(item_types)
            )

        for rid, in query.yield_per(self.batchsize):
            yield rid

    def __len__(self, *item_types):
        session = self.DBSession()
        query = session.query(Resource.rid)

        if item_types:
            query = query.filter(
                Resource.item_type.in_(item_types)
            )

        return query.count()

    def create(self, item_type, rid):
        return Resource(item_type, rid=rid)

    def update(self, model, properties=None, sheets=None, unique_keys=None, links=None):
        session = self.DBSession()
        sp = session.begin_nested()
        try:
            session.add(model)
            self._update_properties(model, properties, sheets)
            if links is not None:
                self._update_rels(model, links)
            if unique_keys is not None:
                keys_add, keys_remove = self._update_keys(model, unique_keys)
            sp.commit()
            return
        except (IntegrityError, FlushError):
            sp.rollback()

        # Try again more carefully
        try:
            session.add(model)
            self._update_properties(model, properties, sheets)
            if links is not None:
                self._update_rels(model, links)
            session.flush()
        except (IntegrityError, FlushError) as e:
            raw_error_msg = get_error_message(e)
            log.error(raw_error_msg)
            msg = 'Cannot update because of one or more conflicting (or undefined) UUIDs'
            raise HTTPConflict(msg)
        assert unique_keys is not None
        # Formerly session.query(Key).get(pk), rewritten for SA2.0
        conflicts = [pk for pk in keys_add if session.get(Key, pk) is not None]
        assert conflicts
        msg = 'Keys conflict: %r' % conflicts
        raise HTTPConflict(msg)

    def purge_uuid(self, rid):
        # WARNING USE WITH CARE PERMANENTLY DELETES RESOURCES
        session = self.DBSession()
        sp = session.begin_nested()
        model = self.get_by_uuid(rid)
        try:
            for current_propsheet in model.data.values():
                # delete the propsheet history
                for propsheet in current_propsheet.history:
                    session.delete(propsheet)
                    # now delete the currentPropsheet
                    session.delete(current_propsheet)
                # now delete the resource, keys and links(via cascade)
            session.delete(model)
            sp.commit()
        except Exception as e:
            sp.rollback()
            raise e

    def _update_properties(self, model, properties, sheets=None):
        if properties is not None:
            model.propsheets[''] = properties
        if sheets is not None:
            for key, value in sheets.items():
                model.propsheets[key] = value

    def _update_keys(self, model, unique_keys):
        keys_set = {(k, v) for k, values in unique_keys.items() for v in values}

        existing = {
            (key.name, key.value)
            for key in model.unique_keys
        }

        to_remove = existing - keys_set
        to_add = keys_set - existing

        session = self.DBSession()
        for pk in to_remove:
            key = session.get(Key, pk)  # formerly session.query(Key).get(pk), rewritten for SA2.0
            session.delete(key)

        for name, value in to_add:
            key = Key(rid=model.rid, name=name, value=value)
            session.add(key)

        return to_add, to_remove

    def _update_rels(self, model, links):
        session = self.DBSession()
        source = model.rid

        rels = {(k, uuid.UUID(target)) for k, targets in links.items() for target in targets}

        existing = {
            (link.rel, link.target_rid)
            for link in model.rels
        }

        to_remove = existing - rels
        to_add = rels - existing

        for rel, target in to_remove:
            # formerly link = session.query(Link).get((source, rel, target)), rewritten for SA2.0
            link = session.get(Link, (source, rel, target))
            session.delete(link)

        for rel, target in to_add:
            link = Link(source_rid=source, rel=rel, target_rid=target)
            session.add(link)

        return to_add, to_remove

    def revision_history(self, *, rid):
        """ Computes revision history of rid by returning all propsheets. """
        session = self.DBSession
        revisions = []
        for revision in session.query(PropertySheet).filter_by(rid=rid).order_by(PropertySheet.sid):
            revision.properties['sid'] = revision.sid
            revisions.append(revision.properties)
        return revisions


class UUID(types.TypeDecorator):
    """Platform-independent UUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    cache_ok = True  # allow caching of uuids (avoid repeated string building)
    impl = types.CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(postgresql.UUID())
        else:
            return dialect.type_descriptor(types.CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return uuid.UUID(value).hex
            else:
                return value.hex

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)


# TODO: refactor to wrap blob storage in common API, so we do not need
# to pass args like kms_key_id in this class
class RDBBlobStorage(object):
    """ Handlers to blobs we store in RDB """
    def __init__(self, DBSession):
        self.DBSession = DBSession

    def store_blob(self, data, download_meta, blob_id=None):
        """ Initializes a db session and stores the data

        Args:
            data: raw attachment data
            download_meta: metadata associated with 'data',
                not actually used unless the caller wants to retain the blob_id
            blob_id: optional arg specifying the id, will be generated if not provided
        """
        if blob_id is None:
            blob_id = uuid.uuid4()
        elif isinstance(blob_id, str):
            blob_id = uuid.UUID(blob_id)
        session = self.DBSession()
        blob = Blob(blob_id=blob_id, data=data)
        session.add(blob)
        download_meta['blob_id'] = str(blob_id)

    def get_blob(self, download_meta):
        """ Gets a blob given from RDB

        Args:
            download_meta: metadata associated with the blob, all that is required
            is an entry for 'blob_id'

        Returns:
            data from the DB
        """
        blob_id = download_meta['blob_id']
        if isinstance(blob_id, str):
            blob_id = uuid.UUID(blob_id)
        session = self.DBSession()
        blob = session.get(Blob, blob_id)  # was session.query(Blob).get(blob_id), rewritten for SA2.0
        return blob.data


class S3BlobStorage(object):
    """ Handler to blobs we store in S3 """
    def __init__(self, bucket, kms_key_id=None):
        self.bucket = bucket
        self.kms_key_id = kms_key_id
        session = boto3.session.Session(region_name='us-east-1')
        self.s3 = session.client('s3', config=Config(signature_version='s3v4'))

    def store_blob(self, data, download_meta, blob_id=None):
        """
        Create a new s3 key = blob_id
        upload the contents and return the meta in download_meta

        Args:
            data: raw blob to store
            download_meta: unused beyond setting some meta data fields
            blob_id: optional ID if you want to provide it, one will be generated
        """
        if blob_id is None:
            blob_id = str(uuid.uuid4())

        content_type = download_meta.get('type', 'binary/octet-stream')
        put_kwargs = dict(
            Bucket=self.bucket,
            Key=blob_id,
            Body=data,
            ContentType=content_type
        )
        if self.kms_key_id:
            put_kwargs.update({
                'ServerSideEncryption': 'aws:kms',
                'SSEKMSKeyId': self.kms_key_id
            })
        self.s3.put_object(**put_kwargs)
        download_meta['bucket'] = self.bucket
        download_meta['key'] = blob_id
        download_meta['blob_id'] = str(blob_id)

    def _get_bucket_key(self, download_meta):
        """ Helper for the below two methods """
        if 'bucket' in download_meta:
            resolved_bucket = download_meta['bucket']
            if resolved_bucket != self.bucket:
                log.error(f'Bucket mismatch found with blobs, overriding metadata and using bucket {self.bucket}')
                resolved_bucket = self.bucket
            return resolved_bucket, download_meta['key']
        else:
            return self.bucket, download_meta['blob_id']

    def get_blob_url(self, download_meta):
        """ Locates a blob on S3 storage

        Args:
            download_meta: dictionary containing meta data, can specify the bucket
            itself if it stored elsewhere otherwise defaults to self.bucket and
            the blob_id

        Returns:
            url to the data
        """
        bucket_name, key = self._get_bucket_key(download_meta)
        location = self.s3.generate_presigned_url(
            ClientMethod='get_object',
            ExpiresIn=36*60*60,
            Params={'Bucket': bucket_name, 'Key': key})
        return location

    def get_blob(self, download_meta):
        """ Locates and gets a blob on S3 storage

        Args:
            download_meta: see above

        Returns:
            data from S3
        """
        bucket_name, key = self._get_bucket_key(download_meta)
        response = self.s3.get_object(Bucket=bucket_name,
                                      Key=key)
        return response['Body'].read().decode()


class Key(Base):
    """
    indexed unique tables for accessions and other unique keys
    """
    __tablename__ = 'keys'

    # typically the field that is unique, i.e. accession
    # might be prefixed with a namespace for per name unique values
    name = Column(types.String, primary_key=True)
    # the unique value
    value = Column(types.String, primary_key=True)

    rid = Column(UUID, ForeignKey('resources.rid'),
                 nullable=False, index=True)

    # Be explicit about dependencies to the ORM layer
    resource = orm.relationship('Resource',
                                backref=backref('unique_keys', cascade='all, delete-orphan'))


class Link(Base):
    """ indexed relations
    """
    __tablename__ = 'links'
    source_rid = Column(
        'source', UUID, ForeignKey('resources.rid'), primary_key=True)
    rel = Column(types.String, primary_key=True)
    target_rid = Column(
        'target', UUID, ForeignKey('resources.rid'), primary_key=True,
        index=True)  # Single column index for reverse lookup

    source = orm.relationship(
        'Resource', foreign_keys=[source_rid], backref=backref('rels',
                                                               cascade='all, delete-orphan'))
    target = orm.relationship(
        'Resource', foreign_keys=[target_rid], backref=backref('revs',
                                                               cascade='all, delete-orphan'))


class PropertySheet(Base):
    """
    A triple describing a resource - sid of time of writing, rid (uuid) and properties (json blob)
    """
    __tablename__ = 'propsheets'
    __table_args__ = (
        schema.ForeignKeyConstraint(
            ['rid', 'name'],
            ['current_propsheets.rid', 'current_propsheets.name'],
            name='fk_property_sheets_rid_name', use_alter=True,
            deferrable=True, initially='DEFERRED',
        ),
    )
    # The sid column also serves as the order.
    sid = Column(types.Integer, autoincrement=True, primary_key=True)
    # B-tree index on rid here greatly optimizes retrieval of revision history
    rid = Column(UUID,
                 ForeignKey('resources.rid',
                            deferrable=True,
                            initially='DEFERRED'),
                 nullable=False, index=True)
    name = Column(types.String, nullable=False)
    properties = Column(JSON)
    resource = orm.relationship('Resource')


class CurrentPropertySheet(Base):
    """
    Table that optimizes access to most recent version of items
    Also a triple of rid, name (unique keys) and sid
    """
    __tablename__ = 'current_propsheets'
    rid = Column(UUID, ForeignKey('resources.rid'),
                 nullable=False, primary_key=True)
    name = Column(types.String, nullable=False, primary_key=True)
    # B-tree index on sid should optimize retrieval of max_sid (and all sids)
    sid = Column(types.Integer, ForeignKey('propsheets.sid'), nullable=False, index=True)
    propsheet = orm.relationship(
        'PropertySheet', lazy='joined', innerjoin=True,
        primaryjoin="CurrentPropertySheet.sid==PropertySheet.sid",
    )
    history = orm.relationship(
        'PropertySheet', order_by=PropertySheet.sid,
        post_update=True,  # Break cyclic dependency
        primaryjoin="""and_(CurrentPropertySheet.rid==PropertySheet.rid,
                    CurrentPropertySheet.name==PropertySheet.name)""",
        viewonly=True,
    )
    resource = orm.relationship('Resource', back_populates='data')
    __mapper_args__ = {'confirm_deleted_rows': False}


class Resource(Base):
    """
    Resources are described by multiple propsheets
    """
    used_datastore = 'database'
    __tablename__ = 'resources'
    rid = Column(UUID, primary_key=True)
    item_type = Column(types.String, nullable=False)
    data = orm.relationship(
        'CurrentPropertySheet', cascade='all, delete-orphan',
        innerjoin=True, lazy='joined',
        collection_class=collections.attribute_mapped_collection('name'),
        back_populates='resource'
    )

    def __init__(self, item_type, data=None, rid=None):
        if rid is None:
            rid = uuid.uuid4()
        super(Resource, self).__init__(item_type=item_type, rid=rid)
        if data is not None:
            for k, v in data.items():
                self.propsheets[k] = v

    def __getitem__(self, key):
        return self.data[key].propsheet.properties

    def __setitem__(self, key, value):
        current = self.data.get(key, None)
        if current is None:
            self.data[key] = current = CurrentPropertySheet(name=key, rid=self.rid)
        propsheet = PropertySheet(name=key, properties=value, rid=self.rid)
        current.propsheet = propsheet

    def keys(self):
        return self.data.keys()

    def items(self):
        for k in self.keys():
            yield k, self[k]

    def get(self, key, default=None):
        try:
            return self.propsheets[key]
        except KeyError:
            return default

    @property
    def properties(self):
        return self.propsheets['']

    @property
    def propsheets(self):
        return self

    @property
    def uuid(self):
        return self.rid

    @property
    def sid(self):
        """
        In some cases there may be more than one sid, but we care about the
        primary one (with '' key)
        """
        return self.data[''].sid

    @property
    def max_sid(self):
        """
        See `RDBStorage.get_max_sid`
        """
        return _DBSESSION.query(func.max(CurrentPropertySheet.sid)).scalar() or 0

    def used_for(self, item):
        pass


class Blob(Base):
    """ Binary data
    """
    __tablename__ = 'blobs'
    blob_id = Column(UUID, primary_key=True)
    data = Column(types.LargeBinary)


def hash_password(password):
    ignored(password)
    raise NotImplementedError('Should not be calling this function')


class User(Base):
    """
    Application's user model.  Use this if you want to store / manage your own user auth
    """
    __tablename__ = 'users'
    user_id = Column(types.Integer, autoincrement=True, primary_key=True)
    name = Column(types.Unicode(60))
    email = Column(types.Unicode(60), unique=True)

    _password = Column('password', types.Unicode(60))

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        self._password = hash_password(password)

    def __init__(self, email, password, name):
        self.name = name
        self.email = email
        self.password = password

    @classmethod
    def get_by_username(cls, email):
        return _DBSESSION.query(cls).filter(cls.email == email).first()

    @classmethod
    def check_password(cls, email, password):
        raise NotImplementedError('This should never happen as we do not manage passwords.')
