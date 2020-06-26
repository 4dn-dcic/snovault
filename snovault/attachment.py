import magic
import mimetypes
import uuid

from base64 import b64decode
from hashlib import md5
from io import BytesIO
from mimetypes import guess_type
from PIL import Image
from pyramid.httpexceptions import HTTPNotFound, HTTPFound
from pyramid.response import Response
from pyramid.traversal import find_root
from pyramid.view import view_config
from structlog import getLogger
from urllib.parse import quote, unquote

from .resources import Item
from .interfaces import BLOBS

from .util import debug_log
from .validation import ValidationFailure


def includeme(config):
    config.scan(__name__)


log = getLogger(__name__)


class ItemWithAttachment(Item):
    """
    Item base class with attachment blob.
    Handles validation, storage, and downloading of attachments given in the
    `attachment` field of the given item.

    NOTE: in production, the datastore we use is S3BlobStorage
    """

    @staticmethod
    def parse_data_uri(uri):
        """ Proceses a raw download string returning the mime_type, charset
            and raw data associated with this download

        Args:
            uri: raw download string, see test_attachment.py for format

        Returns:
            mime_type: type of download, such as 'image/png'
            charset: set of characters associated with this encoding, if necessary
            but for the most part is unused
            data: decoded raw image data
        """
        if not uri.startswith('data:'):
            raise ValueError(uri)
        meta, data = uri[len('data:'):].split(',', 1)
        meta = meta.split(';')
        mime_type = meta[0] or None
        charset = None
        is_base64 = False

        # figure out encoding
        for part in meta[1:]:
            if part == 'base64':
                is_base64 = True
                continue
            if part.startswith('charset='):
                charset = part[len('charset='):]
                continue
            raise ValueError(uri)

        # decode as appropriate
        if is_base64:
            data = b64decode(data)
        else:
            data = unquote(data)

        return mime_type, charset, data

    def mimetypes_are_equal(self, m1, m2):
        """ Checks that mime_type m1 and m2 are equal """
        major1 = m1.split('/')[0]
        major2 = m2.split('/')[0]
        if major1 == 'text' and major2 == 'text':
            return True
        return m1 == m2

    def _process_downloads(self, prop_name, properties, downloads):
        """ Processes, validates and stores the download in RDBS blob storage
            Helper for _update

        Args:
            prop_name: name of property containing attachment
            properties: item properties where property containing attachment is
            located
            downloads: metadata for downloads, used by _update and reset to {}
            for the given prop_name
        """
        attachment = properties[prop_name]
        href = attachment['href']

        # verify data format
        if not href.startswith('data:'):
            msg = "Expected data URI."
            raise ValidationFailure('body', [prop_name, 'href'], msg)

        properties[prop_name] = attachment = attachment.copy()
        download_meta = downloads[prop_name] = {}

        # parse the data
        try:
            mime_type, charset, data = self.parse_data_uri(href)
        except (ValueError, TypeError):
            msg = 'Could not parse data URI.'
            raise ValidationFailure('body', [prop_name, 'href'], msg)
        if charset is not None:
            download_meta['charset'] = charset

        # Make sure the file extensions matches the mimetype
        download_meta['download'] = filename = attachment['download']
        mime_type_from_filename, _ = mimetypes.guess_type(filename)
        if mime_type_from_filename is None:
            mime_type_from_filename = 'application/octet-stream'
        if mime_type:
            if not self.mimetypes_are_equal(mime_type, mime_type_from_filename):
                raise ValidationFailure(
                    'body', [prop_name, 'href'],
                    'Wrong file extension for %s mimetype.' % mime_type)
        else:
            mime_type = mime_type_from_filename

        # Make sure the mimetype appears to be what the client says it is
        # for python3 magic.from_buffer always returns a string a thus doesn't need to be decoded
        try:
            mime_type_detected = magic.from_buffer(data, mime=True).decode('utf-8')
        except AttributeError:
            mime_type_detected = magic.from_buffer(data, mime=True)

        if not self.mimetypes_are_equal(mime_type, mime_type_detected):
            msg = "Incorrect file type. (Appears to be %s)" % mime_type_detected
            raise ValidationFailure('body', [prop_name, 'href'], msg)

        attachment['type'] = mime_type
        if mime_type is not None:
            download_meta['type'] = mime_type

        # Make sure mimetype is not disallowed
        try:
            allowed_types = self.schema['properties'][prop_name]['properties']['type']['enum']
        except KeyError:
            pass
        else:
            if mime_type not in allowed_types:
                raise ValidationFailure(
                    'body', [prop_name, 'href'], 'Mimetype %s is not allowed.' % mime_type)

        # Validate images and store height/width
        major, minor = mime_type.split('/')
        if major == 'image' and minor in ('png', 'jpeg', 'gif', 'tiff'):
            stream = BytesIO(data)
            im = Image.open(stream)
            im.verify()
            attachment['width'], attachment['height'] = im.size

        # Validate md5 sum
        md5sum = md5(data).hexdigest()
        if 'md5sum' in attachment and attachment['md5sum'] != md5sum:
            raise ValidationFailure(
                'body', [prop_name, 'md5sum'], 'MD5 checksum does not match uploaded data.')
        else:
            download_meta['md5sum'] = attachment['md5sum'] = md5sum

        # store blob in blobstorage
        registry = find_root(self).registry
        blob_id = str(uuid.uuid4())
        registry[BLOBS].store_blob(data, download_meta, blob_id)

        attachment['href'] = '@@download/%s/%s' % (prop_name, quote(filename))
        attachment['blob_id'] = blob_id

    def _update(self, properties, sheets=None):
        """ Updates an attachment in the S3BlobStorage using the above method
            as a helper, then propagates the change by calling the parent update

        Args:
            properties: props of the item we are updating
            sheets: any extra entries in propsheets used by the item that are
            separate from normal properties. See file.py:486
        """
        changed = []
        unchanged = []
        removed = []  # used if you remove an attachment property
        forced = []  # allow POST/PATCH of already uploaded attachment info

        # detect changes to any attachments on this item
        for prop_name, prop in self.schema['properties'].items():
            if not prop.get('attachment', False):
                continue

            if prop_name not in properties:
                if prop_name in self.propsheets.get('downloads', {}):
                    removed.append(prop_name)
                continue

            attachment = properties[prop_name]
            if 'href' not in attachment:
                msg = "Expected data uri or existing uri."
                raise ValidationFailure('body', [prop_name, 'href'], msg)

            href = attachment['href']
            if href.startswith('@@download/'):
                try:
                    existing = self.properties[prop_name]['href']
                except KeyError:
                    existing = None
                if existing and existing != href:
                    msg = "Expected data uri or existing uri."
                    raise ValidationFailure('body', [prop_name, 'href'], msg)
                if self.propsheets.get('downloads', {}).get(prop_name):
                    # there is already a propsheet present with same href
                    unchanged.append(prop_name)
                else:
                    # @@download href was provided externally. attempt
                    # to set the value we've got in download propsheet
                    forced.append(prop_name)
            else:
                changed.append(prop_name)

        # process the changes, if any, keeping 'downloads' up to date
        if changed or unchanged or forced:
            properties = properties.copy()
            sheets = {} if sheets is None else sheets.copy()
            sheets['downloads'] = downloads = {}
            for prop_name in unchanged:
                downloads[prop_name] = self.propsheets['downloads'][prop_name]
            for prop_name in changed:
                # hrefs for these attachments are raw data URIs
                self._process_downloads(prop_name, properties, downloads)
            for prop_name in forced:
                downloads[prop_name] = attachment
        super(ItemWithAttachment, self)._update(properties, sheets)


@view_config(name='download', context=ItemWithAttachment, request_method='GET',
             permission='view', subpath_segments=2)
@debug_log
def download(context, request):
    prop_name, filename = request.subpath
    try:
        downloads = context.propsheets['downloads']
    except KeyError:
        raise HTTPNotFound("Cannot find downloads propsheet. Update item.")
    try:
        download_meta = downloads[prop_name]
    except KeyError:
        raise HTTPNotFound(prop_name)

    if download_meta['download'] != filename:
        raise HTTPNotFound(filename)

    mimetype, content_encoding = guess_type(filename, strict=False)
    if mimetype is None:
        mimetype = 'application/octet-stream'

    # If blob is on s3, redirect us there
    blob_storage = request.registry[BLOBS]
    if hasattr(blob_storage, 'get_blob_url'):
        blob_url = blob_storage.get_blob_url(download_meta)
        raise HTTPFound(location=str(blob_url))

    # Otherwise serve the blob data ourselves
    blob = blob_storage.get_blob(download_meta)
    headers = {
        'Content-Type': mimetype,
    }
    return Response(body=blob, headers=headers)
