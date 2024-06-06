# -*- coding: utf-8 -*-
"""Load collections and determine the order."""

from base64 import b64encode
import gzip
import json
import magic
import mimetypes
import os
from PIL import Image
import re
import structlog
import traceback
from typing import Callable, List, Optional, Tuple, Union
from webtest import TestApp
from webtest.response import TestResponse as TestAppResponse
import uuid
from pyramid.paster import get_app
from pyramid.response import Response
from pyramid.router import Router
from pyramid.view import view_config
from dcicutils.data_readers import RowReader
from dcicutils.misc_utils import ignored, environ_bool, to_camel_case, VirtualApp
from dcicutils.submitr.progress_constants import PROGRESS_LOADXL as PROGRESS
from dcicutils.secrets_utils import assume_identity
from snovault.util import debug_log
from .schema_utils import get_identifying_and_required_properties
from .project_app import app_project
from .server_defaults_misc import add_last_modified


text = type(u'')
logger = structlog.getLogger(__name__)


def includeme(config):
    # provide an endpoint to do bulk uploading that just uses loadxl
    config.add_route('load_data', '/load_data')
    config.scan(__name__)


IS_ATTACHMENT = [
    'attachment',
    'file_format_specification',
]


# This uuid should be constant across all portals
LOADXL_USER_UUID = "3202fd57-44d2-44fb-a131-afb1e43d8ae5"


# order of items references with linkTo in a field in  'required' in schemas
def loadxl_order():
    # This should be set by the downstream application
    return app_project().loadxl_order()


class LoadGenWrapper(object):
    """
    Simple class that accepts a generator function and handles errors by
    setting self.caught to the error message.
    """
    def __init__(self, gen):
        self.gen = gen
        self.caught = None

    def __iter__(self):
        """
        Iterate through self.gen and see if 'ERROR: ' bytes are in any yielded
        value. If so, store the error message as self.caught and raise
        StopIteration to halt the generator.
        """
        # self.caught = yield from self.gen
        for iter_val in self.gen:
            if b'ERROR:' in iter_val:
                self.caught = iter_val.decode()
            yield iter_val

    def close(self):
        if self.caught:
            logger.error('load_data: failed to load with iter_response', error=self.caught)


@view_config(route_name='load_data', request_method='POST', permission='add')
@debug_log
def load_data_view(context, request):
    """
    expected input data

    {'local_path': path to a directory or file in file system
     'fdn_dir': inserts folder under encoded/tests/data
     'store': if not local_path or fdn_dir, look for a dictionary of items here
     'overwrite' (Bool): overwrite if existing data
     'itype': (list or str): only pick some types from the source or specify type in in_file
     'iter_response': invoke the Response as an app_iter, directly calling load_all_gen
     'config_uri': user supplied configuration file}

    post can contain 2 different styles of data
    1) reference to a folder or file (local_path or fd_dir). If this is done
       itype can be optionally used to specify type of items loaded from files
    2) store in form of {'item_type': [items], 'item_type2': [items]}
       item_type should be same as insert file names i.e. file_fastq
    """
    ignored(context)
    # this is a bit weird but want to reuse load_data functionality so I'm rolling with it
    config_uri = request.json.get('config_uri', 'production.ini')
    patch_only = request.json.get('patch_only', False)
    post_only = request.json.get('post_only', False)
    testapp = create_testapp(config_uri)
    # expected response
    request.response.status = 200
    result = {
        'status': 'success',
        '@type': ['result'],
    }
    store = request.json.get('store', {})
    local_path = request.json.get('local_path')
    fdn_dir = request.json.get('fdn_dir')
    overwrite = request.json.get('overwrite', False)
    itype = request.json.get('itype')
    iter_resp = request.json.get('iter_response', False)
    inserts = None
    from_json = False
    if fdn_dir:
        inserts = app_project().project_filename(os.path.join('tests/data/', fdn_dir) + '/')
    elif local_path:
        inserts = local_path
    elif store:
        inserts = store
        from_json = True
    # if we want to iterate over the response to keep the connection alive
    # this directly calls load_all_gen, instead of load_all
    if iter_resp:
        return Response(
            content_type='text/plain',
            app_iter=LoadGenWrapper(
                load_all_gen(testapp, inserts, None, overwrite=overwrite, itype=itype,
                             from_json=from_json, patch_only=patch_only, post_only=post_only)
            )
        )
    # otherwise, it is a regular view and we can call load_all as usual
    if inserts:
        res = load_all(testapp, inserts, None, overwrite=overwrite, itype=itype, from_json=from_json)
    else:
        res = 'No uploadable content found!'

    if res:  # None if load_all is successful
        print(LOAD_ERROR_MESSAGE)
        request.response.status = 422
        result['status'] = 'error'
        result['@graph'] = str(res)
    return result


def trim(value):
    """Shorten excessively long fields in error log."""
    if isinstance(value, dict):
        return {k: trim(v) for k, v in value.items()}
    if isinstance(value, list):
        return [trim(v) for v in value]
    if isinstance(value, str) and len(value) > 160:
        return value[:77] + '...' + value[-80:]
    return value


def find_doc(docsdir, filename):
    """tries to find the file, if not returns false."""
    path = None
    if not docsdir:
        return
    for dirpath in docsdir:
        candidate = os.path.join(dirpath, filename)
        if not os.path.exists(candidate):
            continue
        if path is not None:
            msg = 'Duplicate filenames: %s, %s' % (path, candidate)
            raise ValueError(msg)
        path = candidate
    if path is None:
        return
    return path


def attachment(path):
    """Create an attachment upload object from a filename Embeds the attachment as a data url."""
    filename = os.path.basename(path)
    mime_type, encoding = mimetypes.guess_type(path)
    major, minor = mime_type.split('/')
    try:
        detected_type = magic.from_file(path, mime=True).decode('ascii')
    except AttributeError:
        detected_type = magic.from_file(path, mime=True)
    # XXX This validation logic should move server-side.
    if not (detected_type == mime_type or
            detected_type == 'text/plain' and major == 'text'):
        raise ValueError('Wrong extension for %s: %s' % (detected_type, filename))
    with open(path, 'rb') as stream:
        attach = {'download': filename,
                  'type': mime_type,
                  'href': 'data:%s;base64,%s' % (mime_type, b64encode(stream.read()).decode('ascii'))}
        if mime_type in ('application/pdf', "application/zip", 'text/plain',
                         'text/tab-separated-values', 'text/html', 'application/msword', 'application/vnd.ms-excel',
                         'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
            # XXX Should use chardet to detect charset for text files here.
            return attach
        if major == 'image' and minor in ('png', 'jpeg', 'gif', 'tiff'):
            # XXX we should just convert our tiffs to pngs
            stream.seek(0, 0)
            im = Image.open(stream)
            im.verify()
            if im.format != minor.upper():
                msg = "Image file format %r does not match extension for %s"
                raise ValueError(msg % (im.format, filename))
            attach['width'], attach['height'] = im.size
            return attach
    raise ValueError("Unknown file type for %s" % filename)


def format_for_attachment(json_data, docsdir):
    for field in IS_ATTACHMENT:
        if field in json_data:
            if isinstance(json_data[field], dict):
                pass
            elif isinstance(json_data[field], str):
                path = find_doc(docsdir, json_data[field])
                if not path:
                    del json_data[field]
                    logger.error(f'Removing {field} form {json_data["uuid"]}, expecting path')
                else:
                    json_data[field] = attachment(path)
            else:
                # malformatted attachment
                del json_data[field]
                logger.error(f'Removing {field} form {json_data["uuid"]}, expecting path')
    return json_data


LOAD_ERROR_MESSAGE = """#   ██▓     ▒█████   ▄▄▄      ▓█████▄  ██▓ ███▄    █   ▄████
#  ▓██▒    ▒██▒  ██▒▒████▄    ▒██▀ ██▌▓██▒ ██ ▀█   █  ██▒ ▀█▒
#  ▒██░    ▒██░  ██▒▒██  ▀█▄  ░██   █▌▒██▒▓██  ▀█ ██▒▒██░▄▄▄░
#  ▒██░    ▒██   ██░░██▄▄▄▄██ ░▓█▄   ▌░██░▓██▒  ▐▌██▒░▓█  ██▓
#  ░██████▒░ ████▓▒░ ▓█   ▓██▒░▒████▓ ░██░▒██░   ▓██░░▒▓███▀▒
#  ░ ▒░▓  ░░ ▒░▒░▒░  ▒▒   ▓▒█░ ▒▒▓  ▒ ░▓  ░ ▒░   ▒ ▒  ░▒   ▒
#  ░ ░ ▒  ░  ░ ▒ ▒░   ▒   ▒▒ ░ ░ ▒  ▒  ▒ ░░ ░░   ░ ▒░  ░   ░
#    ░ ░   ░ ░ ░ ▒    ░   ▒    ░ ░  ░  ▒ ░   ░   ░ ░ ░ ░   ░
#      ░  ░    ░ ░        ░  ░   ░     ░           ░       ░
#                              ░
#   ██▓ ███▄    █   ██████ ▓█████  ██▀███  ▄▄▄█████▓  ██████
#  ▓██▒ ██ ▀█   █ ▒██    ▒ ▓█   ▀ ▓██ ▒ ██▒▓  ██▒ ▓▒▒██    ▒
#  ▒██▒▓██  ▀█ ██▒░ ▓██▄   ▒███   ▓██ ░▄█ ▒▒ ▓██░ ▒░░ ▓██▄
#  ░██░▓██▒  ▐▌██▒  ▒   ██▒▒▓█  ▄ ▒██▀▀█▄  ░ ▓██▓ ░   ▒   ██▒
#  ░██░▒██░   ▓██░▒██████▒▒░▒████▒░██▓ ▒██▒  ▒██▒ ░ ▒██████▒▒
#  ░▓  ░ ▒░   ▒ ▒ ▒ ▒▓▒ ▒ ░░░ ▒░ ░░ ▒▓ ░▒▓░  ▒ ░░   ▒ ▒▓▒ ▒ ░
#   ▒ ░░ ░░   ░ ▒░░ ░▒  ░ ░ ░ ░  ░  ░▒ ░ ▒░    ░    ░ ░▒  ░ ░
#   ▒ ░   ░   ░ ░ ░  ░  ░     ░     ░░   ░   ░      ░  ░  ░
#   ░           ░       ░     ░  ░   ░                    ░
#
#    █████▒▄▄▄       ██▓ ██▓    ▓█████ ▓█████▄
#  ▓██   ▒▒████▄    ▓██▒▓██▒    ▓█   ▀ ▒██▀ ██▌
#  ▒████ ░▒██  ▀█▄  ▒██▒▒██░    ▒███   ░██   █▌
#  ░▓█▒  ░░██▄▄▄▄██ ░██░▒██░    ▒▓█  ▄ ░▓█▄   ▌
#  ░▒█░    ▓█   ▓██▒░██░░██████▒░▒████▒░▒████▓
#   ▒ ░    ▒▒   ▓▒█░░▓  ░ ▒░▓  ░░░ ▒░ ░ ▒▒▓  ▒
#   ░       ▒   ▒▒ ░ ▒ ░░ ░ ▒  ░ ░ ░  ░ ░ ▒  ▒
#   ░ ░     ░   ▒    ▒ ░  ░ ░      ░    ░ ░  ░
#               ░  ░ ░      ░  ░   ░  ░   ░
#                                       ░                    """


def load_all(testapp, inserts, docsdir, overwrite=True, itype=None, from_json=False, patch_only=False, post_only=False,
             skip_types=None):
    """
    Wrapper function for load_all_gen, which invokes the generator returned
    from that function. Takes all of the same args as load_all_gen, so
    please reference that docstring.

    This function uses LoadGenWrapper, which will catch a returned value from
    the execution of the generator, which is an Exception in the case of
    load_all_gen. Return that Exception if encountered, which is consistent
    with the functionality of load_all_gen.
    """
    gen = LoadGenWrapper(
        load_all_gen(testapp, inserts, docsdir, overwrite, itype, from_json, patch_only, post_only, skip_types)
    )
    # run the generator; don't worry about the output
    for _ in gen:
        pass
    # gen.caught is None for success and an error message on failure
    if gen.caught is None:
        return None
    else:
        return Exception(gen.caught)


LOADXL_ALLOW_NONE = environ_bool("LOADXL_ALLOW_NONE", default=True)

def get_identifying_properties(item: dict, identifying_properties: list) -> List[str]:
    """
    Returns the list of all identifying properties of the given item which have a value,
    given its identifying properties (passed in from the identifyingProperties of the item's
    type schema). Favor uuid property and defavor aliases; no other ordering defined.
    """
    results = []
    if item.get("uuid"):
        results.append("uuid")
    for identifying_property in identifying_properties:
        if identifying_property not in ["aliases", "uuid"] and item.get(identifying_property) is not None:
            results.append(identifying_property)
    if "aliases" in identifying_properties and item.get("aliases") is not None:
        results.append("aliases")
    return results


def get_identifying_paths(item: dict, item_type: str, identifying_properties: list) -> List[str]:
    """
    Returns the Portal URL path for the first identifying property of the given item which has a
    value, given its identifying properties (passed in from the identifyingProperties of the item's
    type schema). Favor uuid property; no ordering defined for other identifying properties.
    """
    results = []
    for identifying_property in get_identifying_properties(item, identifying_properties):
        if (identifying_value := item.get(identifying_property)):
            if isinstance(identifying_value, list):
                for identifying_value_item in identifying_value:
                    results.append(f"/{item_type}/{identifying_value_item}")
            elif identifying_property == "uuid":
                results.append(f"/{identifying_value}")
            else:
                results.append(f"/{item_type}/{identifying_value}")
    return results


def get_identifying_property(item: dict, identifying_properties: list) -> Optional[str]:
    """
    Returns the first identifying property of the given item which has a value, given its
    identifying properties (passed in from the identifyingProperties of the item's type
    schema). Favor uuid property and defavor aliases; no other ordering defined.
    """
    identifying_properties = get_identifying_properties(item, identifying_properties)
    return identifying_properties[0] if len(identifying_properties) > 0 else None


def get_identifying_path(item: dict, item_type: str, identifying_properties: list) -> Optional[str]:
    identifying_paths = get_identifying_paths(item, item_type, identifying_properties)
    return identifying_paths[0] if identifying_paths else None


def get_identifying_value(item: dict, identifying_properties: list) -> Optional[str]:
    """
    Returns the value of the first identifying property of the given item which has a value,
    given its identifying properties (passed in from the identifyingProperties of the item's
    type schema). Favor uuid property and defavor aliases; no other ordering defined.
    """
    identifying_property = get_identifying_property(item, identifying_properties)
    if identifying_property:
        identifying_property_value = item.get(identifying_property)
        if isinstance(identifying_property_value, list) and len(identifying_property_value) > 0:
            return identifying_property_value[0]
        return identifying_property_value
    return None

def item_exists(app, an_item: dict, a_type: str, identifying_properties: list,
                progress: Optional[Callable] = None) -> Optional[str]:
    identifying_paths = get_identifying_paths(an_item, a_type, identifying_properties)
    if not identifying_paths:
        raise Exception("Item has no uuid nor any other identifying property; cannot GET.")
    for identifying_path in identifying_paths:
        try:
            # 301 because @id is the existing item path, not uuid
            # TODO: We we be able to do this part of the process more efficiently in bulk, up front.
            # ALSO: If we could use, for example, "/file-formats/vcf_gz/" (where the trailing slash
            # is important) rather than "/file_format/vcf_gz", we don't get a redirect (301) be get
            # a direct (200) response; but transformation of "file_format" to "file-formats" isn't
            # readily available to us here; and at least in this example (file_format), using the
            # uuid does not get us a direct (200) response, but rather a redirect (301); just FYI.
            progress(PROGRESS.GET) if progress else None
            existing_item = app.get(identifying_path, status=[200, 301])
            # If we get here then the item exists.
            existing_uuid = get_response_uuid(existing_item)
            return existing_uuid or get_identifying_value(an_item, identifying_properties)
        except Exception:
            # Ignoring exception here on purpose; this happens if the
            # item does not (yet) exist in the database, which is fine.
            pass
    return None


def get_response_uuid(response: TestAppResponse) -> Optional[str]:
    if not response:
        return None
    if response.status_code == 301:
        response = response.follow()
    return response.json.get("uuid") or response.json.get("@graph", [{}])[0].get("uuid")


def normalize_deleted_properties(data: dict) -> Tuple[dict, List[str]]:
    deleted_property_names = []
    # TODO: This is not doing it recursively; probably not needed.
    for property_name, property_value in data.items():
        if property_value == RowReader.CELL_DELETION_SENTINEL:
            deleted_property_names.append(property_name)
    for deleted_property_name in deleted_property_names:
        del data[deleted_property_name]
    return data, deleted_property_names


def load_all_gen(testapp, inserts, docsdir, overwrite=True, itype=None, from_json=False,
                 patch_only=False, post_only=False, skip_types=None, validate_only=False,
                 skip_links=False, continue_on_exception: bool = False, verbose=False,
                 progress=None):
    """
    Generator function that yields bytes information about each item POSTed/PATCHed.
    Is the base functionality of load_all function.

    convert data to store format dictionary (same format expected from from_json=True),
    assume main function is to load reasonable number of inserts from a folder

    Args:
        testapp
        inserts : either a folder, file, or a dictionary in the store format
        docsdir : attachment folder
        overwrite (bool)   : if the database contains the item already, skip or patch
        itype (list or str): limit selection to certain type/types
        from_json (bool)   : if set to true, inserts should be dict instead of folder name
        patch_only (bool)  : if set to true will only do second round patch - no posts
        post_only (bool)   : if set to true posts full item no second round or lookup -
                             use with care - will not work if linkTos to items not in db yet
        skip_types (list)  : if set to a list of item files the process will ignore these files
    Yields:
        Bytes with information on POSTed/PATCHed items

    Returns:
        None if successful, otherwise a bytes error message
    """
    if docsdir is None:
        docsdir = []
    progress = progress if callable(progress) else None
    # Collect Items
    store = {}
    if from_json:  # we are directly loading json
        store = inserts
    if not from_json:  # we are loading a file
        use_itype = False
        if os.path.isdir(inserts):  # we've specified a directory
            if not inserts.endswith('/'):
                inserts += '/'
            files = [i for i in os.listdir(inserts) if (i.endswith('.json') or i.endswith('.json.gz'))
                     and (i not in skip_types if skip_types else True)]
        elif os.path.isfile(inserts):  # we've specified a single file
            files = [inserts]
            # use the item type if provided AND not a list
            # otherwise guess from the filename
            use_itype = True if (itype and isinstance(itype, str)) else False
        else:  # cannot get the file
            err_msg = 'Failure loading inserts from %s. Could not find matching file or directory.' % inserts
            print(err_msg)
            yield str.encode(f'ERROR: {err_msg}\n')
            return
            # raise StopIteration
        # load from the directory/file
        for a_file in files:
            if use_itype:
                item_type = itype
            else:
                item_type = a_file.split('/')[-1].split(".")[0]
                a_file = inserts + a_file
            store[item_type] = get_json_file_content(a_file)

    # if there is a defined set of items, subtract the rest
    if itype:
        if isinstance(itype, list):
            store = {i: store[i] for i in itype if i in store}
        else:
            store = {itype: store.get(itype, [])}
    # clear empty values
    store = {k: v for k, v in store.items() if v is not None}
    if not store:
        if LOADXL_ALLOW_NONE:
            return
        if from_json:
            err_msg = 'No items found in input "store" json'
        else:
            err_msg = 'No items found in %s' % inserts
        if itype:
            err_msg += ' for item type(s) %s' % itype
        print(err_msg)
        yield str.encode(f'ERROR: {err_msg}')
        return
        # raise StopIteration
    # order Items
    all_types = list(store.keys())
    for ref_item in reversed(loadxl_order()):
        if ref_item in all_types:
            all_types.insert(0, all_types.pop(all_types.index(ref_item)))
    # collect schemas
    profiles = testapp.get('/profiles/?frame=raw').json

    def get_schema_info(type_name: str) -> (list, list):
        """
        Returns a tuple containing (first) the list of identifying properties and (second) the list
        of any required properties specified by the schema associated with the object of the given
        object type name. The schema is ASSUMED to be contained within the outer profiles dictionary
        variable, keyed by the camel-case version of the given object type name, which itself is
        assumed to be the snake-case version of the type name (though okay if already camel-case).
        See get_identifying_and_required_properties for details of how these fields are extracted.
        """
        schema = profiles[to_camel_case(type_name)]
        return get_identifying_and_required_properties(schema)

    progress(PROGRESS.START) if progress else None
    # run step1 - if item does not exist, post with minimal metadata (and skip indexing since we will patch
    # in round 2)
    second_round_items = {}
    if not patch_only:
        for a_type in all_types:
            first_fields = []
            # minimal schema
            identifying_properties, req_fields = get_schema_info(a_type)
            if not post_only:
                ids = identifying_properties
                # some schemas did not include aliases
                if 'aliases' not in ids:
                    ids.append('aliases')
                # file format is required for files, but its usability depends this field
                if a_type in ['file_format', 'experiment_type']:
                    req_fields.append('valid_item_types')
                first_fields = list(set(req_fields + ids))
            skip_existing_items = set()
            posted = 0
            skip_exist = 0
            for an_item in store[a_type]:
                progress(PROGRESS.ITEM) if progress else None
                existing_item_identifying_value = None
                if not post_only:
                    existing_item_identifying_value = item_exists(testapp, an_item, a_type,
                                                                  identifying_properties, progress=progress)
                    """
                    identifying_paths = get_identifying_paths(an_item, a_type, identifying_properties)
                    if not identifying_paths:
                        raise Exception("Item has no uuid nor any other identifying property; cannot GET.")
                    for identifying_path in identifying_paths:
                        try:
                            # 301 because @id is the existing item path, not uuid
                            # TODO: We we be able to do this part of the process more efficiently in bulk, up front.
                            # ALSO: If we could use, for example, "/file-formats/vcf_gz/" (where the trailing slash
                            # is important) rather than "/file_format/vcf_gz", we don't get a redirect (301) be get
                            # a direct (200) response; but transformation of "file_format" to "file-formats" isn't
                            # readily available to us here; and at least in this example (file_format), using the
                            # uuid does not get us a direct (200) response, but rather a redirect (301); just FYI.
                            existing_item = testapp.get(identifying_path, status=[200, 301])
                            # If we get here then the item exists.
                            exists = True
                            break
                        except Exception:
                            # Ignoring exception here on purpose; this happens if the
                            # item does not (yet) exist in the database, which is fine.
                            pass
                    """

                    """
                    identifying_path = get_identifying_path(an_item, a_type, identifying_properties)
                    if not identifying_path:
                        raise Exception("Item has no uuid nor any other identifying property; cannot GET.")
                    try:
                        # 301 because @id is the existing item path, not uuid
                        # TODO: We we be able to do this part of the process more efficiently in bulk, up front.
                        # ALSO: If we could use, for example, "/file-formats/vcf_gz/" (where the trailing slash
                        # is important) rather than "/file_format/vcf_gz", we don't get a redirect (301) be get
                        # a direct (200) response; but transformation of "file_format" to "file-formats" isn't
                        # readily available to us here; and at least in this example (file_format), using the
                        # uuid does not get us a direct (200) response, but rather a redirect (301); just FYI.
                        existing_item = testapp.get(identifying_path, status=[200, 301])
                        # If we get here then the item exists.
                        exists = True
                    except Exception:
                        # Ignoring exception here on purpose; this happens if the
                        # item does not (yet) exist in the database, which is fine.
                        pass
                    """
                # skip the items that exists
                # if overwrite=True, still include them in PATCH round
                if existing_item_identifying_value:
                    skip_exist += 1
                    identifying_value = existing_item_identifying_value
                    # identifying_value = (get_response_uuid(existing_item) or
                    #                      get_identifying_value(an_item, identifying_properties))
                    if not overwrite:
                        skip_existing_items.add(identifying_value)
                    if verbose and (filename := an_item.get("filename", "")):
                        filename = " " + filename
                    else:
                        filename = ""
                    if validate_only:
                        # 2024-02-21
                        # Discovered that in validation_only mode, if an item already exists,
                        # then it will not hit post_json (because it exists) and will not
                        # hit patch_json (because, well, because this fix was not here); it
                        # would not have made it into second_round_items because validate_only.
                        if validate_patch_path := get_identifying_path(an_item, a_type, identifying_properties):
                            validate_patch_path += "?check_only=true"
                            # To be safe we will unconditionally do skip_links in this case;
                            # to get away with not doing this would require more analysis.
                            # See: https://github.com/4dn-dcic/snovault/pull/283
                            validate_patch_path += "&skip_links=true"
                            try:
                                progress(PROGRESS.PATCH) if progress else None
                                testapp.patch_json(validate_patch_path, an_item)
                            except Exception as e:
                                progress(PROGRESS.ERROR) if progress else None
                                e_str = str(e).replace('\n', '')
                                yield str.encode(f"ERROR: {validate_patch_path} {e_str}")
                                if not continue_on_exception:
                                    progress(PROGRESS.DONE) if progress else None
                                    return
                    yield str.encode(f'SKIP: {identifying_value}{" " + a_type if verbose else ""}{filename}\n')
                else:
                    an_item, _ = normalize_deleted_properties(an_item)
                    if post_only:
                        to_post = an_item
                    else:
                        to_post = {key: value for (key, value) in an_item.items() if key in first_fields}
                    post_request = f'/{a_type}?skip_indexing=true'
                    if validate_only:
                        post_request += '&check_only=true'
                        if skip_links:
                            post_request += "&skip_links=true"
                    to_post = format_for_attachment(to_post, docsdir)
                    try:
                        # This creates the (as yet non-existent) item to the
                        # database with just the minimal data (first_fields).
                        progress(PROGRESS.POST) if progress else None
                        res = testapp.post_json(post_request, to_post)  # skip indexing in round 1
                        if not validate_only:
                            assert res.status_code == 201
                            posted += 1
                            # yield bytes to work with Response.app_iter
                            uuid = get_response_uuid(res)
                            if uuid and "uuid" not in an_item:
                                an_item["uuid"] = uuid  # update our item with the new uuid; maybe controversial?
                            identifying_value = (uuid or get_identifying_value(an_item, identifying_properties))
                            if verbose and (filename := an_item.get("filename", "")):
                                filename = " " + filename
                            else:
                                filename = ""
                            yield str.encode(f'POST: {identifying_value}{" " + a_type if verbose else ""}{filename}\n')
                        else:
                            assert res.status_code == 200
                            identifying_value = (get_response_uuid(res) or
                                                 get_identifying_value(an_item, identifying_properties))
                            yield str.encode(f'CHECK: {identifying_value}{" " + a_type if verbose else ""}\n')
                    except Exception as e:
                        progress(PROGRESS.ERROR) if progress else None
                        print('Posting {} failed. Post body:\n{}\nError Message:{}'
                              ''.format(a_type, str(first_fields), str(e)))
                        # remove newlines from error, since they mess with generator output
                        e_str = str(e).replace('\n', '')
                        try:
                            # 2024-02-13: To help out smaht-submitr refererential integrity
                            # checking, include the identifying path of the problematic object
                            message = f"ERROR: {get_identifying_path(an_item, a_type, identifying_properties)} {e_str}"
                        except Exception:
                            message = f"ERROR: {e_str}"
                        yield str.encode(f'{message}\n')
                        if not continue_on_exception:
                            progress(PROGRESS.DONE) if progress else None
                            return
                        # raise StopIteration
            if not validate_only:
                if not post_only:
                    second_round_items[a_type] = [i for i in store[a_type]
                                                  if get_identifying_value(i, identifying_properties)
                                                  not in skip_existing_items]
            else:
                second_round_items[a_type] = []
            logger.info('{} 1st: {} items posted, {} items exists.'.format(a_type, posted, skip_exist))
            logger.info('{} 1st: {} items will be patched in second round'
                        .format(a_type, str(len(second_round_items.get(a_type, [])))))
    elif overwrite and not post_only:
        logger.info('Posting round skipped')
        for a_type in all_types:
            second_round_items[a_type] = [i for i in store[a_type]]
            logger.info('{}: {} items will be patched in second round'
                        .format(a_type, str(len(second_round_items.get(a_type, [])))))

    progress(PROGRESS.START_SECOND_ROUND) if progress else None
    # Round II - patch the rest of the metadata (ensuring to index by not passing the query param)
    rnd = ' 2nd' if not patch_only else ''
    for a_type in all_types:
        identifying_properties, _ = get_schema_info(a_type)
        patched = 0
        if not second_round_items[a_type]:
            logger.info('{}{}: no items to patch'.format(a_type, rnd))
            continue
        for an_item in second_round_items[a_type]:
            progress(PROGRESS.ITEM_SECOND_ROUND) if progress else None
            an_item = format_for_attachment(an_item, docsdir)
            try:
                add_last_modified(an_item, userid=LOADXL_USER_UUID)
                identifying_path = get_identifying_path(an_item, a_type, identifying_properties)
                if not identifying_path:
                    raise Exception("Item has no uuid nor any other identifying property; cannot PATCH.")
                normalized_item, deleted_properties = normalize_deleted_properties(an_item)
                if deleted_properties:
                    if validate_only and skip_links:
                        identifying_path += f"?delete_fields={','.join(deleted_properties)}&skip_links=true"
                    else:
                        identifying_path += f"?delete_fields={','.join(deleted_properties)}"
                elif validate_only and skip_links:
                    identifying_path += f"?skip_links=true"
                progress(PROGRESS.PATCH) if progress else None
                res = testapp.patch_json(identifying_path, normalized_item)
                assert res.status_code == 200
                patched += 1
                # yield bytes to work with Response.app_iter
                identifying_value = (get_response_uuid(res) or
                                     get_identifying_value(an_item, identifying_properties) or
                                     "<unidentified>")
                if verbose and (filename := an_item.get("filename", "")):
                    filename = " " + filename
                else:
                    filename = ""
                yield str.encode(f'PATCH: {identifying_value}{" " + a_type if verbose else ""}{filename}\n')
            except Exception as e:
                progress(PROGRESS.ERROR) if progress else None
                print('Patching {} failed. Patch body:\n{}\n\nError Message:\n{}'.format(
                      a_type, str(an_item), str(e)))
                print('Full error: %s' % traceback.format_exc())
                e_str = str(e).replace('\n', '')
                try:
                    # 2024-02-13: To help out smaht-submitr refererential integrity
                    # checking, include the identifying path of the problematic object
                    message = f"ERROR: {get_identifying_path(an_item, a_type, identifying_properties)} {e_str}"
                except Exception:
                    message = f"ERROR: {e_str}"
                yield str.encode(f'{message}\n')
                if not continue_on_exception:
                    progress(PROGRESS.DONE) if progress else None
                    return
                # raise StopIteration
        logger.info('{}{}: {} items patched .'.format(a_type, rnd, patched))

    # explicit return upon finish
    progress(PROGRESS.DONE) if progress else None
    return None


def get_json_file_content(filename):
    """
    Helper function to obtain objects from (compressed) json files.

    :param filename: str file path
    :returns: object loaded from file
    """
    if filename.endswith(".json"):
        with open(filename) as f:
            result = json.loads(f.read())
    elif filename.endswith(".json.gz"):
        with gzip.open(filename) as f:
            result = json.loads(f.read())
    else:
        raise Exception("Expecting a .json or .json.gz file but found %s." % filename)
    return result


def load_data(app, indir='inserts', docsdir=None, overwrite=False,
              use_master_inserts=True, skip_types=None):
    """
    This function will take the inserts folder as input, and place them to the given environment.
    args:
        app:
        indir (inserts): inserts folder, should be relative to tests/data/
        docsdir (None): folder with attachment documents, relative to tests/data
    """
    testapp = create_testapp(app)
    # load master-inserts by default
    if indir != 'master-inserts' and use_master_inserts:
        master_inserts = app_project().project_filename('tests/data/master-inserts/')
        master_res = load_all(testapp, master_inserts, [], skip_types=skip_types)
        if master_res:  # None if successful
            print(LOAD_ERROR_MESSAGE)
            logger.error('load_data: failed to load from %s' % master_inserts, error=master_res)
            return master_res

    if not indir.endswith('/'):
        indir += '/'
    if not os.path.isabs(indir):
        inserts = app_project().project_filename(os.path.join('tests/data/', indir))
    else:
        inserts = indir
    if docsdir is None:
        docsdir = []
    else:
        if not docsdir.endswith('/'):
            docsdir += '/'
        docsdir = [app_project().project_filename(os.path.join('tests/data/', docsdir))]
    res = load_all(testapp, inserts, docsdir, overwrite=overwrite)
    if res:  # None if successful
        print(LOAD_ERROR_MESSAGE)
        logger.error('load_data: failed to load from %s' % docsdir, error=res)
        return res
    return None  # unnecessary, but makes it more clear that no error was encountered


def load_test_data(app, overwrite=False):
    """
    Load inserts and master-inserts

    Returns:
        None if successful, otherwise Exception encountered
    """
    return load_data(app, docsdir='documents', indir='inserts',
                     overwrite=overwrite)


def load_local_data(app, overwrite=False):
    """
    Load inserts from temporary insert folders, if present and populated
    with .json insert files.
    If not present, load inserts and master-inserts.

    Returns:
        None if successful, otherwise Exception encountered
    """

    test_insert_dirs = [
        'temp-local-inserts',
        'demo_inserts'
    ]

    for test_insert_dir in test_insert_dirs:
        chk_dir = app_project().project_filename(os.path.join("tests/data/", test_insert_dir))
        for (dirpath, dirnames, filenames) in os.walk(chk_dir):
            if any([fn for fn in filenames if fn.endswith('.json') or fn.endswith('.json.gz')]):
                logger.info('Loading inserts from "{}" directory.'.format(test_insert_dir))
                return load_data(app, docsdir='documents', indir=test_insert_dir, use_master_inserts=True,
                                 overwrite=overwrite)

    # Default to 'inserts' if no temp inserts found.
    return load_data(app, docsdir='documents', indir='inserts', use_master_inserts=True, overwrite=overwrite)


def load_prod_data(app, overwrite=False):
    """
    Load master-inserts

    Returns:
        None if successful, otherwise Exception encountered
    """
    return load_data(app, indir='master-inserts', overwrite=overwrite)


def load_deploy_data(app, overwrite=True, **kwargs):
    """
    Load deploy-inserts and master-inserts. Overwrites duplicate items
    in both directories to match deploy-inserts version.

    Returns:
        None if successful, otherwise Exception encountered
    """
    return load_data(app, docsdir='documents', indir="deploy-inserts", overwrite=True)


# Set of emails required by the application to function
REQUIRED_USER_CONFIG = [
    {
        'email': 'loadxl@hms.harvard.edu',
        'first_name': 'loadxl',
        'last_name': 'loadxl',
        'uuid': '3202fd57-44d2-44fb-a131-afb1e43d8ae5'
    },
    {
        'email': 'cgap.platform@gmail.com',
        'first_name': 'Platform',
        'last_name': 'Admin',
        'uuid': 'b5f738b6-455a-42e5-bc1c-77fbfd9b15d2'
    },
    {
        'email': 'foursight.app@gmail.com',
        'first_name': 'Foursight',
        'last_name': 'App',
        'uuid': '7677f8a8-79d2-4cff-ab0a-a967a2a68e39'
    },
    {
        'email': 'tibanna.app@gmail.com',
        'first_name': 'Tibanna',
        'last_name': 'App',
        'uuid': 'b041dba8-e2b2-4e54-a621-97edb508a0c4'
    },
]


def load_custom_data(app, overwrite=False):
    """
    Load deploy-inserts and master-inserts, EXCEPT instead of loading the default user.json,
    generate users (if they do not already exist) from the ENCODED_ADMIN_USERS setting in
    the GAC. We assume it has structure consistent with what the template will build in 4dn-cloud-infra
    ie:
        [{"first_name": "John", "last_name": "Doe", "email": "john_doe@example.com"}]
    """
    # start with the users
    testapp = create_testapp(app)
    identity = assume_identity()
    admin_users = json.loads(identity.get('ENCODED_ADMIN_USERS', '{}'))
    if not admin_users:  # we assume you must have set one of these
        print(LOAD_ERROR_MESSAGE)
        logger.error('load_custom_data: failed to load users as none were set - ensure GAC value'
                     ' ENCODED_ADMIN_USERS is set and formatted correctly!')
        return admin_users

    # post all users
    for user in (admin_users + REQUIRED_USER_CONFIG):
        try:
            first_name, last_name, email, _uuid = (user['first_name'], user['last_name'], user['email'],
                                                   user.get('uuid', str(uuid.uuid4())))
        except KeyError:
            print(LOAD_ERROR_MESSAGE)
            logger.error('load_custom_data: failed to load users as they were malformed - ensure GAC value'
                         ' ENCODED_ADMIN_USERS is set, has type array and consists of objects all containing keys'
                         ' and values for first_name, last_name and email!')
            return user
        item = {
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'groups': ['admin'],
            'uuid': _uuid
        }
        testapp.post_json('/User', item, status=201)

    res = load_data(app, docsdir='documents', indir='deploy-inserts', overwrite=overwrite, skip_types=['user.json'])
    if res:  # None if successful
        print(LOAD_ERROR_MESSAGE)
        logger.error('load_custom_data: failed to load from deploy-inserts', error=res)
        return res

    return None


def load_cypress_data(app, overwrite=False):
    """
    Load master-inserts and cypress-test-inserts.
    By default, does not overwrite duplicate items in both directories

    Returns:
        None if successful, otherwise Exception encountered
    """
    return load_data(app, indir='cypress-test-inserts', overwrite=overwrite)


def load_data_by_type(app, indir='master-inserts', overwrite=True, itype=None):
    """
    This function will load inserts of type itype from the indir directory.
    args:
        indir (inserts): inserts folder, should be relative to tests/data/
        itype: item type to load (e.g. "higlass_view_config")
    """

    if itype is None:
        print('load_data_by_type: No item type specified. Not loading anything.')
        return

    testapp = create_testapp(app)

    if not indir.endswith('/'):
        indir += '/'
    inserts = app_project().project_filename(os.path.join('tests/data/', indir))

    res = load_all(testapp, inserts, docsdir=[], overwrite=overwrite, itype=itype)
    if res:  # None if successful
        print(LOAD_ERROR_MESSAGE)
        logger.error('load_data_by_type: failed to load from %s' % indir, error=res)
        return res
    return None  # unnecessary, but makes it more clear that no error was encountered


def load_data_via_ingester(vapp: VirtualApp,
                           ontology: dict,
                           itype: Union[str, list] = ["ontology", "ontology_term"],
                           validate_only: bool = False) -> dict:
    """
    Entry point for call from encoded.ingester.processors.handle_ontology_update (2023-03-08).
    Returns dictionary itemizing the created (post), updated (patch), skipped (skip), checked (check),
    and errored (error) ontology term uuids; as well as a count of the number of unique uuids processed;
    the checked category is for validate_only;
    """
    response = load_all_gen(vapp, ontology, None, overwrite=True, itype=itype,
                            from_json=True, patch_only=False, validate_only=validate_only)
    results = {"post": [], "patch": [], "skip": [], "check": [], "error": []}
    unique_uuids = set()
    INGESTION_RESPONSE_PATTERN = re.compile(r"^([A-Z]+): ([0-9a-f-]+)$")
    for item in response:
        # Assume each item in the response looks something like one of (string or bytes):
        # POST: 15425d13-01ce-4e61-be5d-cd04401dff29
        # PATCH: 5b45e66f-7b4f-4923-824b-d0864a689bb
        # SKIP: 4efe24b5-eb17-4406-adb8-060ea2ae2180
        # CHECK: deadbeef-eb17-4406-adb8-0eacafebabe
        # ERROR: 906c4667-483e-4a08-96b9-3ce85ce8bf8c
        # Note that SKIP means skip post/insert; still may to patch/update (if overwrite).
        if isinstance(item, bytes):
            item = item.decode("ascii")
        elif not isinstance(item, str):
            logger.warning(f"load_data_via_ingester: skipping response item of unexpected type ({type(item)}): {item!r}")
            continue
        match = INGESTION_RESPONSE_PATTERN.match(item)
        if not match:
            logger.warning(f"load_data_via_ingester: skipping response item in unexpected form: {item!r}")
            continue
        action = match.group(1).lower()
        uuid = match.group(2)
        if not results.get(action):
            results[action] = []
        results[action].append(uuid)
        unique_uuids.add(uuid)
    results["unique"] = len(unique_uuids)
    return results


def create_testapp(ini_or_app_or_testapp: Union[str, Router, TestApp] = "development.ini",
                   app_name: str = "app") -> TestApp:
    """
    Creates and returns a TestApp; and also adds a get_with_follow method to it.
    Refactored out of above loadxl code (2023-09) to consolidate at a single point,
    and also for use by the generate_local_access_key and view_local_object scripts.
    """
    if isinstance(ini_or_app_or_testapp, TestApp):
        testapp = ini_or_app_or_testapp
    else:
        if isinstance(ini_or_app_or_testapp, Router):
            app = ini_or_app_or_testapp
        else:
            app = get_app(ini_or_app_or_testapp, app_name)
        testapp = TestApp(app, {"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST"})
    if not getattr(testapp, "get_with_follow", None):
        def get_with_follow(self, *args, **kwargs):
            raise_exception = kwargs.pop("raise_exception", True)
            if not isinstance(raise_exception, bool):
                raise_exception = True
            try:
                response = self.get(*args, **kwargs)
                if response and response.status_code in [301, 302, 303, 307, 308]:
                    response = response.follow()
                return response
            except Exception as e:
                if raise_exception:
                    raise e
                return None
        testapp.get_with_follow = get_with_follow.__get__(testapp, TestApp)
    return testapp
