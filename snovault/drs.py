from pyramid.view import view_config
from pyramid.exceptions import HTTPNotFound
from pyramid.response import Response
from dcicutils.misc_utils import ignored
from .util import debug_log


DRS_VERSION_1 = 'v1'
DRS_PREFIX_V1 = f'ga4gh/drs/{DRS_VERSION_1}'
DRS_OBJECT_GET = DRS_PREFIX_V1 + '/objects/{object_id}'
DRS_OBJECT_GET_ACCESS_URL = DRS_PREFIX_V1 + '/objects/{object_id}/access/{access_id}'
DRS_OBJECT_GET_ACCESSS_URL_SLASH = DRS_PREFIX_V1 + '/objects/{object_id}/access/'
DRS_OBJECT_GET_ACCESSS_URL_NO_SLASH = DRS_PREFIX_V1 + '/objects/{object_id}/access'
REQUIRED_FIELDS = [
    'id',
    'created_time',
    'self_uri',
    'size',
    'checksums'
]
ACCESS_METHOD_REQUIRED_FIELDS = [
    'access_url',
    'type'
]


def includeme(config):
    config.add_route('drs_objects', '/' + DRS_OBJECT_GET)
    config.add_route('drs_download', '/' + DRS_OBJECT_GET_ACCESS_URL)
    config.add_route('drs_download_slash', '/' + DRS_OBJECT_GET_ACCESSS_URL_SLASH)
    config.add_route('drs_download_no_slash', '/' + DRS_OBJECT_GET_ACCESSS_URL_NO_SLASH)
    config.scan(__name__)


def validate_drs_object(drs_object):
    """ Validates the structure of a drs object (required fields)
        Because we're not wrapping in any object-oriented structure, the internal API
        will call this and throw a validation error if the returned DRS object
        does not conform to structure.
    """
    for required_key in REQUIRED_FIELDS:
        assert required_key in drs_object
    if 'access_methods' in drs_object:
        for required_key in ACCESS_METHOD_REQUIRED_FIELDS:
            for access_method in drs_object['access_methods']:
                assert required_key in access_method


def get_and_format_drs_object(request, object_uri):
    """ Call request.embed on the object_uri and reformats it such that it fits the DRS
        format, returning access_methods etc as needed if it is a file
    """
    try:
        drs_object = request.embed(object_uri, '@@drs', as_user=True)
    except Exception:
        raise HTTPNotFound('You accessed a DRS object_uri that either does not exist'
                           ' or you do not have access to it.')
    uri = drs_object['id']
    drs_object['self_uri'] = f'drs://{request.host}/{uri}'
    return drs_object


def get_drs_url(request, object_uri):
    """ Does 2 calls - one to verify the object_uri is in fact a valid DRS object and
        another to get the bytes of the DRS object """
    try:
        default_method = None
        requested_download_method = request.path.split('/')[-1]
        drs_obj = get_and_format_drs_object(request, object_uri)
        access_methods = drs_obj.get('access_methods', [])
        for access_method in access_methods:
            if access_method['type'] == 'https':  # prefer https
                default_method = access_method['access_url']
            if not default_method and access_method['type'] == 'http':  # allow http
                default_method = access_method['access_url']
            if access_method['type'] == requested_download_method:
                return access_method['access_url']
        else:
            if default_method:
                return default_method
            raise Exception  # no https default results in exception
    except Exception as e:
        raise HTTPNotFound(f'You accessed a DRS object that either you do not have access to,'
                           f' did not pass valid access_id or does not exist {str(e)}')


@view_config(
    route_name='drs_objects', request_method=['GET', 'OPTIONS']
)
@debug_log
def drs_objects(context, request):
    """ Implements DRS GET as specified by the API description
    https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_getobject
    """
    if request.method == 'OPTIONS':
        return Response(status_code=204)
    drs_object_uri = '/' + request.matchdict['object_id']
    formatted_drs_object = get_and_format_drs_object(request, drs_object_uri)
    try:
        validate_drs_object(formatted_drs_object)
    except AssertionError as e:
        raise ValueError(f'Formatted DRS object does not conform to spec - check your @@drs'
                         f' implementation: {str(e)}')
    return formatted_drs_object


@view_config(route_name='drs_download_no_slash', request_method='GET')
@view_config(route_name='drs_download_slash', request_method='GET')
@view_config(route_name='drs_download', request_method='GET')
@debug_log
def drs_objects_download(context, request):
    """ Implements DRS GET bytes as specified by the API description
    https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_getaccessurl

    NOTE: access_id is discarded - permissions are validated by @@download when navigated
    """
    drs_object_uri = '/' + request.matchdict['object_id']
    return get_drs_url(request, drs_object_uri)
