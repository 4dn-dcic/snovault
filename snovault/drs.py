from pyramid.view import view_config
from pyramid.security import Authenticated
from .util import debug_log


DRS_PREFIX_V1 = 'ga4gh/drs/v1'
DRS_OBJECT_GET = DRS_PREFIX_V1 + '/objects/{object_id}'
DRS_OBJECT_GET_ACCESS_URL = DRS_PREFIX_V1 + '/objects/{object_id}/access/{access_id}'


def includeme(config):
    config.add_route('drs_objects', '/' + DRS_OBJECT_GET)
    config.add_route('drs_objects_download', '/' + DRS_OBJECT_GET_ACCESS_URL)
    config.scan(__name__)


def get_and_format_drs_object(request, object_uri):
    """ Call request.embed on the object_uri and reformats it such that it fits the DRS
        format, returning access_methods etc as needed if it is a file
    """
    rendered_object = request.embed(object_uri, '@@object', as_user=True)
    drs_object = {
        'id': rendered_object['@id'],
        'created_time': rendered_object['date_created'],
        'drs_id': rendered_object['uuid'],
    }
    return drs_object


@view_config(
    route_name='drs_objects', request_method='GET'
)
@debug_log
def drs_objects(context, request):
    """ Implements DRS GET as specified by the API description
    https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_getobject
    """
    drs_object_uri = '/' + request.matchdict['object_id']
    formatted_drs_object = get_and_format_drs_object(request, drs_object_uri)
    return formatted_drs_object


@view_config(
    route_name='drs_objects_download', request_method='GET',
    effective_principals=Authenticated
)
@debug_log
def drs_objects_download(context, request):
    """ Implements DRS GET bytes as specified by the API description
    https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_getaccessurl
    """
    pass



