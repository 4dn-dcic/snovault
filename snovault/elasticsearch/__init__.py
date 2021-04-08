import json

from dcicutils.es_utils import create_es_client
from elasticsearch.connection import RequestsHttpConnection
from elasticsearch.serializer import SerializationError
from pyramid.settings import asbool
from ..json_renderer import json_renderer
from ..util import get_root_request
from .interfaces import APP_FACTORY, ELASTIC_SEARCH


def includeme(config):
    """ Creates the es_client for use by the application
        Important options from settings:
            * elasticsearch.server - URL of the server
            * elasticsearch.aws_auth - whether or not to use local AWS creds
            * elasticsearch.request_timeout - ES request timeout, defaults to 10 seconds but is
              upped to 20 in our settings if not otherwise specified.
            * elasticsearch.request_auto_retry - allows us to disable request auto-retry
    """
    settings = config.registry.settings
    address = settings['elasticsearch.server']
    use_aws_auth = settings.get('elasticsearch.aws_auth')
    es_request_timeout = settings.get('elasticsearch.request_timeout', 20)  # only change is here
    es_request_auto_retry = settings.get('elasticsearch.request_auto_retry', True)
    # make sure use_aws_auth is bool
    if not isinstance(use_aws_auth, bool):
        use_aws_auth = True if use_aws_auth == 'true' else False
    # snovault specific ES options
    # this previously-used option was causing problems (?)
    # 'connection_class': TimedUrllib3HttpConnection
    es_options = {'serializer': PyramidJSONSerializer(json_renderer),
                  'connection_class': TimedRequestsHttpConnection,
                  'timeout': es_request_timeout,
                  'retry_on_timeout': es_request_auto_retry}

    config.registry[ELASTIC_SEARCH] = create_es_client(address,
                                                       use_aws_auth=use_aws_auth,
                                                       **es_options)

    config.include('.cached_views')
    config.include('.esstorage')
    config.include('.indexer_queue')
    config.include('.indexer_utils')  # has invalidation_scope route
    config.include('.indexer')
    if asbool(settings.get('mpindexer')):
        config.include('.mpindexer')


class PyramidJSONSerializer(object):
    mimetype = 'application/json'

    def __init__(self, renderer):
        self.renderer = renderer

    @staticmethod
    def loads(s):
        try:
            return json.loads(s)
        except (ValueError, TypeError) as e:
            raise SerializationError(s, e)

    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, (type(''), type(u''))):
            return data

        try:
            return self.renderer.dumps(data)
        except (ValueError, TypeError) as e:
            raise SerializationError(data, e)


# changed to work with Urllib3HttpConnection (from ES) to RequestsHttpConnection
class TimedRequestsHttpConnection(RequestsHttpConnection):
    stats_count_key = 'es_count'
    stats_time_key = 'es_time'

    def stats_record(self, duration):
        request = get_root_request()
        if request is None:
            return

        duration = int(duration * 1e6)
        if not hasattr(request, "_stats"):
            request._stats = {}
        stats = request._stats
        stats[self.stats_count_key] = stats.get(self.stats_count_key, 0) + 1
        stats[self.stats_time_key] = stats.get(self.stats_time_key, 0) + duration

    def log_request_success(self, method, full_url, path, body, status_code, response, duration):
        self.stats_record(duration)
        return super(TimedRequestsHttpConnection, self).log_request_success(
            method, full_url, path, body, status_code, response, duration)

    def log_request_fail(self, method, full_url, path, body, duration, status_code=None, response=None, exception=None):
        self.stats_record(duration)
        return super(TimedRequestsHttpConnection, self).log_request_fail(
            method, full_url, path, body, duration, status_code=status_code, response=response, exception=exception)
