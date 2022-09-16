import psutil
import pyramid.tweens
import time

from dcicutils.misc_utils import ignored, get_error_message
from pyramid.httpexceptions import HTTPBadRequest
from sqlalchemy import event
from sqlalchemy.engine import Engine
from structlog import get_logger
from urllib.parse import urlencode

from .util import get_root_request


def includeme(config):
    config.add_tween('snovault.stats.stats_tween_factory', under=pyramid.tweens.INGRESS)


log = get_logger()


def requests_timing_hook(prefix='requests'):
    count_key = prefix + '_count'
    time_key = prefix + '_time'

    def response_hook(r, *args, **kwargs):
        ignored(args, kwargs)
        request = get_root_request()
        if request is None:
            return

        stats = request._stats
        stats[count_key] = stats.get(count_key, 0) + 1
        # requests response.elapsed is a timedelta
        e = r.elapsed
        duration = (e.days * 86400 + e.seconds) * 1000000 + e.microseconds
        stats[time_key] = stats.get(time_key, 0) + duration

    return response_hook


# See http://www.sqlalchemy.org/trac/wiki/UsageRecipes/Profiling
@event.listens_for(Engine, 'before_cursor_execute')
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    ignored(conn, cursor, statement, parameters, executemany)
    context._query_start_time = int(time.time() * 1e6)


@event.listens_for(Engine, 'after_cursor_execute')
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    ignored(conn, cursor, statement, parameters, executemany)
    end = int(time.time() * 1e6)

    request = get_root_request()
    if request is None:
        return

    stats = request._stats
    stats['db_count'] = stats.get('db_count', 0) + 1
    duration = end - context._query_start_time
    stats['db_time'] = stats.get('db_time', 0) + duration


# http://docs.pylonsproject.org/projects/pyramid/en/latest/narr/hooks.html#creating-a-tween-factory
def stats_tween_factory(handler, registry):
    ignored(registry)
    process = psutil.Process()

    def stats_tween(request):

        stats: dict = {}
        request._stats = stats  # noQA - yes, using a protected member

        try:

            # set telemetry_id as logger context if passed in
            log_keys = {}
            if 'telemetry_id' in request.params:
                log_keys['telemetry_id'] = request.params['telemetry_id']
            if 'log_action' in request.params:
                log_keys['log_action'] = request.params['log_action']
            if request.registry.settings.get('env_name'):
                log_keys['ff_env'] = request.registry.settings.get('env_name')

            # If stray unicode characters are inserted in the query string, for example, it's possible
            # to get an error such as:
            #     UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc0 in position 1: invalid start byte
            # We see this when WhiteHat pelts us with strange requests as part of a pen test.  The error
            # occurs in the request.path computation.  Ref: https://hms-dbmi.atlassian.net/browse/C4-887
            log_keys['url_path'] = request.path
            log_keys['url_qs'] = request.query_string
            log_keys['host'] = request.host.split(":")[0]

        except Exception as e:
            raise HTTPBadRequest(get_error_message(e))

        log.bind(**log_keys)
        stats.update(log_keys)

        rss_begin = stats['rss_begin'] = process.memory_info().rss
        begin = stats['wsgi_begin'] = int(time.time() * 1e6)
        response = handler(request)
        end = stats['wsgi_end'] = int(time.time() * 1e6)
        rss_end = stats['rss_end'] = process.memory_info().rss
        stats['wsgi_time'] = end - begin
        stats['rss_change'] = rss_end - rss_begin

        environ = request.environ
        if 'mod_wsgi.queue_start' in environ:
            queue_begin = int(environ['mod_wsgi.queue_start'])
            stats['queue_begin'] = queue_begin
            stats['queue_time'] = begin - queue_begin

        xs = response.headers['X-Stats'] = str(urlencode(sorted(stats.items())))
        if getattr(request, '_add_stats_cookie', False):
            response.set_cookie('X-Stats', xs)

        # log all this stuff
        log.bind(**stats).info("Request timings")

        return response

    return stats_tween
