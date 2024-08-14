import logging
import netaddr

from dcicutils.ff_utils import get_health_page
from dcicutils.log_utils import set_logging
from pyramid.config import Configurator
from pyramid.settings import asbool
from .local_roles import LocalRolesAuthorizationPolicy

from .app import app_version, session, configure_dbsession, changelogs, json_from_path
from .calculated import calculated_property  # noqa
from .config import abstract_collection, collection, root  # noqa
from .elasticsearch import APP_FACTORY
from .elasticsearch.interfaces import INVALIDATION_SCOPE_ENABLED
from .interfaces import *  # noqa
from .resources import AbstractCollection, Collection, Item, Resource, Root, display_title_schema  # noqa
from .schema_utils import load_schema  # noqa
from .upgrader import upgrade_step  # noqa


def includeme(config):
    config.include('pyramid_retry')
    config.include('pyramid_tm')
    config.include('snovault.authentication')
    config.include('snovault.util')
    config.include('snovault.drs')
    config.include('snovault.stats')
    config.include('snovault.batchupgrade')
    config.include('snovault.calculated')
    config.include('snovault.config')
    config.include('snovault.connection')
    config.include('snovault.custom_embed')
    config.include('snovault.embed')
    config.include('snovault.json_renderer')
    config.include('snovault.validation')
    config.include('snovault.predicates')
    config.include('snovault.invalidation')
    config.include('snovault.upgrader')
    config.include('snovault.aggregated_items')
    config.include('snovault.storage')
    config.include('snovault.typeinfo')
    config.include('snovault.types')
    config.include('snovault.resources')
    config.include('snovault.attachment')
    config.include('snovault.schema_graph')
    config.include('snovault.jsonld_context')
    config.include('snovault.schema_views')
    config.include('snovault.crud_views')
    config.include('snovault.indexing_views')
    config.include('snovault.resource_views')
    config.include('snovault.settings')
    config.include('snovault.server_defaults')
    config.include('snovault.routes')


def main(global_config, **local_config):
    """
    This function returns a Pyramid WSGI application.
    """
    settings = global_config
    settings.update(local_config)

    # adjust log levels for some annoying loggers
    lnames = ['boto', 'urllib', 'elasticsearch', 'dcicutils']
    for name in logging.Logger.manager.loggerDict:
        if any(logname in name for logname in lnames):
            logging.getLogger(name).setLevel(logging.WARNING)

    set_logging(in_prod=settings.get('production'))
    # set_logging(settings.get('elasticsearch.server'), settings.get('production'))

    # TODO - these need to be set for dummy app
    # settings['snovault.jsonld.namespaces'] = json_asset('snovault:schemas/namespaces.json')
    # settings['snovault.jsonld.terms_namespace'] = 'https://www.encodeproject.org/terms/'
    settings['snovault.jsonld.terms_prefix'] = 'snovault'

    config = Configurator(settings=settings)
    config.registry[APP_FACTORY] = main  # used by mp_indexer
    config.include(app_version)

    config.include('pyramid_multiauth')  # must be before calling set_authorization_policy
    # Override default authz policy set by pyramid_multiauth
    config.set_authorization_policy(LocalRolesAuthorizationPolicy())
    config.include(session)

    config.include(configure_dbsession)
    config.include('snovault')
    config.commit()  # commit so search can override listing

    config.include('.renderers')

    if settings.get('elasticsearch.server'):
        config.include('snovault.search.search')
        config.include('snovault.search.compound_search')

    # only include this stuff if we're testing
    if asbool(settings.get('testing', False)):
        config.include('snovault.tests.testing_views')
        config.include('snovault.tests.root')

        # in addition, enable invalidation scope for testing - but NOT by default
        settings[INVALIDATION_SCOPE_ENABLED] = True
    else:
        config.include('snovault.root')

    if 'elasticsearch.server' in config.registry.settings:
        config.include('snovault.elasticsearch')

    # configure redis server in production.ini
    if 'redis.server' in config.registry.settings:
        config.include('snovault.redis')

    config.include(changelogs)

    # TODO This is optional AWS only - possibly move to a plug-in
    aws_ip_ranges = json_from_path(settings.get('aws_ip_ranges_path'), {'prefixes': []})
    config.registry['aws_ipset'] = netaddr.IPSet(
        record['ip_prefix'] for record in aws_ip_ranges['prefixes'] if record['service'] == 'AMAZON')

    # cache mirror_health in registry if need be
    mirror = settings.get('mirror.env.name', None)
    if mirror:
        settings['mirror_health'] = get_health_page(ff_env=mirror)

    app = config.make_wsgi_app()

    return app
