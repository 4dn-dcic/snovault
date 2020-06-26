import re

from pyramid.decorator import reify
from pyramid.security import (
    ALL_PERMISSIONS,
    Allow,
    Authenticated,
    Deny,
    Everyone,
)
from ..calculated import calculated_property
from ..resources import Root
from ..config import root


accession_re = re.compile(r'^SNO(SS|FL)[0-9][0-9][0-9][A-Z][A-Z][A-Z]$')
test_accession_re = re.compile(r'^TST(SS|FL)[0-9][0-9][0-9]([0-9][0-9][0-9]|[A-Z][A-Z][A-Z])$')


def is_accession(instance):
    """ From snowflakes - pattern checker """
    return (
        accession_re.match(instance) is not None or
        test_accession_re.match(instance) is not None
    )


def includeme(config):
    config.scan(__name__)


def acl_from_settings(settings):
    # XXX Unsure if any of the demo instance still need this
    acl = []
    for k, v in settings.items():
        if k.startswith('allow.'):
            action = Allow
            permission = k[len('allow.'):]
            principals = v.split()
        elif k.startswith('deny.'):
            action = Deny
            permission = k[len('deny.'):]
            principals = v.split()
        else:
            continue
        if permission == 'ALL_PERMISSIONS':
            permission = ALL_PERMISSIONS
        for principal in principals:
            if principal == 'Authenticated':
                principal = Authenticated
            elif principal == 'Everyone':
                principal = Everyone
            acl.append((action, principal, permission))
    return acl


@root
class TestRoot(Root):
    properties = {
        'title': 'Home',
        'portal_title': 'Snowflakes',
    }

    @reify
    def __acl__(self):
        acl = acl_from_settings(self.registry.settings) + [
            (Allow, Everyone, ['list', 'search']),
            (Allow, 'group.admin', ALL_PERMISSIONS)
        ] + [(Allow, 'remoteuser.INDEXER', ['view', 'view_raw', 'list', 'index']),
        (Allow, 'remoteuser.EMBED', ['view', 'view_raw', 'expand']),
        (Allow, Everyone, ['visible_for_edit'])]
        return acl

    # BBB
    def get_by_uuid(self, uuid, default=None):
        return self.connection.get_by_uuid(uuid, default)

    def get(self, name, default=None):
        resource = super(TestRoot, self).get(name, None)
        if resource is not None:
            return resource
        resource = self.connection.get_by_unique_key('page:location', name)
        if resource is not None:
            return resource
        if is_accession(name):
            resource = self.connection.get_by_unique_key('accession', name)
            if resource is not None:
                return resource
        if ':' in name:
            resource = self.connection.get_by_unique_key('alias', name)
            if resource is not None:
                return resource
        return default

    @calculated_property(schema={
        "title": "Application version",
        "type": "string",
    })
    def app_version(self, registry):
        return registry.settings['snovault.app_version']
