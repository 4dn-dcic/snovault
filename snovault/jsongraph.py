from past.builtins import basestring
from pyramid.view import view_config
# Python thinks Item is unused, but it doesn't check for uses in other files, so I'm leaving mention of that
# temporarily just in case someone is importing it from here. Remove these comments later. -kmp 9-Jun-2020
from .resources import Root, CONNECTION  # , Item
from .elasticsearch.indexer_utils import get_uuids_for_types
from .util import debug_log


def includeme(config):
    config.scan(__name__)


def uuid_to_ref(obj, path):
    # TODO: The compatibility code for basestring allows bytes (b'...') as within scope,
    #       but to split bytes without error you'd have to do path.split(b'.'), so I don't think
    #       we ever get that. I think we should rewrite this as just isinstance(path, str). -kmp 9-Jun-2020
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if remaining:
        if isinstance(value, list):
            for v in value:
                uuid_to_ref(v, remaining)
        else:
            uuid_to_ref(value, remaining)
        return
    if isinstance(value, list):
        obj[name] = [
            {'$type': 'ref', 'value': ['uuid', v]}
            for v in value
        ]
    else:
        obj[name] = {'$type': 'ref', 'value': ['uuid', value]}


def item_jsongraph(context, properties):
    properties = properties.copy()
    for path in context.type_info.schema_links:
        uuid_to_ref(properties, path)
    properties['uuid'] = str(context.uuid)
    properties['@type'] = context.type_info.name
    return properties


@view_config(context=Root, request_method='GET', name='jsongraph')
@debug_log
def jsongraph(context, request):
    conn = request.registry[CONNECTION]
    cache = {
        'uuid': {},
    }
    for uuid in get_uuids_for_types(request.registry):
        item = conn[uuid]
        properties = item.__json__(request)
        cache['uuid'][uuid] = item_jsongraph(item, properties)
        for k, values in item.unique_keys(properties).items():
            for v in values:
                cache.setdefault(k, {})[v] = {'$type': 'ref', 'value': ['uuid', str(item.uuid)]}
    return cache
