"""Service-free coverage for calculated-property-aware selective reindexing."""

import copy

from types import SimpleNamespace

import pytest

from ..calculated import CalculatedProperties
from ..interfaces import CALCULATED_PROPERTIES, COLLECTIONS, TYPES
from ..elasticsearch import calculated_property_signature as signature_module
from ..elasticsearch import create_mapping
from ..elasticsearch.calculated_property_signature import (
    CALCULATED_PROPERTIES_SIGNATURE_META_KEY,
    calculated_properties_signature,
)
from ..elasticsearch.create_mapping import (
    build_index,
    build_index_record,
    compare_against_existing_mapping,
)
from ..elasticsearch.interfaces import ELASTIC_SEARCH, INDEXER_QUEUE


pytestmark = [pytest.mark.unit]


CALCULATED_SCHEMA = {
    'type': 'string',
    'calculatedProperty': True,
}


def implementation_one():
    return 'one'


def implementation_two():
    return 'two'


def condition_one():
    return True


def condition_two():
    return False


def unchanged_implementation():
    return 'unchanged'


def implementation_using_helper(self):
    return self.helper()


def helper_one(self):
    return 'helper-one'


def helper_two(self):
    return 'helper-two'


class FakeTypeInfo:
    def __init__(self, factory, item_type, schema, embedded_list=None,
                 aggregated_items=None):
        self.factory = factory
        self.item_type = item_type
        self.name = factory.__name__
        self.base_types = list(getattr(factory, 'base_types', []))
        self.schema = schema
        self.embedded_list = embedded_list or []
        self.aggregated_items = aggregated_items or {}


class FakeTypes:
    def __init__(self, type_infos):
        self.by_item_type = {
            type_info.item_type: type_info
            for type_info in type_infos
        }
        self.all = {}
        for type_info in type_infos:
            self.all[type_info.item_type] = type_info
            self.all[type_info.name] = type_info
            self.all[type_info.factory] = type_info

    def __getitem__(self, name):
        return self.all[name]


def factory(name, base=object, **attributes):
    attributes.setdefault('__module__', __name__)
    attributes.setdefault('base_types', [])
    return type(name, (base,), attributes)


def registry_for(type_infos, registrations):
    calculated = CalculatedProperties()
    for registration in registrations:
        calculated.register_prop(**registration)
    return {
        TYPES: FakeTypes(type_infos),
        CALCULATED_PROPERTIES: calculated,
    }


def own_type_registry(implementation=implementation_one, **prop_settings):
    root = factory('Root', calc=implementation)
    schema = {
        'type': 'object',
        'properties': {'calc': copy.deepcopy(CALCULATED_SCHEMA)},
    }
    type_info = FakeTypeInfo(root, 'root', schema)
    registration = {
        'fn': implementation,
        'name': 'calc',
        'context': root,
        'attr': 'calc',
        'schema': {'type': 'string'},
    }
    registration.update(prop_settings)
    return registry_for([type_info], [registration])


def test_signature_is_unchanged_and_deterministic():
    registry = own_type_registry()

    first = calculated_properties_signature(registry, 'root')
    second = calculated_properties_signature(registry, 'root')

    assert first == second
    assert first['complete'] is True
    assert len(first['digest']) == 64


def test_implementation_change_changes_signature_when_mapping_is_identical():
    before_registry = own_type_registry(implementation_one)
    after_registry = own_type_registry(implementation_two)

    before = calculated_properties_signature(before_registry, 'root')
    after = calculated_properties_signature(after_registry, 'root')

    assert before_registry[TYPES]['root'].schema == after_registry[TYPES]['root'].schema
    assert before['digest'] != after['digest']
    assert before['complete'] is after['complete'] is True


def test_referenced_factory_helper_implementation_change_is_detected():
    def helper_registry(helper):
        root = factory(
            'HelperRoot',
            calc=implementation_using_helper,
            helper=helper,
        )
        schema = {
            'type': 'object',
            'properties': {'calc': copy.deepcopy(CALCULATED_SCHEMA)},
        }
        return registry_for([FakeTypeInfo(root, 'helper_root', schema)], [{
            'fn': implementation_using_helper,
            'name': 'calc',
            'context': root,
            'attr': 'calc',
            'schema': {'type': 'string'},
        }])

    before = calculated_properties_signature(helper_registry(helper_one), 'helper_root')
    after = calculated_properties_signature(helper_registry(helper_two), 'helper_root')

    assert before['complete'] is after['complete'] is True
    assert before['digest'] != after['digest']


@pytest.mark.parametrize(
    'before_settings,after_settings',
    [
        ({'define': False}, {'define': True}),
        ({'condition': condition_one}, {'condition': condition_two}),
        ({'schema': {'type': 'string'}},
         {'schema': {'type': 'string', 'title': 'Changed decorator configuration'}}),
        ({'attr': 'calc'}, {'attr': None}),
    ],
)
def test_decorator_configuration_changes_signature(before_settings, after_settings):
    before = calculated_properties_signature(
        own_type_registry(**before_settings), 'root'
    )
    after = calculated_properties_signature(
        own_type_registry(**after_settings), 'root'
    )

    assert before['digest'] != after['digest']


def inherited_registry(base_implementation, override=False):
    base = factory('Base', calc=base_implementation)
    child_attributes = {'base_types': ['Base']}
    registrations = [{
        'fn': base_implementation,
        'name': 'calc',
        'context': base,
        'attr': 'calc',
        'schema': {'type': 'string'},
    }]
    if override:
        child_attributes['calc'] = implementation_two
    child = factory('Child', base=base, **child_attributes)
    if override:
        registrations.append({
            'fn': implementation_two,
            'name': 'calc',
            'context': child,
            'attr': 'calc',
            'schema': {'type': 'string'},
        })
    schema = {
        'type': 'object',
        'properties': {'calc': copy.deepcopy(CALCULATED_SCHEMA)},
    }
    return registry_for([FakeTypeInfo(child, 'child', schema)], registrations)


def test_inherited_implementation_and_subclass_override_are_detected():
    inherited = calculated_properties_signature(
        inherited_registry(implementation_one), 'child'
    )
    changed_base = calculated_properties_signature(
        inherited_registry(implementation_two), 'child'
    )
    overridden = calculated_properties_signature(
        inherited_registry(implementation_one, override=True), 'child'
    )

    assert inherited['digest'] != changed_base['digest']
    assert inherited['digest'] != overridden['digest']


def embedded_registry(target_implementation):
    root = factory('EmbeddedRoot')
    target = factory('EmbeddedTarget', nested_calc=target_implementation)
    root_schema = {
        'type': 'object',
        'properties': {
            'linked': {
                'type': 'string',
                'linkTo': 'EmbeddedTarget',
            },
        },
    }
    target_schema = {
        'type': 'object',
        'properties': {
            '@id': {'type': 'string'},
            '@type': {'type': 'array', 'items': {'type': 'string'}},
            'display_title': {'type': 'string'},
            'nested_calc': copy.deepcopy(CALCULATED_SCHEMA),
            'principals_allowed': {'type': 'object', 'properties': {}},
            'status': {'type': 'string'},
            'uuid': {'type': 'string'},
        },
    }
    infos = [
        FakeTypeInfo(root, 'embedded_root', root_schema,
                     embedded_list=['linked.nested_calc']),
        FakeTypeInfo(target, 'embedded_target', target_schema),
    ]
    registrations = [{
        'fn': target_implementation,
        'name': 'nested_calc',
        'context': target,
        'attr': 'nested_calc',
        'schema': {'type': 'string'},
    }]
    return registry_for(infos, registrations)


def test_nested_embedded_calculated_property_changes_root_signature():
    before = calculated_properties_signature(
        embedded_registry(implementation_one), 'embedded_root'
    )
    after = calculated_properties_signature(
        embedded_registry(implementation_two), 'embedded_root'
    )

    assert before['complete'] is after['complete'] is True
    assert before['digest'] != after['digest']


def shared_registries(shared_implementation):
    infos = []
    registrations = []
    for name in ('SharedA', 'SharedB'):
        item_type = name.lower()
        item_factory = factory(name)
        infos.append(FakeTypeInfo(
            item_factory,
            item_type,
            {'type': 'object', 'properties': {'calc': copy.deepcopy(CALCULATED_SCHEMA)}},
        ))
        registrations.append({
            'fn': shared_implementation,
            'name': 'calc',
            'context': item_factory,
            'schema': {'type': 'string'},
        })
    unaffected = factory('Unaffected')
    infos.append(FakeTypeInfo(
        unaffected,
        'unaffected',
        {'type': 'object', 'properties': {'calc': copy.deepcopy(CALCULATED_SCHEMA)}},
    ))
    registrations.append({
        'fn': unchanged_implementation,
        'name': 'calc',
        'context': unaffected,
        'schema': {'type': 'string'},
    })
    return registry_for(infos, registrations)


def test_shared_implementation_change_affects_every_using_type_only():
    before = shared_registries(implementation_one)
    after = shared_registries(implementation_two)

    for item_type in ('shareda', 'sharedb'):
        assert (calculated_properties_signature(before, item_type)['digest'] !=
                calculated_properties_signature(after, item_type)['digest'])
    assert (calculated_properties_signature(before, 'unaffected') ==
            calculated_properties_signature(after, 'unaffected'))


class MappingIndices:
    def __init__(self, index_name, mapping):
        self.index_name = index_name
        self.mapping = copy.deepcopy(mapping)
        self.deleted = []
        self.created = []

    def get_mapping(self, index):
        return {index: {'mappings': copy.deepcopy(self.mapping)}}

    def exists(self, index):
        return True

    def delete(self, index, ignore=None):
        self.deleted.append(index)
        return {'acknowledged': True}

    def create(self, index, body):
        self.created.append((index, body))
        self.mapping = copy.deepcopy(body['mappings'])
        return {'acknowledged': True}


class MappingES:
    def __init__(self, index_name, mapping):
        self.indices = MappingIndices(index_name, mapping)


def mapping_with_state(state, field_type='keyword'):
    mapping = {'properties': {'field': {'type': field_type}}}
    return build_index_record(
        mapping,
        'root',
        calculated_properties_state=state,
    )['mappings']


def test_uninspectable_source_is_never_accepted_for_selective_skip(monkeypatch):
    def unavailable(_fn):
        raise OSError('source unavailable')

    monkeypatch.setattr(signature_module.inspect, 'getsource', unavailable)
    state = calculated_properties_signature(own_type_registry(), 'root')
    mapping = mapping_with_state(state)
    es = MappingES('root-index', mapping)
    record = {'mappings': copy.deepcopy(mapping), 'settings': {}}

    assert state['complete'] is False
    assert compare_against_existing_mapping(es, 'root-index', 'root', record) is True
    assert compare_against_existing_mapping(
        es, 'root-index', 'root', record, selective_reindex=True
    ) is False


def test_missing_calculated_property_registry_is_incomplete_not_an_error():
    registry = own_type_registry()
    del registry[CALCULATED_PROPERTIES]

    state = calculated_properties_signature(registry, 'root')

    assert state['complete'] is False
    assert len(state['digest']) == 64


def test_mapping_change_still_forces_reindex_with_matching_signature():
    state = calculated_properties_signature(own_type_registry(), 'root')
    es = MappingES('root-index', mapping_with_state(state, field_type='keyword'))
    new_record = {
        'mappings': mapping_with_state(state, field_type='text'),
        'settings': {},
    }

    assert compare_against_existing_mapping(
        es, 'root-index', 'root', new_record, selective_reindex=True
    ) is False


def test_legacy_mapping_comparison_ignores_new_signature_metadata():
    before = calculated_properties_signature(
        own_type_registry(implementation_one), 'root'
    )
    after = calculated_properties_signature(
        own_type_registry(implementation_two), 'root'
    )
    es = MappingES('root-index', mapping_with_state(before))
    record = {'mappings': mapping_with_state(after), 'settings': {}}

    assert compare_against_existing_mapping(es, 'root-index', 'root', record) is True
    assert compare_against_existing_mapping(
        es, 'root-index', 'root', record, selective_reindex=True
    ) is False


def test_changed_signature_rebuilds_type_and_queues_entire_collection(monkeypatch):
    old_state = {'version': 1, 'digest': 'old', 'complete': True}
    new_state = {'version': 1, 'digest': 'new', 'complete': True}
    es = MappingES('root-index', mapping_with_state(old_state))
    collection = SimpleNamespace(index_settings=lambda: {})
    app = SimpleNamespace(registry={COLLECTIONS: {'root': collection}})
    all_uuids = {'uuid-1', 'uuid-2', 'uuid-3'}
    queued_by_type = {}

    monkeypatch.setattr(
        create_mapping, 'calculated_properties_signature',
        lambda registry, item_type: new_state,
    )
    monkeypatch.setattr(
        create_mapping, 'get_uuids_for_types',
        lambda registry, types: all_uuids,
    )
    monkeypatch.setattr(create_mapping, 'confirm_mapping', lambda *args, **kwargs: 0)

    build_index(
        app,
        es,
        'root-index',
        'root',
        {'properties': {'field': {'type': 'keyword'}}},
        queued_by_type,
        False,
        check_first=True,
        selective_reindex=True,
    )

    assert es.indices.deleted == ['root-index']
    assert [index for index, _body in es.indices.created] == ['root-index']
    assert queued_by_type == {'root': all_uuids}
    persisted = es.indices.created[0][1]['mappings']['_meta']
    assert persisted[CALCULATED_PROPERTIES_SIGNATURE_META_KEY] == new_state


class RunQueue:
    def __init__(self):
        self.added = []

    def add_uuids(self, registry, uuids, **kwargs):
        self.added.append((uuids, kwargs))


class RunIndices:
    def exists(self, index):
        return True


def test_unchanged_large_collection_is_skipped_and_changed_type_stays_scoped(monkeypatch):
    queue = RunQueue()
    collections = {
        'changed': SimpleNamespace(properties_datastore='sql'),
        'unchanged_large': SimpleNamespace(properties_datastore='sql'),
    }
    registry = {
        COLLECTIONS: CollectionRegistry(collections),
        ELASTIC_SEARCH: SimpleNamespace(indices=RunIndices()),
        INDEXER_QUEUE: queue,
    }
    app = SimpleNamespace(registry=registry)
    changed_uuids = {f'changed-{number}' for number in range(50001)}

    monkeypatch.setattr(create_mapping, 'create_mapping_by_type',
                        lambda item_type, registry: {'properties': {}})
    monkeypatch.setattr(create_mapping, 'get_namespaced_index',
                        lambda app, item_type: item_type)

    def selective_build(app, es, index_name, item_type, mapping, queued_by_type,
                        dry_run, check_first, index_diff, print_count_only,
                        selective_reindex):
        assert check_first is selective_reindex is True
        queued_by_type[item_type] = changed_uuids if item_type == 'changed' else set()

    monkeypatch.setattr(create_mapping, 'build_index', selective_build)

    create_mapping.run(app, selective_reindex=True)

    assert len(queue.added) == 1
    queued, options = queue.added[0]
    assert set(queued) == changed_uuids
    assert options['strict'] is True


def test_selective_reindex_rejects_partial_index_diff_mode():
    app = SimpleNamespace(registry={
        ELASTIC_SEARCH: object(),
        INDEXER_QUEUE: object(),
    })

    with pytest.raises(ValueError, match='mutually exclusive'):
        create_mapping.run(
            app,
            selective_reindex=True,
            index_diff=True,
        )


class CollectionRegistry:
    def __init__(self, collections):
        self.by_item_type = collections
        self._collections = collections

    def __getitem__(self, item_type):
        return self._collections[item_type]
