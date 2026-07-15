"""Deterministic calculated-property signatures for selective reindexing."""

import ast
import enum
import hashlib
import inspect
import json
import logging
import math
import textwrap

from functools import partial, reduce

from ..interfaces import CALCULATED_PROPERTIES, TYPES
from ..schema_utils import combine_schemas
from ..util import add_default_embeds


CALCULATED_PROPERTIES_SIGNATURE_VERSION = 1
CALCULATED_PROPERTIES_SIGNATURE_META_KEY = 'snovault_calculated_properties'


def _identity(value):
    """Return a stable Python identity without a filename or object address."""
    module = getattr(value, '__module__', None)
    name = getattr(value, '__qualname__', None) or getattr(value, '__name__', None)
    if module and name:
        return f'{module}.{name}'
    return None


def _logger_record(value):
    """Return a stable record for logger singletons, or None if not a logger.

    Module-global loggers appear throughout implementation code but never
    affect calculated output; identifying them by name keeps signatures
    complete instead of conservatively rebuilding every type that logs.
    """
    if isinstance(value, logging.Logger):
        return {'logger': value.name}
    cls = type(value)
    if cls.__module__.split('.')[0] == 'structlog' and 'Logger' in cls.__name__:
        factory_args = getattr(value, '_logger_factory_args', None) or ()
        name = '.'.join(str(arg) for arg in factory_args)
        return {'logger': name or None}
    return None


class _SignatureBuilder:
    """Build JSON-safe signature input while tracking anything unresolved."""

    def __init__(self):
        self.complete = True
        self._active_callables = set()

    def unresolved(self, value):
        self.complete = False
        return {'unresolved': _identity(value) or type(value).__name__}

    def value(self, value):
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if math.isnan(value):
                return {'float': 'nan'}
            if math.isinf(value):
                return {'float': 'infinity' if value > 0 else '-infinity'}
            return value
        if isinstance(value, bytes):
            return {'bytes': value.hex()}
        if isinstance(value, enum.Enum):
            return {
                'enum': _identity(type(value)),
                'value': self.value(value.value),
            }
        if isinstance(value, list):
            return [self.value(item) for item in value]
        if isinstance(value, tuple):
            return {'tuple': [self.value(item) for item in value]}
        if isinstance(value, (set, frozenset)):
            items = [self.value(item) for item in value]
            return {'set': sorted(items, key=_canonical_json)}
        if isinstance(value, dict):
            items = [
                [self.value(key), self.value(item)]
                for key, item in value.items()
            ]
            return {'dict': sorted(items, key=lambda item: _canonical_json(item[0]))}
        logger_record = _logger_record(value)
        if logger_record is not None:
            return logger_record
        if inspect.ismodule(value):
            return {'module': value.__name__}
        if inspect.isclass(value):
            identity = _identity(value)
            return {'class': identity} if identity else self.unresolved(value)
        if isinstance(value, partial) or inspect.isfunction(value) or inspect.ismethod(value):
            return self.callable(value)
        return self.unresolved(value)

    def callable(self, fn):
        if isinstance(fn, partial):
            return {
                'partial': self.callable(fn.func),
                'args': self.value(fn.args),
                'keywords': self.value(fn.keywords or {}),
            }
        if inspect.ismethod(fn):
            fn = fn.__func__
        identity = _identity(fn)
        if not inspect.isfunction(fn) or not identity:
            return self.unresolved(fn)
        if id(fn) in self._active_callables:
            return {'callable_reference': identity}

        self._active_callables.add(id(fn))
        try:
            try:
                source = inspect.getsource(fn)
                source_tree = ast.parse(textwrap.dedent(source))
                # Calculated-property decorator inputs are serialized from the
                # resolved registry below, where dictionary ordering is
                # canonical. Do not also depend on their incidental source
                # spelling or ordering here.
                for node in ast.walk(source_tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        node.decorator_list = []
                syntax = ast.dump(
                    source_tree,
                    annotate_fields=True,
                    include_attributes=False,
                )
            except Exception:
                return self.unresolved(fn)

            closure = {}
            if fn.__closure__:
                for name, cell in zip(fn.__code__.co_freevars, fn.__closure__):
                    try:
                        closure[name] = self.value(cell.cell_contents)
                    except ValueError:
                        closure[name] = self.unresolved(fn)

            referenced_globals = {}
            for name in sorted(set(fn.__code__.co_names)):
                if name not in fn.__globals__:
                    continue
                global_value = fn.__globals__[name]
                logger_record = _logger_record(global_value)
                if logger_record is not None:
                    referenced_globals[name] = logger_record
                elif (inspect.isfunction(global_value)
                        and global_value.__module__ == fn.__module__):
                    referenced_globals[name] = self.callable(global_value)
                elif callable(global_value) and not inspect.isclass(global_value):
                    global_identity = _identity(global_value)
                    if global_identity:
                        referenced_globals[name] = {'callable': global_identity}
                    else:
                        referenced_globals[name] = self.unresolved(global_value)
                else:
                    referenced_globals[name] = self.value(global_value)

            return {
                'identity': identity,
                'syntax': syntax,
                'code': self.code(fn.__code__),
                'defaults': self.value(fn.__defaults__),
                'kwdefaults': self.value(fn.__kwdefaults__),
                'closure': self.value(closure),
                'globals': self.value(referenced_globals),
                'wrapped': (
                    self.callable(fn.__wrapped__)
                    if inspect.isfunction(getattr(fn, '__wrapped__', None))
                    else None
                ),
            }
        finally:
            self._active_callables.remove(id(fn))

    def code(self, code):
        """Represent executable code without filenames or source line data."""
        constants = []
        for constant in code.co_consts:
            if inspect.iscode(constant):
                constants.append({'code': self.code(constant)})
            else:
                constants.append(self.value(constant))
        return {
            'bytecode': code.co_code.hex(),
            'constants': constants,
            'names': list(code.co_names),
            'varnames': list(code.co_varnames),
            'freevars': list(code.co_freevars),
            'cellvars': list(code.co_cellvars),
            'argcount': code.co_argcount,
            'posonlyargcount': code.co_posonlyargcount,
            'kwonlyargcount': code.co_kwonlyargcount,
            'flags': code.co_flags,
        }


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _concrete_type_infos(types, target):
    """Resolve a linkTo target to every concrete type it can represent."""
    target_info = types.all[target]
    target_name = target_info.name
    return [
        type_info
        for type_info in types.by_item_type.values()
        if target_name in ([type_info.name] + list(type_info.base_types))
    ]


def _combined_schema(type_infos):
    schemas = [type_info.schema for type_info in type_infos]
    if not schemas:
        return None
    return reduce(combine_schemas, schemas)


def _relevant_type_infos(registry, item_type, builder):
    """Find root and embedded concrete item types evaluated for one ES type."""
    types = registry[TYPES]
    root_type_info = types.by_item_type[item_type]
    relevant = {root_type_info.item_type: root_type_info}
    schema = root_type_info.schema
    try:
        embeds = add_default_embeds(
            item_type,
            types,
            root_type_info.embedded_list,
            schema,
        )
    except Exception:
        builder.complete = False
        return list(relevant.values())

    for embed in sorted(set(embeds)):
        current_schema = schema
        for element in embed.split('.'):
            if element == '*':
                break
            properties = current_schema.get('properties', current_schema)
            if not isinstance(properties, dict) or element not in properties:
                builder.complete = False
                break
            current_schema = properties[element]
            if current_schema.get('type') == 'array':
                current_schema = current_schema.get('items', {})
            link_targets = current_schema.get('linkTo')
            if not link_targets:
                continue
            if not isinstance(link_targets, list):
                link_targets = [link_targets]
            linked_type_infos = []
            for target in link_targets:
                try:
                    concrete_types = _concrete_type_infos(types, target)
                except (KeyError, AttributeError, TypeError):
                    builder.complete = False
                    concrete_types = []
                if not concrete_types:
                    builder.complete = False
                for type_info in concrete_types:
                    relevant[type_info.item_type] = type_info
                    linked_type_infos.append(type_info)
            current_schema = _combined_schema(linked_type_infos) or {}

    return sorted(relevant.values(), key=lambda type_info: type_info.item_type)


def _referenced_names(fn):
    """Names an implementation function may resolve against its factory."""
    if inspect.ismethod(fn):
        fn = fn.__func__
    if not inspect.isfunction(fn):
        return ()
    return fn.__code__.co_names


def _implementation_dependencies(factory, implementation, builder):
    """Fingerprint factory members transitively reachable from an implementation.

    Walks the implementation's referenced names against the factory MRO and
    keeps following names referenced by any factory function found, so a
    change to a helper-of-a-helper, or to plain class-attribute data such as
    ``rev`` (read by ``Item.get_rev_links``) or ``filtered_rev_statuses``,
    always changes the signature.
    """
    dependencies = {}
    pending = list(_referenced_names(implementation))
    seen = set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        raw_value = None
        for cls in factory.mro():
            # Builtin members (e.g. object.__class__) are interpreter-defined
            # and cannot change between deployments of the application.
            if cls.__module__ == 'builtins':
                continue
            if name in cls.__dict__:
                raw_value = cls.__dict__[name]
                break
        if raw_value is None:
            continue
        if isinstance(raw_value, (classmethod, staticmethod)):
            raw_value = raw_value.__func__
        wrapped = getattr(raw_value, 'wrapped', None)
        if isinstance(raw_value, property):
            dependencies[name] = {
                'get': builder.callable(raw_value.fget) if raw_value.fget else None,
                'set': builder.callable(raw_value.fset) if raw_value.fset else None,
            }
            pending.extend(_referenced_names(raw_value.fget))
            pending.extend(_referenced_names(raw_value.fset))
        elif inspect.isfunction(raw_value):
            dependencies[name] = builder.callable(raw_value)
            pending.extend(_referenced_names(raw_value))
        elif inspect.isfunction(wrapped):
            # Non-data descriptors wrapping a function, e.g. pyramid ``reify``.
            dependencies[name] = builder.callable(wrapped)
            pending.extend(_referenced_names(wrapped))
        else:
            # Plain class-attribute data an implementation reads via ``self``.
            dependencies[name] = builder.value(raw_value)
    return dependencies


def _property_record(type_info, name, prop, builder):
    implementation = prop.fn
    if prop.attr:
        try:
            implementation = getattr(type_info.factory, prop.attr)
        except (AttributeError, TypeError):
            implementation = None
    if isinstance(implementation, str):
        implementation_signature = implementation
    elif implementation is None:
        implementation_signature = builder.unresolved(prop)
    else:
        implementation_signature = builder.callable(implementation)

    if prop.condition is None or isinstance(prop.condition, str):
        condition_signature = prop.condition
        condition_dependencies = {}
    else:
        condition_signature = builder.callable(prop.condition)
        condition_dependencies = _implementation_dependencies(
            type_info.factory, prop.condition, builder
        )

    return {
        'name': name,
        'attr': prop.attr,
        'define': prop.define,
        'schema': builder.value(prop.schema),
        'implementation': implementation_signature,
        'implementation_dependencies': _implementation_dependencies(
            type_info.factory, implementation, builder
        ),
        'condition': condition_signature,
        'condition_dependencies': condition_dependencies,
    }


def _factory_configuration(factory, builder):
    """Class-attribute configuration that changes indexed content directly.

    ``rev`` and ``filtered_rev_statuses`` determine rev-link calculated
    property values; ``name_key`` determines ``@id`` and resource paths for
    this type wherever it is rendered, including inside other types'
    embedded documents.
    """
    return {
        'rev': builder.value(getattr(factory, 'rev', None)),
        'filtered_rev_statuses': builder.value(
            getattr(factory, 'filtered_rev_statuses', None)
        ),
        'name_key': builder.value(getattr(factory, 'name_key', None)),
    }


def _acl_record(factory, builder):
    """Fingerprint the ACL inputs to the root document's principals_allowed.

    Walks ``__acl__`` and everything it references on the factory (such as
    ``STATUS_ACL``), so a permission change with an unchanged mapping still
    rebuilds and reindexes the type.
    """
    acl = getattr(factory, '__acl__', None)
    if acl is None:
        return None
    if inspect.isfunction(acl) or inspect.ismethod(acl):
        return {
            'implementation': builder.callable(acl),
            'dependencies': _implementation_dependencies(factory, acl, builder),
        }
    # Pyramid also permits a plain ACL list attribute.
    return {'value': builder.value(acl)}


def calculated_properties_signature(registry, item_type):
    """Return a deterministic, versioned signature for one indexed item type.

    ``complete`` is deliberately part of the result. Selective comparison must
    reject an incomplete signature even if its digest happens to repeat.
    """
    builder = _SignatureBuilder()
    try:
        type_infos = _relevant_type_infos(registry, item_type, builder)
    except (KeyError, AttributeError, TypeError):
        type_infos = []
        builder.complete = False

    try:
        calculated_properties = registry[CALCULATED_PROPERTIES]
    except (KeyError, AttributeError, TypeError):
        calculated_properties = None
        builder.complete = False
    type_records = []
    for type_info in type_infos:
        try:
            props = (
                calculated_properties.props_for(type_info.factory)
                if calculated_properties is not None else {}
            )
            property_records = [
                _property_record(type_info, name, props[name], builder)
                for name in sorted(props)
            ]
            factory_identity = _identity(type_info.factory)
            if not factory_identity:
                builder.complete = False
            factory_configuration = _factory_configuration(type_info.factory, builder)
        except Exception:
            builder.complete = False
            property_records = []
            factory_identity = None
            factory_configuration = None
        type_records.append({
            'item_type': type_info.item_type,
            'factory': factory_identity,
            'base_types': builder.value(list(type_info.base_types)),
            'configuration': factory_configuration,
            'properties': property_records,
        })

    try:
        root_type_info = registry[TYPES].by_item_type.get(item_type)
    except (KeyError, AttributeError, TypeError):
        root_type_info = None
    if root_type_info is None:
        builder.complete = False
        index_configuration = None
    else:
        try:
            acl_record = _acl_record(root_type_info.factory, builder)
        except Exception:
            builder.complete = False
            acl_record = None
        index_configuration = {
            # add_default_embeds treats ordering and duplicates as incidental.
            'embedded_list': sorted(set(root_type_info.embedded_list)),
            'aggregated_items': builder.value(root_type_info.aggregated_items),
            # principals_allowed is indexed on every root document.
            'acl': acl_record,
        }

    payload = {
        'version': CALCULATED_PROPERTIES_SIGNATURE_VERSION,
        'item_type': item_type,
        'index_configuration': index_configuration,
        'types': type_records,
    }
    digest = hashlib.sha256(_canonical_json(payload).encode('utf-8')).hexdigest()
    return {
        'version': CALCULATED_PROPERTIES_SIGNATURE_VERSION,
        'digest': digest,
        'complete': builder.complete,
    }
