from __future__ import absolute_import
import venusian
import functools
from concurrent.futures import ThreadPoolExecutor
from pyramid.decorator import reify
from pyramid.traversal import find_root
from pyramid.scripting import prepare
from types import MethodType
from .interfaces import (
    CALCULATED_PROPERTIES,
    CONNECTION,
)
import structlog


log = structlog.getLogger(__name__)


CALC_PROP_THREAD_COUNT = 8


def includeme(config):
    config.registry[CALCULATED_PROPERTIES] = CalculatedProperties()
    config.add_directive('add_calculated_property', add_calculated_property)


class ItemNamespace(object):
    def __init__(self, context, request, defined=None, ns=None):
        self.context = context
        self.request = request
        self._defined = defined or {}
        if ns:
            self.__dict__.update(ns)
        self._results = {}

    @reify
    def _properties(self):
        return self.context.__json__(self.request)

    @reify
    def root(self):
        return find_root(self.context)

    @reify
    def registry(self):
        return self.request.registry

    def __getattr__(self, name):
        context = self.context
        request = self.request
        conn = self.registry[CONNECTION]
        if name in self._defined:
            value = self._defined[name](self)
            setattr(self, name, value)
            return value
        if name in self._properties:
            value = self._properties[name]
            if name in context.type_info.schema_links:
                if isinstance(value, list):
                    value = [
                        request.resource_path(conn.get_by_uuid(v))
                        for v in value
                    ]
                else:
                    value = request.resource_path(conn.get_by_uuid(value))
            setattr(self, name, value)
            return value
        if name in context.rev:
            value = context.get_rev_links(request, name)
            value = [
                request.resource_path(conn.get_by_uuid(v))
                for v in value
            ]
            setattr(self, name, value)
            return value
        raise AttributeError(name)

    def __call__(self, fn):
        try:
            return self._results[fn]
        except KeyError:
            pass

        if isinstance(fn, str):
            result = self._results[fn] = getattr(self, fn, None)
            return result

        start = 1 if isinstance(fn, MethodType) else 0
        # Not using inspect.getargspec as it is slow
        args = fn.__code__.co_varnames[start:fn.__code__.co_argcount]
        kw = {}
        for name in args:
            try:
                kw[name] = getattr(self, name)
            except AttributeError:
                pass

        result = self._results[fn] = fn(**kw)
        return result


class CalculatedProperties(object):
    def __init__(self):
        self.category_cls_props = {}

    def register_prop(self, fn, name, context, condition=None, schema=None,
                      attr=None, define=False, category='object'):
        prop = CalculatedProperty(fn, name, attr, condition, schema, define)
        cls_props = self.category_cls_props.setdefault(category, {})
        cls_props.setdefault(context, {})[name] = prop

    def props_for(self, context, category='object'):
        if isinstance(context, type):
            cls = context
        else:
            cls = type(context)
        props = {}
        cls_props = self.category_cls_props.get(category, {})
        for base in reversed(cls.mro()):
            props.update(cls_props.get(base, {}))
        return props


class CalculatedProperty(object):
    condition_args = None

    def __init__(self, fn, name, attr=None, condition=None, schema=None, define=False):
        self.fn = fn
        self.attr = attr
        self.name = name
        self.condition = condition
        self.define = define

        if schema is not None:
            if 'default' in schema:
                raise ValueError('schema may not specify default for calculated property')
            schema = schema.copy()
            schema['calculatedProperty'] = True
        self.schema = schema

    def __call__(self, namespace):
        if self.condition is not None:
            if not namespace(self.condition):
                return None
        if self.attr:
            fn = getattr(namespace.context, self.attr)
        else:
            fn = self.fn
        return namespace(fn)


# Imperative configuration
def add_calculated_property(config, fn, name, context, condition=None, schema=None,
                            attr=None, define=False, category='object'):
    calculated_properties = config.registry[CALCULATED_PROPERTIES]
    config.action(
        ('calculated_property', context, category, name),
        calculated_properties.register_prop,
        (fn, name, context, condition, schema, attr, define, category),
    )


# Declarative configuration
def calculated_property(**settings):
    """ Register a calculated property
    """

    def decorate(wrapped):
        def callback(scanner, factory_name, factory):
            if settings.get('context') is None:
                settings['context'] = factory
            if settings.get('name') is None:
                settings['name'] = factory_name
            scanner.config.add_calculated_property(wrapped, **settings)

        info = venusian.attach(wrapped, callback, category='object')

        if info.scope == 'class':
            # if the decorator was attached to a method in a class, or
            # otherwise executed at class scope, we need to set an
            # 'attr' into the settings if one isn't already in there
            if settings.get('attr') is None:
                settings['attr'] = wrapped.__name__
            if settings.get('name') is None:
                settings['name'] = wrapped.__name__

        elif settings.get('context') is None:
            raise TypeError('must supply context type for function')

        return wrapped

    return decorate


def calculate_properties(context, request, ns=None, category='object'):
    """ This function computes calculated properties asynchronously by creating a ThreadPool
        and mapping a compute function for all calculated properties collected.
    """
    calculated_properties = request.registry[CALCULATED_PROPERTIES]
    props = calculated_properties.props_for(context, category)
    defined = {name: prop for name, prop in props.items() if prop.define}
    if isinstance(context, type):
        context = None
    namespace = ItemNamespace(context, request, defined, ns)  # maybe pass

    # create threadpool, map compute
    with ThreadPoolExecutor(max_workers=CALC_PROP_THREAD_COUNT) as executor:
        calculated = {}

        # helper function for compuitng the properties and collecting under the above object
        def compute(tuple, _prepare=True):
            def _compute():
                name, prop = tuple[0], tuple[1]
                # print('BEFORE: name: %s, prop: %s' % (name, prop))
                val = prop(namespace)
                # print('AFTER: name: %s, prop val: %s' % (name, val))
                if val is not None:
                    calculated[name] = val
            if _prepare:
                with prepare(request=request):
                    _compute()
            else:
                _compute()

        prop_items = props.items()
        for name, prop in prop_items:
            # Setting all the properties first should avoid threading collisions.
            # This initial value will be clobbered before return.
            calculated[name] = "CALCULATION RESULT MISSING"
        # Start computation of all but reverse links in background
        future = executor.map(compute, [(name, prop) for name, prop in prop_items if 'rev' not in name])
        # Start computation of reverse links in foreground where we have the transaction context
        # implementation-A
        map(functools.partial(compute, _prepare=False), [(name, prop) for name, prop in prop_items if 'rev' in name])
        # implementation-B
        # for name, prop in [(name, prop) for name, prop in props.items() if 'rev' in name]:
        #     val = prop(namespace)
        #     if val is not None:
        #         calculated[name] = val
        # end of A-B
        list(future)  # noqa force the wait

    return calculated
