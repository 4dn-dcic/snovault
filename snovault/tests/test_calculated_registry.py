"""
Unit tests for the snovault.calculated registry classes -- CalculatedProperty
(schema guard/marking, condition/attr dispatch) and CalculatedProperties
(per-category registration, MRO-ordered props_for). These drive every calculated
property in the system but were only covered indirectly through a single
integration test (test_calculated.py). No services required.

Complements (does not replace) test_calculated.py's end-to-end check.
"""
import pytest

from ..calculated import CalculatedProperty, CalculatedProperties, ItemNamespace
from ..interfaces import CONNECTION


pytestmark = [pytest.mark.unit]


class TestCalculatedProperty:

    def test_schema_with_default_is_rejected(self):
        with pytest.raises(ValueError) as excinfo:
            CalculatedProperty(fn=lambda: 1, name='x',
                               schema={'type': 'string', 'default': 'nope'})
        assert 'default' in str(excinfo.value)

    def test_schema_is_copied_and_marked_calculated(self):
        original = {'type': 'string'}
        prop = CalculatedProperty(fn=lambda: 1, name='x', schema=original)
        assert prop.schema == {'type': 'string', 'calculatedProperty': True}
        # the caller's schema dict must not be mutated
        assert original == {'type': 'string'}

    def test_no_schema_stays_none(self):
        prop = CalculatedProperty(fn=lambda: 1, name='x')
        assert prop.schema is None

    def test_call_invokes_fn_through_namespace(self):
        prop = CalculatedProperty(fn=lambda: 'computed', name='x')
        namespace = ItemNamespace(context=None, request=None)
        assert prop(namespace) == 'computed'

    def test_call_returns_none_when_condition_fails(self):
        prop = CalculatedProperty(fn=lambda: 'computed', name='x',
                                  condition=lambda: False)
        namespace = ItemNamespace(context=None, request=None)
        assert prop(namespace) is None

    def test_call_computes_when_condition_passes(self):
        prop = CalculatedProperty(fn=lambda: 'computed', name='x',
                                  condition=lambda: True)
        namespace = ItemNamespace(context=None, request=None)
        assert prop(namespace) == 'computed'

    def test_fn_args_are_resolved_from_namespace(self):
        # fn parameters are looked up by name on the namespace; ns entries
        # populate the namespace's __dict__.
        prop = CalculatedProperty(fn=lambda first, last: first + ' ' + last, name='x')
        namespace = ItemNamespace(context=None, request=None,
                                  ns={'first': 'Ada', 'last': 'Lovelace'})
        assert prop(namespace) == 'Ada Lovelace'

    def test_attr_dispatches_to_context_method(self):
        class Ctx:
            def display_title(self):
                return 'from attr'

        prop = CalculatedProperty(fn=None, name='display_title', attr='display_title')
        namespace = ItemNamespace(context=Ctx(), request=None)
        assert prop(namespace) == 'from attr'


class TestItemNamespaceCall:

    def test_results_are_memoized_per_fn(self):
        calls = []

        def fn():
            calls.append(1)
            return 'value'

        namespace = ItemNamespace(context=None, request=None)
        assert namespace(fn) == 'value'
        assert namespace(fn) == 'value'
        assert len(calls) == 1

    def test_string_fn_resolves_attribute_or_none(self):
        # For the unknown-name branch the namespace must be able to walk
        # __getattr__ to its terminal AttributeError, which touches the
        # connection registry, item properties, and rev links along the way.
        class FakeContext:
            rev = {}

        namespace = ItemNamespace(
            context=FakeContext(), request=None,
            ns={'known': 42, 'registry': {CONNECTION: None}, '_properties': {}})
        assert namespace('known') == 42
        assert namespace('unknown') is None


class TestCalculatedPropertiesRegistry:

    @pytest.fixture
    def hierarchy(self):
        class Base:
            pass

        class Sub(Base):
            pass

        return Base, Sub

    def test_props_for_inherits_from_base(self, hierarchy):
        Base, Sub = hierarchy
        registry = CalculatedProperties()
        registry.register_prop(lambda: 'baseonly', 'base_only', Base)
        props = registry.props_for(Sub)
        assert set(props) == {'base_only'}
        assert props['base_only'].fn() == 'baseonly'

    def test_subclass_registration_overrides_base(self, hierarchy):
        Base, Sub = hierarchy
        registry = CalculatedProperties()
        registry.register_prop(lambda: 'base', 'shared', Base)
        registry.register_prop(lambda: 'sub', 'shared', Sub)
        assert registry.props_for(Sub)['shared'].fn() == 'sub'
        # base class itself still sees its own registration
        assert registry.props_for(Base)['shared'].fn() == 'base'

    def test_props_for_accepts_instance_or_class(self, hierarchy):
        Base, Sub = hierarchy
        registry = CalculatedProperties()
        registry.register_prop(lambda: 'v', 'prop', Base)
        assert set(registry.props_for(Sub())) == set(registry.props_for(Sub)) == {'prop'}

    def test_categories_are_isolated(self, hierarchy):
        Base, Sub = hierarchy
        registry = CalculatedProperties()
        registry.register_prop(lambda: 'obj', 'objprop', Base, category='object')
        registry.register_prop(lambda: 'page', 'pageprop', Base, category='page')
        assert set(registry.props_for(Sub, category='object')) == {'objprop'}
        assert set(registry.props_for(Sub, category='page')) == {'pageprop'}
        assert registry.props_for(Sub, category='missing') == {}
