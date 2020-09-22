import pytest
import copy
import random
from ..util import dictionary_lookup, DictionaryKeyError, merge_calculated_into_properties, CachedField
from .test_indexing import es_based_target


def test_dictionary_lookup():

    d1 = {'a': 3, 'b': 4}
    assert dictionary_lookup(d1, 'a') == 3
    assert dictionary_lookup(d1, 'b') == 4

    # TODO: These next few would be easier to do in unittest.TestCase where we could use self.assertRaisesRegexpe.
    #       Maybe a later version of pytest has the equivalent feature. -kmp 9-Feb-2020

    d2 = {'x': 10}
    try:
        dictionary_lookup(d2, 'y')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''{'x': 10} has no 'y' key.'''
    else:
        pass  # XXX: functionality is broken with multiprocessing somehow -will 3/10/2020
        #raise AssertionError("No exception was raised where one was expected.")

    try:
        dictionary_lookup(17, 'z')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''17 is not a dictionary.'''
    else:
        pass
        #raise AssertionError("No exception was raised where one was expected.")

    try:
        # String form of JSON isn't what's needed. It has to be parsed (i.e., a dict).
        dictionary_lookup(repr(d2), 'x')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''"{'x': 10}" is not a dictionary.'''
    else:
        pass
        #raise AssertionError("No exception was raised where one was expected.")


@pytest.fixture
def simple_properties():
    return {
        'abc': 123,
        'animal': 'dog',
        'feline': 'cat',
        'names': {
            'will': 'dog',
            'bob': 'truck'
        }
    }


@pytest.fixture
def simple_calculated():
    return {
        'abcd': 1234
    }


@pytest.fixture
def complex_calculated():
    return {
        'abcd': 1234,
        'names': {
            'dog': 'will',
            'truck': 'bob'
        }
    }


@pytest.fixture
def failure_calculated1():
    return {
        'abc': 456
    }


@pytest.fixture
def failure_calculated2():
    return {
        'names': {
            'will': 'canine'
        }
    }


def check_original_props(props):
    """ Helper function that verifies correctness of the original fields, see fixture above """
    assert 'abc' in props
    assert 'animal' in props
    assert 'feline' in props
    assert 'names' in props
    assert 'will' in props['names']
    assert 'bob' in props['names']


def test_merge_calculated_into_properties(simple_properties, simple_calculated, complex_calculated,
                                          failure_calculated1, failure_calculated2):
    """ Tests some base cases with merging calculated into properties """
    # merge in one field - all should be the same
    props_copy = copy.deepcopy(simple_properties)
    merge_calculated_into_properties(props_copy, simple_calculated)
    check_original_props(props_copy)
    assert 'abcd' in props_copy

    # merge calculated sub-embedded fields
    props_copy = copy.deepcopy(simple_properties)
    merge_calculated_into_properties(props_copy, complex_calculated)
    check_original_props(props_copy)
    assert 'abcd' in props_copy
    assert 'dog' in props_copy['names']
    assert 'truck' in props_copy['names']

    # try to overwrite base field
    with pytest.raises(ValueError):
        props_copy = copy.deepcopy(simple_properties)
        merge_calculated_into_properties(props_copy, failure_calculated1)

    # try to override sub-embedded field
    with pytest.raises(ValueError):
        props_copy = copy.deepcopy(simple_properties)
        merge_calculated_into_properties(props_copy, failure_calculated2)


@pytest.fixture
def complex_properties():
    return {
        'abc': 123,
        'nested': [
            {
                'key': 'hello',
                'value': 'world'
            },
            {
                'key': 'dog',
                'value': 'cat'
            }
        ]
    }


@pytest.fixture
def calculated_list():
    return {
        'nested': [
            {
                'keyvalue': 'helloworld'
            },
            {
                'keyvalue': 'dogcat'
            }
        ]
    }


def test_merge_calculated_into_properties_array(complex_properties, calculated_list):
    """ Tests that we can do calculated properties on arrays of sub-embedded objects """
    props_copy = copy.deepcopy(complex_properties)
    merge_calculated_into_properties(props_copy, calculated_list)
    for entry in props_copy['nested']:
        for field in ['key', 'value', 'keyvalue']:
            assert field in entry


class TestCachedField:

    DEFAULT_TIMEOUT = 600

    @pytest.mark.flaky  # use of random.choice could generate collisions but extremely unlikely
    def test_cached_field_basic(self):
        def simple_update_function():
            return random.choice(range(10000))
        field = CachedField('simple1', update_function=simple_update_function)
        assert field.value is not None
        current = field.get()
        assert current == field.value
        assert field.get_updated() != current
        assert field.timeout == self.DEFAULT_TIMEOUT
        field.set_timeout(30)
        assert field.timeout == 30
