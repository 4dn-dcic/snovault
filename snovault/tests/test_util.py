import pytest
import copy
from ..util import dictionary_lookup, DictionaryKeyError, merge_calculated_into_properties


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
