import pytest
from ..util import dictionary_lookup

def test_dictionary_lookup():

    d1 = {'a': 3, 'b': 4}
    assert dictionary_lookup(d1, 'a') == 3
    assert dictionary_lookup(d1, 'b') == 4

    d2 = {'x': 10}
    try:
        dictionary_lookup(d2, 'y')
    except ValueError as e:
        assert isinstance(e, ValueError)
        assert str(e) == '''{'x': 10} has no 'y' key.'''

    try:
        dictionary_lookup(17, 'z')
    except ValueError as e:
        assert isinstance(e, ValueError)
        assert str(e) == '''17 is not a dictionary.'''

    try:
        dictionary_lookup(repr(d2), 'x')
    except ValueError as e:
        assert isinstance(e, ValueError)
        assert str(e) == '''"{'x': 10}" is not a dictionary.'''
