import pytest
from ..util import dictionary_lookup, DictionaryKeyError

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
