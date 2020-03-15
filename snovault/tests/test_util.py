import os
import pytest
from ..util import dictionary_lookup, DictionaryKeyError, set_or_remove, dictionary_bindings, environ_bindings

def test_set_or_remove():

    props = {'a': 1, 'b': 2, 'c': 3}
    set_or_remove(props, 'a', 11)
    assert props == {'a': 11, 'b': 2, 'c': 3}
    set_or_remove(props, 'a', None)
    assert props == {'b': 2, 'c': 3}


def test_dictionary_bindings():

    props = {'a': 1, 'b': 5, 'z': 20, 'zz': 200}
    with dictionary_bindings(props, a=0, c=99, x=None, z=props['z'], zz=props['zz']):
        assert props == {'a': 0, 'b': 5, 'c': 99, 'z': 20, 'zz': 200}
        props['a'] = 11
        assert props == {'a': 11, 'b': 5, 'c': 99, 'z': 20, 'zz': 200}
        del props['c']
        assert props == {'a': 11, 'b': 5, 'z': 20, 'zz': 200}
        props['x'] = 17
        assert props == {'a': 11, 'b': 5, 'x': 17, 'z': 20, 'zz': 200}
        props['y'] = 66
        assert props == {'a': 11, 'b': 5, 'x': 17, 'y': 66, 'z': 20, 'zz': 200}
        del props['zz']
        assert props == {'a': 11, 'b': 5, 'x': 17, 'y': 66, 'z': 20}
    assert props == {'a': 1, 'b': 5, 'y': 66, 'z': 20, 'zz': 200}

def test_environ_bindings():

    keys = tuple("TEST_ENVIRON_BINDINGS_INTERNAL_%s" % n for n in range(6))
    KEY0, KEY1, KEY2, KEY3, KEY4, KEY5 = keys

    # Make sure the test cleaned up well the last time.
    for key in keys:
        assert key not in os.environ

    try:

        os.environ[KEY0] = "outer"
        os.environ[KEY1] = "outer"
        os.environ[KEY2] = "outer"

        def check_outer_state():
            assert os.environ[KEY0] == "outer"
            assert os.environ[KEY1] == "outer"
            assert os.environ[KEY2] == "outer"

        check_outer_state()

        # These will not be bound, so innert changes might affect them.
        assert KEY3 not in os.environ
        os.environ[KEY4] = "outer"
        assert KEY5 not in os.environ

        with environ_bindings(**{KEY0: "inner", KEY1: "inner", KEY2: None, KEY3: None}):

            # Verify binding state
            assert os.environ[KEY0] == "inner"
            assert os.environ[KEY1] == "inner"
            assert KEY2 not in os.environ
            assert KEY3 not in os.environ
            assert os.environ[KEY4] == "outer"

            os.environ[KEY0] = "new_inner"
            del os.environ[KEY1]

            os.environ[KEY2] = "new_inner"

            os.environ[KEY3] = "inner_value"
            del os.environ[KEY4]
            os.environ[KEY5] = "inner_value"

        check_outer_state()

        # Verify that inner changes are still in play here.
        # They get cleaned up in the finally clause below.
        assert KEY3 not in os.environ
        # there was no binding, so inner effect shows through for these next two
        assert KEY4 not in os.environ
        assert os.environ[KEY5]  == "inner_value"

    finally:

        for key in keys:
            if key in os.environ:
                del os.environ[key]


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
        raise AssertionError("No exception was raised where one was expected.")

    try:
        dictionary_lookup(17, 'z')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''17 is not a dictionary.'''
    else:
        raise AssertionError("No exception was raised where one was expected.")

    try:
        # String form of JSON isn't what's needed. It has to be parsed (i.e., a dict).
        dictionary_lookup(repr(d2), 'x')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''"{'x': 10}" is not a dictionary.'''
    else:
        raise AssertionError("No exception was raised where one was expected.")
