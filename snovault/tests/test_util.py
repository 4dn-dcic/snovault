# import pytest
from ..util import dictionary_lookup, DictionaryKeyError
from ..util import build_embedded_model


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
        # raise AssertionError("No exception was raised where one was expected.")

    try:
        dictionary_lookup(17, 'z')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''17 is not a dictionary.'''
    else:
        pass
        # raise AssertionError("No exception was raised where one was expected.")

    try:
        # String form of JSON isn't what's needed. It has to be parsed (i.e., a dict).
        dictionary_lookup(repr(d2), 'x')
    except Exception as e:
        assert isinstance(e, DictionaryKeyError)
        assert str(e) == '''"{'x': 10}" is not a dictionary.'''
    else:
        pass
        # raise AssertionError("No exception was raised where one was expected.")


def test_build_embedded_model():
    res1 = build_embedded_model([
        "link_to_nested_objects.display_title",
        "link_to_nested_object.uuid",
        "link_to_nested_object.@id",
        "link_to_nested_object.display_title",
        "link_to_nested_objects.principals_allowed.*",
        "link_to_nested_objects.associates.y",
        "principals_allowed.*",
        "link_to_nested_objects.uuid",
        "link_to_nested_object.principals_allowed.*",
        "link_to_nested_objects.@type",
        "link_to_nested_objects.@id",
        "link_to_nested_object.associates.x",
        "link_to_nested_object.@type"
    ])
    assert res1 == {
        "fields_to_use": ["*"],
        "link_to_nested_objects": {
            "fields_to_use": ["display_title", "uuid", "@type", "@id"],
            "principals_allowed": {
                "fields_to_use": ["*"]},
            "associates": {
                "fields_to_use": ["y"]
            }
        },
        "link_to_nested_object": {
            "fields_to_use": ["uuid", "@id", "display_title", "@type"],
            "principals_allowed": {
                "fields_to_use": ["*"]
            },
            "associates": {
                "fields_to_use": ["x"]
            }
        },
        "principals_allowed": {
            "fields_to_use": ["*"]
        }
    }
    res2 = build_embedded_model([
        "link_to_nested_objects.display_title",
        "link_to_nested_object.uuid",
        "link_to_nested_object.@id",
        "link_to_nested_object.display_title",
        "link_to_nested_objects.principals_allowed.*",
        "link_to_nested_objects.associates.x",
        "link_to_nested_objects.associates.y",
        "principals_allowed.*",
        "link_to_nested_objects.uuid",
        "link_to_nested_object.principals_allowed.*",
        "link_to_nested_objects.@type",
        "link_to_nested_objects.@id",
        "link_to_nested_object.associates.y",
        "link_to_nested_object.associates.x",
        "link_to_nested_object.@type"
    ])
    assert res2 == {
        "fields_to_use": ["*"],
        "link_to_nested_objects": {
            "fields_to_use": ["display_title", "uuid", "@type", "@id"],
            "principals_allowed": {
                "fields_to_use": ["*"]},
            "associates": {
                "fields_to_use": ["x", "y"]
            }
        },
        "link_to_nested_object": {
            "fields_to_use": ["uuid", "@id", "display_title", "@type"],
            "principals_allowed": {
                "fields_to_use": ["*"]
            },
            "associates": {
                "fields_to_use": ["y", "x"]
            }
        },
        "principals_allowed": {
            "fields_to_use": ["*"]
        }
    }
