import pytest  # noQA
from snovault.schema_validation import NO_DEFAULT
from snovault.schema_utils import SchemaValidator
from jsonschema.exceptions import ValidationError


validator_class = SchemaValidator


fake_schema = {
    '$id': 'abc',
    'title': 'Fake',
    'description': 'Schema',
    '$schema': 'https://json-schema.org/draft/2020-12/schema',
    'type': 'object',
    'required': ['award', 'lab'],
    'identifyingProperties': ['uuid'],
    'additionalProperties': False,
    'properties': {
        'uuid': {
            'type': 'string'
        },
        'award': {
            'type': 'string'
        },
        'lab': {
            'type': 'string'
        }
    }
}


def make_default(instance, subschema):
    if instance.get('skip'):
        return NO_DEFAULT
    assert subschema == {'serverDefault': 'test'}
    return 'bar'


def test_validator_serializes_default_properties():
    schema = {
        'properties': {
            'foo': {
                'default': 'bar'
            }
        }
    }
    result, errors = validator_class(
        schema
    ).serialize(
        {}
    )
    assert result == {'foo': 'bar'}
    assert errors == []


def test_validator_serializes_default_properties_in_items():
    schema = {
        'items': {
            'properties': {
                'foo': {
                    'default': 'bar'
                }
            }
        }
    }
    result, errors = validator_class(
        schema,
    ).serialize(
        [
            {}
        ]
    )
    assert result == [{'foo': 'bar'}]
    assert errors == []


def test_validator_serializes_server_default_properties():
    schema = {
        'properties': {
            'foo': {
                'serverDefault': 'test'
            }
        }
    }
    result, errors = validator_class(
        schema,
    ).add_server_defaults(
        {
            'test': make_default
        }
    ).serialize(
        {}
    )
    assert result == {'foo': 'bar'}
    assert errors == []


def test_validator_ignores_server_default_returning_no_default():
    schema = {
        'properties': {
            'foo': {
                'serverDefault': 'test'
            },
            'skip': {}
        }
    }
    result, errors = validator_class(
        schema,
    ).add_server_defaults(
        {
            'test': make_default
        }
    ).serialize(
        {
            'skip': True
        }
    )
    assert result == {'skip': True}
    assert errors == []


def test_validator_serializes_server_default_properties_in_items():
    schema = {
        'items': {
            'properties': {
                'foo': {
                    'serverDefault': 'test'
                }
            }
        }
    }
    result, errors = SchemaValidator(
        schema
    ).add_server_defaults(
        {
            'test': make_default
        }
    ).serialize(
        [{}]
    )
    assert result == [{'foo': 'bar'}]
    assert errors == []


def test_validator_serializes_properties_in_order_of_input():
    schema = {
        'properties': {
            'foo': {},
            'bar': {},
        }
    }
    validator = validator_class(
        schema
    )
    value = {
        'bar': 1,
        'foo': 2
    }
    result, errors = validator.serialize(value)
    assert list(result) == ['bar', 'foo']
    assert errors == []


def test_validator_returns_error():
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string'
            }
        },
        'required': ['name']
    }
    result, errors = validator_class(
        schema,
    ).serialize(
        {
            'name': 'abc'
        }
    )
    assert result == {'name': 'abc'}
    assert not errors
    result, errors = validator_class(
        schema,
    ).serialize(
        {
            'name': 1
        }
    )
    assert result == {
        'name': 1
    }
    assert isinstance(errors[0], ValidationError)
    assert errors[0].message == "1 is not of type 'string'"
    result, errors = validator_class(
        schema,
    ).serialize(
        {}
    )
    assert result == {}
    assert isinstance(errors[0], ValidationError)
    assert errors[0].message == "'name' is a required property"


def test_validator_check_schema():
    validator_class.check_schema(fake_schema)


def test_validator_extend_with_default():
    from copy import deepcopy
    from snovault.schema_validation import SerializingSchemaValidator
    original_instance = {
        'x': 'y'
    }
    mutated_instance = deepcopy(original_instance)
    assert original_instance == mutated_instance
    schema = {'properties': {'foo': {'default': 'bar'}}}
    SerializingSchemaValidator(schema).validate(mutated_instance)
    assert original_instance == {'x': 'y'}
    assert mutated_instance == {'x': 'y', 'foo': 'bar'}


def test_validator_extend_with_default_and_serialize():
    instance = {
        'x': 'y'
    }
    from snovault.schema_validation import SerializingSchemaValidator
    schema = {'properties': {'foo': {'default': 'bar'}}}
    result, errors = SerializingSchemaValidator(schema).serialize(instance)
    assert instance == {'x': 'y'}
    assert result == {'x': 'y', 'foo': 'bar'}
    assert errors == []
    schema = {
        'properties': {
            'foo': {
                'default': 'bar'
            },
            'name': {
                'type': 'string'
            }
        },
        'required': ['name']
    }
    result, errors = SerializingSchemaValidator(schema).serialize(
        {
            'foo': 'thing',
        }
    )
    assert result == {'foo': 'thing'}
    assert errors[0].message == "'name' is a required property"


def test_validator_extend_with_server_default_and_serialize():
    instance = {
        'x': 'y'
    }
    from snovault.schema_validation import SerializingSchemaValidator
    schema = {'properties': {'foo': {'serverDefault': 'test'}}}
    result, errors = SerializingSchemaValidator(
        schema,
    ).add_server_defaults(
        {
            'test': make_default
        }
    ).serialize(instance)
    assert instance == {'x': 'y'}
    assert result == {'x': 'y', 'foo': 'bar'}
    assert errors == []
    schema = {
        'properties': {
            'foo': {
                'serverDefault': 'test'
            },
            'name': {
                'type': 'string'
            }
        },
        'required': ['name']
    }
    result, errors = SerializingSchemaValidator(
        schema
    ).add_server_defaults(
        {
            'test': make_default
        }
    ).serialize(
        {
            'foo': 'thing',
        }
    )
    assert result == {'foo': 'thing'}
    assert errors[0].message == "'name' is a required property"
    result, errors = SerializingSchemaValidator(
        schema,
    ).add_server_defaults(
        {
            'test': make_default
        }
    ).serialize(
        {
            'name': 'other thing',
        }
    )
    assert result == {
        'foo': 'bar',
        'name': 'other thing',
    }
    assert not errors


@pytest.mark.parametrize('obj, expected', [
    ({
        'accession': 'not valid'
    }, False),
    ({
        'accession': 'SNOnotvalid'
    }, False),
    ({
        'accession': 'TSTnotvalid'
    }, False),
    ({
        'accession': 'TSTabcclose'
    }, False),
    ({
         'accession': 'TSTAB439abcd'  # lowercase fails
     }, False),
    ({
         'accession': 'TSTAB4399428'  # real one that matches
     }, True),
    ({
         'accession': 'TSTAB439ABCD'  # real one that matches (can be trailing characters)
     }, True),
    ({}, True),  # serverDefault should work
])
def test_custom_validator_with_real_type(testapp, obj, expected):
    """ Tests that we can validate accessions (or other nonstandard format fields) """
    if not expected:
        testapp.post_json('/TestingServerDefault', obj, status=422)
    else:
        testapp.post_json('/TestingServerDefault', obj, status=201)
