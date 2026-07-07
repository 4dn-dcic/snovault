"""
Unit tests for snovault.typedsheets -- the pure value-casting helpers used when
loading typed CSV/insert data. This module had no direct test coverage; the
functions here are fully deterministic (the only dependency is
``pyramid.settings.asbool``) so they can be exercised without any live services.
"""
import pytest

from .. import typedsheets as ts


pytestmark = [pytest.mark.unit]


class TestCast:

    def test_cast_defaults_to_string_when_no_types(self):
        # An empty type stack falls back to 'string', so the value passes through.
        assert ts.cast([], 'hello') == 'hello'

    def test_cast_strips_whitespace(self):
        assert ts.cast(['string'], '  spaced  ') == 'spaced'

    @pytest.mark.parametrize('type_name', ['string', 'integer', 'number', 'boolean'])
    def test_cast_null_is_none_regardless_of_type(self, type_name):
        # The literal 'null' (any case) always becomes None, before type parsing.
        assert ts.cast([type_name], 'null') is None
        assert ts.cast([type_name], 'NULL') is None

    def test_cast_empty_non_string_is_none(self):
        assert ts.cast(['integer'], '') is None
        assert ts.cast(['number'], '') is None

    def test_cast_empty_string_type_stays_empty(self):
        # Asymmetry worth pinning: '' for a 'string' type is kept as '', NOT None.
        assert ts.cast(['string'], '') == ''

    def test_cast_integer(self):
        assert ts.cast(['integer'], '42') == 42

    def test_cast_number_int_then_float(self):
        assert ts.parse_number([], '5') == 5
        assert isinstance(ts.parse_number([], '5'), int)
        assert ts.parse_number([], '5.5') == 5.5
        assert isinstance(ts.parse_number([], '5.5'), float)

    def test_cast_number_non_numeric_raises(self):
        with pytest.raises(ValueError):
            ts.parse_number([], 'not-a-number')

    def test_parse_integer_rejects_float_string(self):
        with pytest.raises(ValueError):
            ts.parse_integer([], '5.5')

    @pytest.mark.parametrize('raw,expected', [
        ('true', True), ('True', True), ('1', True), ('yes', True),
        ('false', False), ('False', False), ('0', False), ('no', False),
    ])
    def test_cast_boolean(self, raw, expected):
        assert ts.cast(['boolean'], raw) is expected

    def test_parse_string_asserts_no_remaining_types(self):
        # The scalar parsers assert their type stack is empty; misordered casts fail loudly.
        with pytest.raises(AssertionError):
            ts.parse_integer(['array'], '5')


class TestParseArray:

    def test_parse_array_basic(self):
        assert ts.parse_array([], 'a;b;c') == ['a', 'b', 'c']

    def test_parse_array_skips_blank_entries(self):
        assert ts.parse_array([], 'a;;b; ;c') == ['a', 'b', 'c']

    def test_parse_array_empty(self):
        assert ts.parse_array([], '') == []

    def test_parse_array_of_integers(self):
        assert ts.parse_array(['integer'], '1;2;3') == [1, 2, 3]


class TestParseObject:

    def test_parse_object_basic(self):
        assert ts.parse_object([], 'k:v,x:y') == {'k': 'v', 'x': 'y'}

    def test_parse_object_strips_keys_and_values(self):
        assert ts.parse_object([], ' k : v ') == {'k': 'v'}

    def test_parse_object_empty_yields_empty_dict(self):
        # The generator's `if value.strip()` guard means empty input -> {}.
        assert ts.parse_object([], '') == {}

    def test_parse_object_value_without_colon_raises(self):
        # 'k:v,bad' -> the 'bad' item has no ':' so split(':', 1) yields one element.
        with pytest.raises(ValueError):
            ts.parse_object([], 'k:v,bad')


class TestConvert:

    def test_convert_simple(self):
        assert ts.convert('field:integer', '5') == ('field', 5)

    def test_convert_plain_field_is_string(self):
        assert ts.convert('name', 'value') == ('name', 'value')

    def test_convert_array_of_integers(self):
        # Casts apply right-to-left: pop 'array', which casts each element as integer.
        assert ts.convert('f:integer:array', '1;2;3') == ('f', [1, 2, 3])

    def test_convert_ignore_yields_none(self):
        assert ts.convert('f:ignore', 'whatever') == ('f', None)


class TestRowGenerators:

    def test_cast_row_values(self):
        rows = [{'a:integer': '5', 'b': 'hi'}]
        assert list(ts.cast_row_values(rows)) == [{'a': 5, 'b': 'hi'}]

    def test_cast_row_values_coerces_none_to_empty_string(self):
        # `value or ''` protects against a None cell in the source dict.
        assert list(ts.cast_row_values([{'a': None}])) == [{'a': ''}]

    def test_remove_nulls_drops_none_and_blank_names(self):
        rows = [{'a': None, 'b': 1, '': 2, 'c': 0}]
        # None values dropped; the entry with a falsy name ('') dropped; 0 kept.
        assert list(ts.remove_nulls(rows)) == [{'b': 1, 'c': 0}]

    def test_remove_nulls_empty(self):
        assert list(ts.remove_nulls([])) == []
