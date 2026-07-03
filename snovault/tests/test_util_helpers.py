"""
Unit tests for pure helper functions in snovault.util that were not directly
covered by tests/test_util.py. These are all deterministic and need no live
services. (test_util.py already covers dictionary_lookup, CachedField and the
common merge_calculated_into_properties paths; this file targets the remaining
untested helpers and the merge edge branches.)
"""
import gzip
import os

import pytest

from ..util import (
    deduplicate_list,
    ensurelist,
    gunzip_content,
    convert_integer_to_comma_string,
    simple_path_ids,
    recursively_process_field,
    resolve_file_path,
    merge_calculated_into_properties,
)


pytestmark = [pytest.mark.unit]


class TestDeduplicateList:

    def test_removes_duplicates(self):
        assert sorted(deduplicate_list([1, 1, 2, 3, 3])) == [1, 2, 3]

    def test_empty_list(self):
        assert deduplicate_list([]) == []

    def test_accepts_tuple_returns_list(self):
        result = deduplicate_list(('a', 'a', 'b'))
        assert isinstance(result, list)
        assert sorted(result) == ['a', 'b']

    def test_unhashable_elements_raise(self):
        with pytest.raises(TypeError):
            deduplicate_list([{'a': 1}, {'a': 1}])


class TestEnsurelist:

    def test_string_becomes_singleton_list(self):
        assert ensurelist('abc') == ['abc']

    def test_list_passes_through(self):
        value = ['a', 'b']
        assert ensurelist(value) is value

    def test_none_passes_through(self):
        assert ensurelist(None) is None

    def test_int_passes_through(self):
        # Only str is special-cased; other scalars are returned unchanged.
        assert ensurelist(5) == 5


class TestGunzipContent:

    def test_round_trip(self):
        assert gunzip_content(gzip.compress(b'hello world')) == 'hello world'

    def test_round_trip_unicode(self):
        assert gunzip_content(gzip.compress('héllo'.encode('utf-8'))) == 'héllo'

    def test_malformed_content_raises(self):
        with pytest.raises(Exception):
            gunzip_content(b'not gzipped data')


class TestConvertIntegerToCommaString:

    def test_formats_large_integer(self):
        assert convert_integer_to_comma_string(1234567) == '1,234,567'

    def test_small_integer(self):
        assert convert_integer_to_comma_string(42) == '42'

    def test_negative_integer(self):
        assert convert_integer_to_comma_string(-1234567) == '-1,234,567'

    def test_non_integer_returns_none(self):
        assert convert_integer_to_comma_string('123') is None
        assert convert_integer_to_comma_string(1.5) is None
        assert convert_integer_to_comma_string(None) is None


class TestSimplePathIds:

    def test_leaf_with_empty_path_yields_object(self):
        assert list(simple_path_ids('x', [])) == ['x']

    def test_dotted_string_path(self):
        obj = {'a': {'b': 'value'}}
        assert list(simple_path_ids(obj, 'a.b')) == ['value']

    def test_flattens_lists(self):
        obj = {'a': {'b': [1, 2, 3]}}
        assert list(simple_path_ids(obj, 'a.b')) == [1, 2, 3]

    def test_missing_key_yields_nothing(self):
        assert list(simple_path_ids({'a': 1}, 'x.y')) == []

    def test_none_value_yields_nothing(self):
        assert list(simple_path_ids({'a': None}, 'a')) == []

    def test_list_path_argument(self):
        obj = {'a': {'b': 'value'}}
        assert list(simple_path_ids(obj, ['a', 'b'])) == ['value']


class TestRecursivelyProcessField:

    def test_simple_nested(self):
        assert recursively_process_field({'a': {'b': 'v'}}, ['a', 'b']) == 'v'

    def test_list_at_intermediate_level(self):
        assert recursively_process_field({'a': [{'b': 1}, {'b': 2}]}, ['a', 'b']) == [1, 2]

    def test_missing_intermediate_returns_none(self):
        assert recursively_process_field({'a': None}, ['a', 'b']) is None

    def test_cannot_drill_into_scalar_returns_scalar(self):
        # A scalar encountered before the path ends is returned as-is.
        assert recursively_process_field({'a': 5}, ['a', 'b']) == 5

    def test_scalar_top_level_returns_item(self):
        # AttributeError on .get -> returns the item itself.
        assert recursively_process_field('scalar', ['a']) == 'scalar'

    def test_single_field_path(self):
        assert recursively_process_field({'a': 'v'}, ['a']) == 'v'


class TestResolveFilePath:

    def test_relative_to_default_root(self):
        result = resolve_file_path('schemas/foo.json')
        assert os.path.isabs(result)
        assert result.endswith(os.path.join('schemas', 'foo.json'))

    def test_relative_to_file_loc(self):
        result = resolve_file_path('bar.json', file_loc='/some/dir/module.py')
        assert result == os.path.join('/some/dir', 'bar.json')

    def test_custom_root_dir(self):
        result = resolve_file_path('x.json', root_dir='/custom/root')
        assert result == os.path.join('/custom/root', 'x.json')

    def test_tilde_expansion(self):
        result = resolve_file_path('~/thing.json', root_dir='/ignored')
        assert '~' not in result
        assert result == os.path.expanduser('~/thing.json')


class TestMergeCalculatedEdgeCases:
    """ Complements the base-path tests in test_util.py. """

    def test_uuid_type_mismatch_is_tolerated(self):
        # Special-case: a 'uuid' key with mismatched calc/props types does NOT raise;
        # the base property value is preserved (added for frame=raw view handling).
        props = {'uuid': 'base-value'}
        merge_calculated_into_properties(props, {'uuid': ['calc', 'value']})
        assert props == {'uuid': 'base-value'}

    def test_non_uuid_type_mismatch_raises(self):
        props = {'foo': 'a-string'}
        with pytest.raises(ValueError):
            merge_calculated_into_properties(props, {'foo': ['a', 'list']})

    def test_list_of_dicts_merged_by_position(self):
        props = {'items': [{'a': 1}, {'a': 2}]}
        merge_calculated_into_properties(props, {'items': [{'b': 10}, {'b': 20}]})
        assert props == {'items': [{'a': 1, 'b': 10}, {'a': 2, 'b': 20}]}

    def test_new_key_is_added(self):
        props = {'existing': 1}
        merge_calculated_into_properties(props, {'brand_new': 2})
        assert props == {'existing': 1, 'brand_new': 2}
