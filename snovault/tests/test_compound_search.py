"""
Unit tests for pure/leaf helper methods on
snovault.search.compound_search.CompoundSearchBuilder that previously had no
direct test coverage in this repo (compound_search has no snovault-level ES
test fixture at all - see test_search_efficiency.py's docstring). These
exercise string/dict manipulation and validation logic directly, without any
ES or Pyramid subrequest dependency.
"""

import pytest
from pyramid.httpexceptions import HTTPBadRequest

from snovault.search.compound_search import CompoundSearchBuilder


class TestCombineQueryStrings:

    def test_merges_distinct_params(self):
        result = CompoundSearchBuilder.combine_query_strings('?type=Item', 'status=released')
        assert result == 'type=Item&status=released'

    def test_duplicate_param_values_are_concatenated_not_overwritten(self):
        result = CompoundSearchBuilder.combine_query_strings('status=released', 'status=archived')
        # both values must survive as separate entries for the same key
        assert result == 'status=released&status=archived'

    def test_leading_question_marks_are_stripped_from_both_sides(self):
        result = CompoundSearchBuilder.combine_query_strings('?a=1', '?b=2')
        assert result == 'a=1&b=2'

    def test_empty_second_string_returns_first_unchanged(self):
        result = CompoundSearchBuilder.combine_query_strings('?type=Item', '')
        assert result == 'type=Item'


class TestAddTypeToFlagIfNeeded:

    def test_adds_type_when_absent(self):
        result = CompoundSearchBuilder._add_type_to_flag_if_needed('status=released', 'type=Item')
        assert result == 'status=released&type=Item'

    def test_returns_bare_type_flag_when_flags_empty(self):
        result = CompoundSearchBuilder._add_type_to_flag_if_needed('', 'type=Item')
        assert result == 'type=Item'

    def test_appends_type_again_even_when_already_present(self):
        # NOTE: the presence check is `if type_flag not in flags or type_flag.lower()
        # not in flags:` (an OR) - since type_flag.lower() is checked against the
        # *original-case* flags string, that second clause is true almost
        # whenever flags isn't already all-lowercase, so this "not already
        # present" guard does not actually prevent duplication in the common
        # case. Documenting current (likely unintended) behavior here rather
        # than the presumably-intended dedup behavior, since this file only
        # closes test gaps and does not change behavior.
        result = CompoundSearchBuilder._add_type_to_flag_if_needed('type=Item&status=released', 'type=Item')
        assert result == 'type=Item&status=released&type=Item'


class TestEsResultsGenerator:

    def test_yields_embedded_frame_for_each_hit(self):
        es_results = {'hits': {'hits': [
            {'_source': {'embedded': {'uuid': '1'}}},
            {'_source': {'embedded': {'uuid': '2'}}},
        ]}}
        results = list(CompoundSearchBuilder.es_results_generator(es_results))
        assert results == [{'uuid': '1'}, {'uuid': '2'}]

    def test_yields_nothing_for_no_hits(self):
        es_results = {'hits': {}}
        assert list(CompoundSearchBuilder.es_results_generator(es_results)) == []


class TestValidateFlag:

    def test_valid_flag_does_not_raise(self):
        CompoundSearchBuilder.validate_flag({'name': 'my_flag', 'query': 'status=released'})

    def test_missing_name_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_flag({'query': 'status=released'})

    def test_missing_query_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_flag({'name': 'my_flag'})

    def test_non_string_name_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_flag({'name': 123, 'query': 'status=released'})

    def test_non_string_query_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_flag({'name': 'my_flag', 'query': ['status=released']})


class TestValidateFilterBlock:

    def test_valid_filter_block_does_not_raise(self):
        CompoundSearchBuilder.validate_filter_block({'query': 'status=released', 'flags_applied': []})

    def test_missing_query_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_filter_block({'flags_applied': []})

    def test_missing_flags_applied_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_filter_block({'query': 'status=released'})

    def test_non_string_query_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_filter_block({'query': 123, 'flags_applied': []})

    def test_non_list_flags_applied_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.validate_filter_block({'query': 'status=released', 'flags_applied': 'not_a_list'})


class TestExtractFilterSetFromSearchBody:
    """ Only exercises the non-'@id' branch (building a filter_set dict from
        a raw POST body) - the '@id' branch delegates to get_item_or_none,
        which needs a real item store and is out of scope for a unit test. """

    def test_missing_type_raises(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.extract_filter_set_from_search_body(request=None, body={})

    def test_minimal_body_with_only_type(self):
        result = CompoundSearchBuilder.extract_filter_set_from_search_body(
            request=None, body={'search_type': 'File'}
        )
        assert result == {'search_type': 'File'}

    def test_flags_must_be_a_list(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.extract_filter_set_from_search_body(
                request=None, body={'search_type': 'File', 'flags': {'name': 'x', 'query': 'y'}}
            )

    def test_filter_blocks_must_be_a_list(self):
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.extract_filter_set_from_search_body(
                request=None, body={'search_type': 'File', 'filter_blocks': {'query': 'y', 'flags_applied': []}}
            )

    def test_valid_flags_and_filter_blocks_pass_through(self):
        body = {
            'search_type': 'File',
            'flags': [{'name': 'my_flag', 'query': 'status=released'}],
            'filter_blocks': [{'query': 'type=File', 'flags_applied': ['my_flag']}],
        }
        result = CompoundSearchBuilder.extract_filter_set_from_search_body(request=None, body=body)
        assert result == body

    def test_invalid_flag_in_list_raises(self):
        body = {
            'search_type': 'File',
            'flags': [{'name': 'my_flag'}],  # missing 'query'
        }
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.extract_filter_set_from_search_body(request=None, body=body)

    def test_invalid_filter_block_in_list_raises(self):
        body = {
            'search_type': 'File',
            'filter_blocks': [{'query': 'type=File'}],  # missing 'flags_applied'
        }
        with pytest.raises(HTTPBadRequest):
            CompoundSearchBuilder.extract_filter_set_from_search_body(request=None, body=body)
