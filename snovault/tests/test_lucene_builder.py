"""
Unit tests for pure/leaf helper methods on snovault.search.lucene_builder.LuceneBuilder
that previously had zero test coverage anywhere in the repo (confirmed via grep -
no snovault/tests/*.py file references LuceneBuilder before this file). These
exercise the lucene query-dict construction logic directly, without any ES
dependency.
"""

import pytest
from webob.multidict import MultiDict

from snovault.search.lucene_builder import LuceneBuilder
from snovault.search.search_utils import QueryConstructionException


class DummyTypeInfo:

    def __init__(self, schema=None):
        self.schema = schema or {}


class DummyRequest:

    def __init__(self, params, path='/search/', registry=None):
        self.normalized_params = MultiDict(params)
        self.path = path
        self.registry = registry or {'types': {'Item': DummyTypeInfo()}}


class TestHandleShouldQuery:

    def test_builds_terms_should_query(self):
        result = LuceneBuilder.handle_should_query('embedded.status.raw', ['released', 'archived'])
        assert result == {'bool': {'should': {'terms': {'embedded.status.raw': ['released', 'archived']}}}}


class TestExtractFieldFromTo:

    def test_matches_from_suffix(self):
        matched, field, direction = LuceneBuilder.extract_field_from_to('date_created.from')
        assert (matched, field, direction) == (True, 'date_created', 'from')

    def test_matches_to_suffix_on_nested_field(self):
        matched, field, direction = LuceneBuilder.extract_field_from_to('files.file_size.to')
        assert (matched, field, direction) == (True, 'files.file_size', 'to')

    def test_no_match_returns_false_and_nones(self):
        matched, field, direction = LuceneBuilder.extract_field_from_to('status')
        assert (matched, field, direction) == (False, None, None)


class TestCanonicalizeBoundsAndRangeIncludesZero:
    """ canonicalize_bounds normalizes a range filter (gt/gte/lt/lte) to an
        inclusive-lower/exclusive-upper (lower, upper) pair by nudging
        exclusive bounds by SMALLEST_NONZERO_IEEE_32 (~1.1754e-38);
        range_includes_zero uses that to decide whether a numeric range
        filter should also match documents with no value for the field
        ('add_no_value' schema flag). """

    def test_gte_lte_bounds_pass_through_unchanged_at_this_magnitude(self):
        # At magnitude 10, float64 precision (~1e-15 absolute here) completely
        # swallows the ~1e-38 epsilon, so the "exclusive" nudge on lte has no
        # observable effect away from zero - only the sign/direction matters
        # for range_includes_zero's purposes.
        lower, upper = LuceneBuilder.canonicalize_bounds({'gte': 0, 'lte': 10})
        assert lower == 0
        assert upper == 10.0

    def test_gt_lt_bounds_pass_through_unchanged_at_this_magnitude(self):
        lower, upper = LuceneBuilder.canonicalize_bounds({'gt': 5, 'lt': 10})
        assert lower == 5.0
        assert upper == 10

    def test_epsilon_nudge_is_only_observable_exactly_at_zero(self):
        # The epsilon is only large enough to perturb the boundary when the
        # pivot itself is at (or extremely near) zero. Exclusive bounds are
        # nudged *away* from the pivot (gt up, lt down) so an inclusive (<=)
        # comparison against the nudged value correctly excludes the pivot.
        lower, _ = LuceneBuilder.canonicalize_bounds({'gt': 0})
        assert lower > 0
        _, upper = LuceneBuilder.canonicalize_bounds({'lt': 0})
        assert upper < 0
        _, upper = LuceneBuilder.canonicalize_bounds({'lte': 0})
        assert upper > 0

    def test_range_including_zero(self):
        assert LuceneBuilder.range_includes_zero({'gte': -5, 'lte': 5}) is True

    def test_range_excluding_zero_positive(self):
        assert LuceneBuilder.range_includes_zero({'gte': 1, 'lte': 5}) is False

    def test_range_excluding_zero_negative(self):
        assert LuceneBuilder.range_includes_zero({'gte': -5, 'lte': -1}) is False

    def test_range_with_gte_zero_includes_zero(self):
        assert LuceneBuilder.range_includes_zero({'gte': 0, 'lte': 10}) is True

    def test_range_with_exclusive_gt_zero_boundary_excludes_zero(self):
        # `gt: 0` (strictly greater than zero) must exclude zero.
        assert LuceneBuilder.range_includes_zero({'gt': 0, 'lte': 10}) is False

    def test_range_with_exclusive_lt_zero_boundary_excludes_zero(self):
        # `lt: 0` (strictly less than zero) must exclude zero.
        assert LuceneBuilder.range_includes_zero({'gte': -10, 'lt': 0}) is False


class TestConstructNestedSubQueries:

    def test_no_filters_returns_empty_dict(self):
        result = LuceneBuilder.construct_nested_sub_queries('embedded.foo', {'must_terms': []}, key='must_terms')
        assert result == {}

    def test_single_filter_uses_match(self):
        result = LuceneBuilder.construct_nested_sub_queries(
            'embedded.foo', {'must_terms': ['bar']}, key='must_terms'
        )
        assert result == {'match': {'embedded.foo': 'bar'}}

    def test_multiple_filters_combine_with_should(self):
        result = LuceneBuilder.construct_nested_sub_queries(
            'embedded.foo', {'must_terms': ['bar', 'baz']}, key='must_terms'
        )
        assert result == {'bool': {'should': [
            {'match': {'embedded.foo': 'bar'}},
            {'match': {'embedded.foo': 'baz'}},
        ]}}

    def test_invalid_key_raises_query_construction_exception(self):
        with pytest.raises(QueryConstructionException):
            LuceneBuilder.construct_nested_sub_queries('embedded.foo', {}, key='not_a_real_key')


class TestCreateFieldFilters:
    """ Ported from Fourfront to support the group_by facet. Builds a single
        bool/must+must_not query from a dict of {query_field: {must_terms, must_not_terms}}. """

    def test_must_and_must_not_terms(self):
        field_filters = {
            'embedded.status.raw': {'must_terms': ['released'], 'must_not_terms': ['deleted']}
        }
        result = LuceneBuilder.create_field_filters(field_filters)
        assert result == {'bool': {
            'must': [{'terms': {'embedded.status.raw': ['released']}}],
            'must_not': [{'terms': {'embedded.status.raw': ['deleted']}}],
        }}

    def test_no_filters_produces_empty_must_and_must_not(self):
        field_filters = {'embedded.status.raw': {'must_terms': [], 'must_not_terms': []}}
        result = LuceneBuilder.create_field_filters(field_filters)
        assert result == {'bool': {'must': [], 'must_not': []}}


class TestHandleRangeFilters:
    """ handle_range_filters is the core translation of URL range-query params
        (field.from=, field.to=) into ES range filter dicts, including
        date-vs-numerical detection, default time-of-day correction for
        date-only terms, and widening when multiple overlapping ranges are
        given for the same field. It had zero test coverage. """

    def _numeric_schema_request(self, params):
        return DummyRequest(params, registry={
            'types': {'Item': DummyTypeInfo({'properties': {'count': {'type': 'integer'}}})}
        })

    def test_ordinary_term_filter_is_recorded_in_result_filters(self):
        request = DummyRequest([('status', 'released')])
        result = {'filters': []}
        range_filters = LuceneBuilder.handle_range_filters(request, result, {}, ['Item'])
        assert range_filters == {}
        assert result['filters'] == [{'field': 'status', 'term': 'released',
                                       'remove': '/search/?type=Item'}]

    def test_q_and_excluded_params_are_skipped(self):
        request = DummyRequest([('q', 'foo'), ('limit', '25'), ('frame', 'embedded')])
        result = {'filters': []}
        LuceneBuilder.handle_range_filters(request, result, {}, ['Item'])
        assert result['filters'] == []

    def test_numerical_range_from_to(self, monkeypatch):
        import snovault.search.lucene_builder as lucene_builder_module
        monkeypatch.setattr(
            lucene_builder_module, 'schema_for_field',
            lambda field, request, doc_types: {'type': 'integer'}
        )
        request = DummyRequest([('count.from', '5'), ('count.to', '10')])
        result = {'filters': []}
        range_filters = LuceneBuilder.handle_range_filters(request, result, {}, ['Item'])
        assert range_filters == {'embedded.count': {'gte': '5', 'lte': '10'}}

    def test_date_range_defaults_time_of_day_when_missing(self, monkeypatch):
        import snovault.search.lucene_builder as lucene_builder_module
        monkeypatch.setattr(
            lucene_builder_module, 'schema_for_field',
            lambda field, request, doc_types: {'format': 'date'}
        )
        request = DummyRequest([('date_created.from', '2024-01-01'), ('date_created.to', '2024-01-31')])
        result = {'filters': []}
        range_filters = LuceneBuilder.handle_range_filters(request, result, {}, ['Item'])
        # 'from' -> gte with 00:00 appended; 'to' -> lte with 23:59 appended
        assert range_filters['embedded.date_created']['gte'] == '2024-01-01 00:00'
        assert range_filters['embedded.date_created']['lte'] == '2024-01-31 23:59'
        assert range_filters['embedded.date_created']['format'] == 'yyyy-MM-dd HH:mm'

    def test_overlapping_ranges_widen_to_the_looser_bound(self, monkeypatch):
        import snovault.search.lucene_builder as lucene_builder_module
        monkeypatch.setattr(
            lucene_builder_module, 'schema_for_field',
            lambda field, request, doc_types: {'type': 'integer'}
        )
        request = DummyRequest([('count.from', '5'), ('count.from', '2')])
        result = {'filters': []}
        range_filters = LuceneBuilder.handle_range_filters(request, result, {}, ['Item'])
        # two overlapping gte filters on the same field - the wider (smaller) bound wins
        assert range_filters['embedded.count']['gte'] == '2'

    def test_not_qualifier_still_records_filter_without_treating_as_range(self):
        request = DummyRequest([('status!', 'deleted')])
        result = {'filters': []}
        range_filters = LuceneBuilder.handle_range_filters(request, result, {}, ['Item'])
        assert range_filters == {}
        assert result['filters'][0]['field'] == 'status!'

    def test_no_value_term_marks_field_filters_add_no_value(self):
        request = DummyRequest([('status', 'No value')])
        result = {'filters': []}
        field_filters = {}
        LuceneBuilder.handle_range_filters(request, result, field_filters, ['Item'])
        assert field_filters['embedded.status.raw']['add_no_value'] is True

    def test_type_equals_item_is_recorded_on_at_type_raw(self):
        # type=Item is the one 'type' value NOT skipped by this function (see
        # `elif field == 'type' and term != 'Item': continue` below) - it gets
        # recorded as an ordinary field filter on embedded.@type.raw.
        request = DummyRequest([('type', 'Item')])
        result = {'filters': []}
        field_filters = {}
        LuceneBuilder.handle_range_filters(request, result, field_filters, ['Item'])
        assert field_filters['embedded.@type.raw']['must_terms'] == ['Item']
        assert result['filters'][0]['field'] == 'type'

    def test_type_equals_other_value_is_skipped_entirely(self):
        # A specific, non-'Item' type filter (e.g. type=File) is skipped by this
        # function entirely - actual type filtering is already applied via
        # doc_types in initialize_field_filters, so handle_range_filters doesn't
        # duplicate it (and doesn't add a removable UI filter entry for it).
        request = DummyRequest([('type', 'File')])
        result = {'filters': []}
        field_filters = {}
        LuceneBuilder.handle_range_filters(request, result, field_filters, ['Item'])
        assert field_filters == {}
        assert result['filters'] == []
