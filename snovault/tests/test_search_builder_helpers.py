"""
Unit tests for additional SearchBuilder helper methods with no prior test
coverage: pagination parsing, initial-columns construction, extra-aggregation
extraction, group-by-facet term grouping, and per-hit frame selection. Built
the same way as test_search_efficiency.py - minimally-populated SearchBuilder
instances via object.__new__, no ES/pyramid app required.
"""

from snovault.search.search import SearchBuilder


class DummyParams:

    def __init__(self, params):
        self._params = params

    def getall(self, key):
        return self._params.get(key, [])

    def get(self, key, default=None):
        values = self._params.get(key)
        if values:
            return values[0]
        return default


class DummyRequest:

    def __init__(self, params):
        self.normalized_params = DummyParams(params)


def make_builder(params=None):
    builder = object.__new__(SearchBuilder)
    builder.request = DummyRequest(params or {})
    return builder


class TestSetPagination:

    def test_defaults_when_no_params_given(self):
        builder = make_builder({})
        builder.set_pagination()
        assert builder.from_ == 0
        assert builder.size == SearchBuilder.PAGINATION_SIZE

    def test_limit_all_sets_size_to_all_string(self):
        builder = make_builder({'limit': ['all']})
        builder.set_pagination()
        assert builder.size == 'all'

    def test_limit_empty_string_sets_size_to_all(self):
        builder = make_builder({'limit': ['']})
        builder.set_pagination()
        assert builder.size == 'all'

    def test_numeric_limit_and_from_are_parsed_as_ints(self):
        builder = make_builder({'limit': ['50'], 'from': ['20']})
        builder.set_pagination()
        assert builder.size == 50
        assert builder.from_ == 20

    def test_invalid_limit_falls_back_to_default_pagination_size(self):
        builder = make_builder({'limit': ['not-a-number']})
        builder.set_pagination()
        assert builder.size == SearchBuilder.PAGINATION_SIZE

    def test_invalid_from_falls_back_to_zero(self):
        builder = make_builder({'limit': ['25'], 'from': ['not-a-number']})
        builder.set_pagination()
        assert builder.from_ == 0
        assert builder.size == 25


class TestBuildInitialColumns:

    def test_always_includes_title_column_first(self):
        columns = SearchBuilder.build_initial_columns([{}])
        assert next(iter(columns)) == 'display_title'
        assert columns['display_title']['order'] == -1000

    def test_status_and_date_created_added_when_absent_from_schema(self):
        columns = SearchBuilder.build_initial_columns([{}])
        assert 'status' in columns
        assert 'date_created' in columns

    def test_schema_columns_are_merged_in(self):
        schema = {'columns': {'accession': {'title': 'Accession'}}}
        columns = SearchBuilder.build_initial_columns([schema])
        assert columns['accession'] == {'title': 'Accession'}

    def test_schema_can_override_default_status_column(self):
        schema = {'columns': {'status': {'title': 'Custom Status'}}}
        columns = SearchBuilder.build_initial_columns([schema])
        # schema-provided status overrides/updates the default, doesn't duplicate
        assert columns['status']['title'] == 'Custom Status'

    def test_multiple_schemas_merge_without_clobbering_earlier_ones(self):
        schema1 = {'columns': {'accession': {'title': 'Accession'}}}
        schema2 = {'columns': {'lab': {'title': 'Lab'}}}
        columns = SearchBuilder.build_initial_columns([schema1, schema2])
        assert 'accession' in columns
        assert 'lab' in columns


class TestFormatExtraAggregations:

    def test_no_aggregations_key_returns_empty_dict(self):
        assert SearchBuilder.format_extra_aggregations({}) == {}

    def test_all_items_key_is_excluded(self):
        es_results = {'aggregations': {'all_items': {'foo': 1}, 'my_stat': {'value': 42}}}
        assert SearchBuilder.format_extra_aggregations(es_results) == {'my_stat': {'value': 42}}

    def test_empty_aggregations_dict(self):
        assert SearchBuilder.format_extra_aggregations({'aggregations': {}}) == {}


class TestGroupFacetTerms:

    def test_groups_terms_by_subaggregation_and_sums_doc_counts(self):
        result_facet = {
            'field': 'assay_type',
            'group_by_field': 'category',
            'terms': [
                {'key': 'RNA-seq', 'doc_count': 5},
                {'key': 'ChIP-seq', 'doc_count': 3},
            ],
        }
        agg = {
            'primary_agg': {
                'buckets': [
                    {'key': 'sequencing', 'sub_terms': {'buckets': [
                        {'key': 'RNA-seq'}, {'key': 'ChIP-seq'},
                    ]}},
                ]
            }
        }
        SearchBuilder.group_facet_terms(result_facet, agg, filters=[])

        assert result_facet['has_group_by'] is True
        assert 'group_by_field' not in result_facet
        assert len(result_facet['terms']) == 1
        group = result_facet['terms'][0]
        assert group['key'] == 'sequencing'
        assert group['doc_count'] == 8  # 5 + 3
        assert {t['key'] for t in group['terms']} == {'RNA-seq', 'ChIP-seq'}

    def test_terms_with_no_matching_subaggregation_group_under_missing_group(self):
        result_facet = {
            'field': 'assay_type',
            'group_by_field': 'category',
            'terms': [{'key': 'Unknown Assay', 'doc_count': 1}],
        }
        agg = {'primary_agg': {'buckets': []}}
        SearchBuilder.group_facet_terms(result_facet, agg, filters=[])
        assert result_facet['terms'][0]['key'] == '(Missing group)'

    def test_filter_term_not_in_results_is_added_with_zero_doc_count(self):
        # A term the user has actively filtered on but which returned zero
        # matching documents (and thus isn't in the aggregation buckets) must
        # still show up in the grouped output, per the inline comment about
        # "exists in filters".
        result_facet = {
            'field': 'assay_type',
            'group_by_field': 'category',
            'terms': [],
        }
        agg = {
            'primary_agg': {
                'buckets': [
                    {'key': 'sequencing', 'sub_terms': {'buckets': [{'key': 'RNA-seq'}]}},
                ]
            }
        }
        filters = [{'field': 'assay_type', 'term': 'RNA-seq'}]
        SearchBuilder.group_facet_terms(result_facet, agg, filters=filters)
        assert len(result_facet['terms']) == 1
        assert result_facet['terms'][0]['terms'] == [{'key': 'RNA-seq', 'doc_count': 0}]

    def test_filter_on_a_different_field_is_ignored(self):
        result_facet = {
            'field': 'assay_type',
            'group_by_field': 'category',
            'terms': [],
        }
        agg = {'primary_agg': {'buckets': []}}
        filters = [{'field': 'status', 'term': 'released'}]
        SearchBuilder.group_facet_terms(result_facet, agg, filters=filters)
        assert result_facet['terms'] == []

    def test_none_result_facet_or_agg_is_a_noop(self):
        # should not raise
        SearchBuilder.group_facet_terms(None, {'primary_agg': {'buckets': []}}, [])
        SearchBuilder.group_facet_terms({'terms': []}, None, [])


class TestFormatResults:
    """ _format_results picks which _source frame to yield per hit, and merges
        in validation_errors/aggregated_items if the frame itself doesn't
        already carry them. """

    def _builder(self, search_frame, fields_requested=None):
        builder = object.__new__(SearchBuilder)
        builder.request = DummyRequest({'field': fields_requested} if fields_requested else {})
        builder.search_frame = search_frame
        return builder

    def test_embedded_frame_yields_embedded_source(self):
        builder = self._builder('embedded')
        hits = [{'_source': {'embedded': {'uuid': '1'}}}]
        assert list(builder._format_results(hits)) == [{'uuid': '1'}]

    def test_raw_frame_reads_from_properties_key(self):
        builder = self._builder('raw')
        hits = [{'_source': {'properties': {'uuid': '1'}}}]
        assert list(builder._format_results(hits)) == [{'uuid': '1'}]

    def test_object_frame_reads_from_object_key(self):
        builder = self._builder('object')
        hits = [{'_source': {'object': {'uuid': '1'}}}]
        assert list(builder._format_results(hits)) == [{'uuid': '1'}]

    def test_explicit_field_param_forces_embedded_frame_regardless_of_search_frame(self):
        builder = self._builder('object', fields_requested=['status'])
        hits = [{'_source': {'embedded': {'uuid': '1'}}, }]
        assert list(builder._format_results(hits)) == [{'uuid': '1'}]

    def test_validation_errors_merged_in_when_absent_from_frame(self):
        builder = self._builder('embedded')
        hits = [{'_source': {
            'embedded': {'uuid': '1'},
            'validation_errors': [{'error': 'oops'}],
        }}]
        result = list(builder._format_results(hits))[0]
        assert result['validation_errors'] == [{'error': 'oops'}]

    def test_validation_errors_not_overwritten_when_already_in_frame(self):
        builder = self._builder('embedded')
        hits = [{'_source': {
            'embedded': {'uuid': '1', 'validation_errors': ['frame-specific']},
            'validation_errors': ['top-level'],
        }}]
        result = list(builder._format_results(hits))[0]
        assert result['validation_errors'] == ['frame-specific']

    def test_aggregated_items_merged_in_when_absent_from_frame(self):
        builder = self._builder('embedded')
        hits = [{'_source': {
            'embedded': {'uuid': '1'},
            'aggregated_items': {'foo': ['bar']},
        }}]
        result = list(builder._format_results(hits))[0]
        assert result['aggregated_items'] == {'foo': ['bar']}
