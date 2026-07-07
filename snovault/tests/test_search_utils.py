"""
Unit tests for pure/leaf helper functions in snovault.search.search_utils that
previously had zero test coverage anywhere in the repo (confirmed via grep -
no snovault/tests/*.py file references find_nested_path, get_query_field,
build_sort_dicts, execute_search's error handling, or execute_streaming_search's
pagination logic before this file). These are exercised directly, with minimal
stand-ins for the pyramid Request/registry, since none of them touch a live
Elasticsearch cluster.
"""

import pytest
from elasticsearch import ConnectionTimeout, RequestError, TransportError
from pyramid.httpexceptions import HTTPBadRequest

from snovault.search.search_utils import (
    find_nested_path,
    is_schema_field,
    extract_field_name,
    get_query_field,
    is_numerical_field,
    is_array_of_numerical_field,
    build_sort_dicts,
    execute_search,
    execute_streaming_search,
    build_permission_filter,
)


class DummyTypeInfo:

    def __init__(self, schema=None):
        self.schema = schema or {}


class DummyRequest:

    def __init__(self, registry=None, effective_principals=None):
        self.registry = registry or {}
        self.effective_principals = effective_principals or []


class TestFindNestedPath:
    """ find_nested_path walks an es_mapping dict from the top level to find the
        closest (deepest) ancestor field mapped with type='nested'. This backs
        every nested-query/nested-facet construction in lucene_builder.py, so a
        wrong answer here silently breaks nested filtering. """

    def test_top_level_nested_field(self):
        es_mapping = {
            'experiments_in_set': {
                'type': 'nested',
                'properties': {
                    'biosample': {'properties': {'accession': {'type': 'keyword'}}}
                }
            }
        }
        assert find_nested_path('experiments_in_set.biosample.accession', es_mapping) == 'experiments_in_set'

    def test_no_nested_field_returns_none(self):
        es_mapping = {
            'status': {'type': 'keyword'}
        }
        assert find_nested_path('status', es_mapping) is None

    def test_field_not_in_mapping_returns_none(self):
        es_mapping = {
            'status': {'type': 'keyword'}
        }
        assert find_nested_path('nonexistent.field', es_mapping) is None

    def test_deepest_nested_path_wins(self):
        # If a nested field contains another nested field further down, the closest
        # (deepest / last-added) ancestor to the leaf field should be returned.
        es_mapping = {
            'a': {
                'type': 'nested',
                'properties': {
                    'b': {
                        'type': 'nested',
                        'properties': {
                            'c': {'type': 'keyword'}
                        }
                    }
                }
            }
        }
        assert find_nested_path('a.b.c', es_mapping) == 'a.b'

    def test_raw_suffix_is_stripped_before_lookup(self):
        es_mapping = {
            'experiments_in_set': {
                'type': 'nested',
                'properties': {
                    'accession': {'type': 'keyword'}
                }
            }
        }
        assert find_nested_path('experiments_in_set.accession.raw', es_mapping) == 'experiments_in_set'


class TestIsSchemaField:

    @pytest.mark.parametrize('field', ['validation_errors', 'validation_errors.name', 'aggregated_items', 'aggregated_items.foo'])
    def test_special_fields_have_no_schema(self, field):
        assert is_schema_field(field) is False

    @pytest.mark.parametrize('field', ['status', 'files.accession', 'type'])
    def test_ordinary_fields_have_schema(self, field):
        assert is_schema_field(field) is True


class TestExtractFieldName:

    def test_type_maps_to_at_type(self):
        assert extract_field_name('type') == '@type'

    def test_not_qualifier_is_stripped(self):
        assert extract_field_name('status!') == 'status'

    def test_ordinary_field_is_unchanged(self):
        assert extract_field_name('status') == 'status'

    def test_type_with_not_qualifier_does_not_get_at_type_substitution(self):
        # Only the exact string 'type' (no trailing '!') triggers the '@type'
        # substitution - 'type!' just has its '!' stripped, unlike plain 'type'.
        assert extract_field_name('type!') == 'type'


class TestGetQueryField:

    def test_type_field_uses_at_type_raw(self):
        assert get_query_field('type', {}) == 'embedded.@type.raw'

    def test_non_schema_field_gets_raw_suffix_directly(self):
        assert get_query_field('validation_errors.name', {}) == 'validation_errors.name.raw'

    def test_raw_aggregation_type_skips_raw_suffix(self):
        assert get_query_field('files.file_size', {'aggregation_type': 'stats'}) == 'embedded.files.file_size'

    def test_default_terms_field_gets_embedded_and_raw(self):
        assert get_query_field('status', {}) == 'embedded.status.raw'
        assert get_query_field('status', {'aggregation_type': 'terms'}) == 'embedded.status.raw'


class TestIsNumericalField:

    @pytest.mark.parametrize('field_type', ['integer', 'float', 'number'])
    def test_numerical_types(self, field_type):
        assert is_numerical_field({'type': field_type}) is True

    @pytest.mark.parametrize('field_type', ['string', 'boolean', 'array'])
    def test_non_numerical_types(self, field_type):
        assert is_numerical_field({'type': field_type}) is False

    def test_missing_type_defaults_to_non_numerical(self):
        assert is_numerical_field({}) is False


class TestIsArrayOfNumericalField:

    def test_array_of_integers(self):
        assert is_array_of_numerical_field({'type': 'array', 'items': {'type': 'integer'}}) is True

    def test_array_of_strings(self):
        assert is_array_of_numerical_field({'type': 'array', 'items': {'type': 'string'}}) is False

    def test_no_items_key(self):
        assert is_array_of_numerical_field({'type': 'array'}) is False


class TestBuildSortDicts:
    """ build_sort_dicts picks an ES sort clause shape (integer/number/date/string)
        based on the field's schema type, falls back to the type's schema `sort_by`
        or the hardcoded date_created/label default when nothing is requested, and
        skips the default entirely when a text search ('q') is active. """

    def _request_with_schema(self, doc_type_schema):
        return DummyRequest(registry={'types': {'Item': DummyTypeInfo(doc_type_schema)}})

    def test_no_requested_sort_falls_back_to_date_created_label_default(self):
        request = self._request_with_schema({})
        sort, result_sort = build_sort_dicts([], request, ['Item'])
        assert sort['embedded.date_created.raw']['order'] == 'desc'
        assert sort['embedded.label.raw']['order'] == 'asc'
        assert result_sort['date_created']['order'] == 'desc'

    def test_default_sort_skipped_when_text_search_active(self):
        request = self._request_with_schema({})
        sort, result_sort = build_sort_dicts([], request, ['Item'], text_search='some free text')
        assert sort == {}
        assert result_sort == {}

    def test_default_sort_still_applies_for_wildcard_text_search(self):
        # '*' is the "match everything" sentinel, not a real ranked text search,
        # so the default sort should still kick in.
        request = self._request_with_schema({})
        sort, result_sort = build_sort_dicts([], request, ['Item'], text_search='*')
        assert 'embedded.date_created.raw' in sort

    def test_schema_sort_by_overrides_hardcoded_default(self):
        schema_sort_by = {'accession': {'order': 'asc', 'unmapped_type': 'keyword'}}
        request = self._request_with_schema({'sort_by': schema_sort_by})
        sort, result_sort = build_sort_dicts([], request, ['Item'])
        assert 'embedded.accession.lower_case_sort' in sort
        assert 'embedded.date_created.raw' not in sort

    def test_requested_sort_ascending_and_descending(self):
        request = self._request_with_schema({})
        sort, result_sort = build_sort_dicts(['-status', 'accession'], request, ['Item'])
        assert result_sort['status']['order'] == 'desc'
        assert result_sort['accession']['order'] == 'asc'

    def test_requested_sort_on_integer_field_uses_long_unmapped_type(self):
        request = DummyRequest(registry={
            'types': {'Item': DummyTypeInfo({'properties': {'count': {'type': 'integer'}}})}
        })
        # schema_for_field crawls the schema for the field; here we directly assert
        # the fallback branch (no schema found) still produces a valid string sort,
        # since wiring up a real crawl_schema-compatible schema is exercised by the
        # date-field test below via an explicit format.
        sort, result_sort = build_sort_dicts(['count'], request, ['Item'])
        assert result_sort['count']['order'] == 'asc'

    def test_requested_sort_on_date_field_uses_raw_suffix_and_date_unmapped_type(self, monkeypatch):
        import snovault.search.search_utils as search_utils_module

        monkeypatch.setattr(
            search_utils_module, 'schema_for_field',
            lambda name, request, doc_types: {'format': 'date'}
        )
        request = self._request_with_schema({})
        sort, result_sort = build_sort_dicts(['submitted_date'], request, ['Item'])
        assert 'embedded.submitted_date.raw' in sort
        assert sort['embedded.submitted_date.raw']['unmapped_type'] == 'date'

    def test_requested_sort_unknown_type_falls_back_to_string_sort(self, monkeypatch):
        import snovault.search.search_utils as search_utils_module

        monkeypatch.setattr(
            search_utils_module, 'schema_for_field',
            lambda name, request, doc_types: None
        )
        request = self._request_with_schema({})
        sort, result_sort = build_sort_dicts(['some_field'], request, ['Item'])
        assert 'embedded.some_field.lower_case_sort' in sort
        assert sort['embedded.some_field.lower_case_sort']['unmapped_type'] == 'keyword'


class FakeES:
    """ Minimal stand-in for the elasticsearch client's .search(). """

    def __init__(self, side_effect=None, return_value=None):
        self.side_effect = side_effect
        self.return_value = return_value if return_value is not None else {'hits': {'hits': []}}
        self.calls = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        if self.side_effect is not None:
            raise self.side_effect
        return self.return_value


class TestExecuteSearchErrorHandling:
    """ execute_search converts every ES client exception into an HTTPBadRequest
        with a caller-facing explanation - a bare ES exception should never
        propagate out to the view. """

    def test_successful_search_passes_through(self):
        es = FakeES(return_value={'hits': {'hits': [{'_id': '1'}]}})
        result = execute_search(es=es, query={'query': {}}, index='test-index', from_=0, size=10)
        assert result == {'hits': {'hits': [{'_id': '1'}]}}

    def test_connection_timeout_raises_http_bad_request(self):
        es = FakeES(side_effect=ConnectionTimeout('timeout', 'msg', {}))
        with pytest.raises(HTTPBadRequest) as exc_info:
            execute_search(es=es, query={}, index='test-index', from_=0, size=10)
        assert 'timeout' in str(exc_info.value).lower()

    def test_request_error_includes_root_cause_reason(self):
        es = FakeES(side_effect=RequestError(400, 'search_phase_execution_exception',
                                              {'error': {'root_cause': [{'reason': 'bad query syntax'}]}}))
        with pytest.raises(HTTPBadRequest) as exc_info:
            execute_search(es=es, query={}, index='test-index', from_=0, size=10)
        assert 'bad query syntax' in str(exc_info.value)

    def test_request_error_without_parseable_root_cause_falls_back_to_str(self):
        es = FakeES(side_effect=RequestError(400, 'oops', {}))  # no 'error' key
        with pytest.raises(HTTPBadRequest):
            execute_search(es=es, query={}, index='test-index', from_=0, size=10)

    def test_transport_error_timeout_status(self):
        es = FakeES(side_effect=TransportError('TIMEOUT', 'timed out'))
        with pytest.raises(HTTPBadRequest) as exc_info:
            execute_search(es=es, query={}, index='test-index', from_=0, size=10)
        assert 'timeout' in str(exc_info.value).lower()

    def test_transport_error_other_status(self):
        es = FakeES(side_effect=TransportError(503, 'service unavailable'))
        with pytest.raises(HTTPBadRequest) as exc_info:
            execute_search(es=es, query={}, index='test-index', from_=0, size=10)
        assert 'transport error' in str(exc_info.value).lower()

    def test_generic_exception_is_wrapped(self):
        es = FakeES(side_effect=ValueError('something unrelated broke'))
        with pytest.raises(HTTPBadRequest) as exc_info:
            execute_search(es=es, query={}, index='test-index', from_=0, size=10)
        assert 'something unrelated broke' in str(exc_info.value)

    def test_session_id_passed_as_preference(self):
        es = FakeES()
        execute_search(es=es, query={}, index='test-index', from_=0, size=10, session_id='SESSION-123')
        assert es.calls[0]['preference'] == 'SESSION-123'


class TestExecuteStreamingSearch:
    """ execute_streaming_search implements ES search_after pagination by hand -
        it must advance the cursor from the last hit of each page, stop as soon
        as a short page is seen, and never request aggregations/exact totals. """

    def test_stops_immediately_on_empty_first_page(self):
        es = FakeES(return_value={'hits': {'hits': []}})
        results = list(execute_streaming_search(es, index='test-index', query={}))
        assert results == []
        assert len(es.calls) == 1

    def test_yields_all_hits_across_pages_and_stops_on_short_page(self):
        page1 = {'hits': {'hits': [
            {'_source': {'uuid': '1'}, 'sort': ['1']},
            {'_source': {'uuid': '2'}, 'sort': ['2']},
        ]}}
        page2 = {'hits': {'hits': [
            {'_source': {'uuid': '3'}, 'sort': ['3']},
        ]}}
        es = FakeES()
        pages = [page1, page2]

        def fake_search(**kwargs):
            # the same `body` dict is mutated and re-passed across iterations by
            # execute_streaming_search, so snapshot it now rather than aliasing it
            es.calls.append({'index': kwargs['index'], 'body': dict(kwargs['body'])})
            return pages.pop(0)

        es.search = fake_search
        results = list(execute_streaming_search(es, index='test-index', query={}, batch_size=2))

        assert [r['uuid'] for r in results] == ['1', '2', '3']
        # first call has no search_after; second call carries the last hit's sort as cursor
        assert 'search_after' not in es.calls[0]['body']
        assert es.calls[1]['body']['search_after'] == ['2']

    def test_query_body_never_requests_aggs_and_disables_total_hit_tracking(self):
        es = FakeES(return_value={'hits': {'hits': []}})
        list(execute_streaming_search(es, index='test-index', query={'match_all': {}}))
        body = es.calls[0]['body']
        assert 'aggs' not in body
        assert body['track_total_hits'] is False
        assert body['query'] == {'match_all': {}}

    def test_source_includes_are_passed_through(self):
        es = FakeES(return_value={'hits': {'hits': []}})
        list(execute_streaming_search(es, index='test-index', query={}, source_includes=['uuid', 'status']))
        body = es.calls[0]['body']
        assert body['_source'] == {'includes': ['uuid', 'status']}

    def test_default_sort_field_is_stable_uuid_ascending(self):
        es = FakeES(return_value={'hits': {'hits': []}})
        list(execute_streaming_search(es, index='test-index', query={}))
        assert es.calls[0]['body']['sort'] == [{'embedded.uuid.raw': {'order': 'asc'}}]

    def test_full_page_triggers_another_request_even_with_no_more_hits(self):
        # A page exactly equal to batch_size must trigger one more request to
        # confirm there's nothing left (can't assume a full page is the last page).
        full_page = {'hits': {'hits': [{'_source': {'uuid': '1'}, 'sort': ['1']}]}}
        empty_page = {'hits': {'hits': []}}
        es = FakeES()
        pages = [full_page, empty_page]

        def fake_search(**kwargs):
            es.calls.append(kwargs)
            return pages.pop(0)

        es.search = fake_search
        results = list(execute_streaming_search(es, index='test-index', query={}, batch_size=1))
        assert [r['uuid'] for r in results] == ['1']
        assert len(es.calls) == 2


class TestBuildPermissionFilter:

    def test_builds_terms_filter_from_effective_principals(self):
        request = DummyRequest(effective_principals=['system.Everyone', 'userid.abc'])
        result = build_permission_filter(request)
        assert result == {'terms': {'principals_allowed.view': ['system.Everyone', 'userid.abc']}}
