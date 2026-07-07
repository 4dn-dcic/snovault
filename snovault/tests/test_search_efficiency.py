"""
Unit tests for targeted efficiency fixes in the search query builder:

    1. compound_search's multi-block path restricting `_source` to `embedded.*`
    2. default per-field facets being skipped for non-'embedded' search frames
    3. `list_source_fields` no longer fetching unused `embedded.*` for
       frame=object/raw
    4. `limit=all` pagination dropping `aggs`/`track_total_hits` from
       subsequent-page queries
    5. `skip_default_facets` being excluded from field-filter parsing
       (COMMON_EXCLUDED_URI_PARAMS)
    6. compound_search's per-block `/build_query` subrequest passing
       `skip_default_facets=true`
    7. `schema_for_field`'s per-request memoization actually persisting onto
       the request
    8. removal of the stray `print(result_facet)` debug statement in
       `group_facet_terms`

These exercise the pure query-construction logic on `SearchBuilder` directly
(via a minimally-populated instance) rather than through a full pyramid app +
Elasticsearch stack, since none of these paths touch ES itself - only the
lucene query dict that would eventually be sent to it. There is no existing
ES-backed test coverage for `compound_search` in this repo (it is exercised
downstream), so its fix is validated the same way: asserting on the
constructed query dict, without invoking `execute_filter_set` end-to-end.
"""

from snovault.search.search import SearchBuilder
import snovault.search.search as search_module
import snovault.search.search_utils as search_utils_module
import snovault.search.compound_search as compound_search_module


class DummyParams:
    """ Minimal stand-in for webob's MultiDict, as used via request.normalized_params. """

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

    def __init__(self, params, registry=None):
        self.normalized_params = DummyParams(params)
        self.registry = registry or {}


class DummyTypeInfo:

    def __init__(self, schema=None):
        self.schema = schema or {}


def make_search_builder(frame='embedded', field=None, doc_types=('Item',),
                         additional_facets=None, item_type_es_mapping=None):
    """ Builds a SearchBuilder instance with only the attributes needed by
        list_source_fields()/initialize_facets(), bypassing __init__ (which
        requires a full pyramid registry with ES/storage handles).
    """
    params = {'frame': [frame]}
    if field:
        params['field'] = field
    builder = object.__new__(SearchBuilder)
    builder.doc_types = list(doc_types)
    builder.request = DummyRequest(params, registry={'types': {dt: DummyTypeInfo() for dt in doc_types}})
    builder.search_frame = frame
    builder.additional_facets = additional_facets or []
    builder.item_type_es_mapping = item_type_es_mapping or {}
    builder.prepared_terms = {}
    return builder


class TestListSourceFields:
    """ Fix #3: frame=object/raw should only fetch their own frame from _source,
        not also 'embedded.*' (which is never read for those frames). """

    def test_embedded_frame_unaffected(self):
        builder = make_search_builder(frame='embedded')
        assert builder.list_source_fields() == ['embedded.*']

    def test_object_frame_only_fetches_object(self):
        builder = make_search_builder(frame='object')
        assert builder.list_source_fields() == ['object.*']

    def test_raw_frame_only_fetches_properties(self):
        builder = make_search_builder(frame='raw')
        assert builder.list_source_fields() == ['properties.*']

    def test_explicit_field_param_still_scoped_to_embedded(self):
        builder = make_search_builder(frame='object', field=['status'])
        fields = builder.list_source_fields()
        assert fields == ['embedded.@id', 'embedded.@type', 'embedded.status']


class TestInitializeFacetsFrameGating:
    """ Fix #2: default per-field facets should not be computed for
        frame != 'embedded', since format_facets() discards them anyway
        (search.py's format_facets short-circuits for non-embedded frames).
        The schema-defined `additional_facet` (?additional_facet=) path must
        still work regardless of frame - that's a separate, explicit request
        for a specific aggregation, not a default. """

    def test_non_embedded_frame_skips_default_facets(self):
        builder = make_search_builder(frame='object')
        assert builder.initialize_facets() == []

    def test_raw_frame_skips_default_facets(self):
        builder = make_search_builder(frame='raw')
        assert builder.initialize_facets() == []

    def test_embedded_frame_still_computes_default_facets(self):
        builder = make_search_builder(frame='embedded')
        facets = builder.initialize_facets()
        # default 'type' facet is always present for embedded-frame searches
        assert any(field == 'type' for field, _ in facets)

    def test_non_embedded_frame_still_honors_additional_facet_param(self):
        # additional_facet is an explicit, non-default aggregation request and
        # must survive the non-embedded fast path (mirrors SKIP_DEFAULT_FACETS).
        builder = make_search_builder(frame='object', additional_facets=['status'])
        facets = builder.initialize_facets()
        assert any(field == 'status' for field, _ in facets)

    def test_skip_default_facets_param_still_works_for_embedded_frame(self):
        builder = make_search_builder(frame='embedded')
        builder.request.normalized_params._params['skip_default_facets'] = ['true']
        assert builder.initialize_facets() == []


class TestCompoundSearchSourceRestriction:
    """ Fix #1: compound_search's multi-block path must restrict _source to
        'embedded.*', matching what its only two consumers
        (format_result_for_endpoint_response, es_results_generator) read.

        Building a real SearchBuilder via `from_search` (as execute_filter_set
        does) requires a full pyramid registry (ES client handle, storage,
        types) to satisfy the unconditional setup in `SearchBuilder.__init__`
        even with skip_bootstrap=True - there's no existing snovault-level ES
        test fixture wired up for compound_search (see the audit report), so
        constructing that here would mean re-building a large slice of the
        pyramid app in a unit test. Instead this asserts directly against the
        source of execute_filter_set that the fix is present immediately
        after the from_search() call, which is the actual regression this
        guards against (someone editing that block and dropping the line).
        This should be supplemented by a real integration-level test
        downstream (smaht-portal/cgap-portal) where compound_search already
        has ES-backed test coverage.
    """

    def test_execute_filter_set_restricts_source_to_embedded_after_from_search(self):
        import inspect
        from snovault.search.compound_search import CompoundSearchBuilder

        source = inspect.getsource(CompoundSearchBuilder.execute_filter_set)
        from_search_idx = source.index('SearchBuilder.from_search(')
        source_fix_idx = source.index("search_builder_instance.query['_source'] = ['embedded.*']")
        assert source_fix_idx > from_search_idx, (
            "execute_filter_set must restrict _source to ['embedded.*'] on the "
            "SearchBuilder built via from_search(), since format_result_for_endpoint_response "
            "and es_results_generator only ever read hit['_source']['embedded']"
        )


class TestGetAllSubsequentResultsDropsAggs:
    """ Fix #4: `limit=all` pagination re-sent the full default-facet
        aggregation block (and total-hit tracking) on every page, even though
        each facet aggregation runs in a `global` (whole-index) context - so
        it costs the same on every page - and format_facets only ever reads
        aggregations from the *first* page's response. Subsequent-page
        queries should carry neither `aggs` nor `track_total_hits`, and the
        rest of the query body (used for the actual hit search) must be
        unaffected. """

    def test_subsequent_pages_drop_aggs_and_total_hit_tracking(self, monkeypatch):
        captured_queries = []

        def fake_execute_search(es, query, index, from_, size, session_id=None):
            captured_queries.append(query)
            return {'hits': {'hits': []}}

        monkeypatch.setattr(search_module, 'execute_search', fake_execute_search)

        builder = object.__new__(SearchBuilder)
        builder.es = None
        builder.es_index = 'test-index'
        builder.search_session_id = None
        builder.query = {
            'query': {'bool': {'must': ['sentinel']}},
            'sort': ['sentinel-sort'],
            'aggs': {'all_items': {'global': {}, 'aggs': {'type': {}}}},
        }

        list(builder.get_all_subsequent_results(extra_requests_needed_count=3, size_increment=100))

        assert len(captured_queries) == 3
        for query in captured_queries:
            assert 'aggs' not in query
            assert query['track_total_hits'] is False
            # the rest of the query body (what actually finds/sorts hits) is untouched
            assert query['query'] == {'bool': {'must': ['sentinel']}}
            assert query['sort'] == ['sentinel-sort']

        # the original query object (used for the first page) must not be mutated
        assert 'aggs' in builder.query

    def test_first_page_query_still_keeps_aggs(self, monkeypatch):
        # execute_search_for_all_results issues the first page directly with self.query,
        # which format_facets reads aggregations from - only subsequent pages should drop aggs.
        first_page_queries = []

        def fake_execute_search(es, query, index, from_, size, session_id=None):
            first_page_queries.append(query)
            return {'hits': {'hits': [], 'total': {'value': 0}}}

        monkeypatch.setattr(search_module, 'execute_search', fake_execute_search)

        builder = object.__new__(SearchBuilder)
        builder.es = None
        builder.es_index = 'test-index'
        builder.search_session_id = None
        builder.query = {'query': {'bool': {}}, 'aggs': {'all_items': {}}}

        builder.execute_search_for_all_results()

        assert len(first_page_queries) == 1
        assert 'aggs' in first_page_queries[0]


class TestSkipDefaultFacetsExcludedFromFieldFilters:
    """ Fix #5: `skip_default_facets` was documented as a URL query param but
        never added to COMMON_EXCLUDED_URI_PARAMS, so it was silently parsed
        as a field filter on a nonexistent field
        ('embedded.skip_default_facets'), matching zero documents. """

    def test_skip_default_facets_in_excluded_params(self):
        assert 'skip_default_facets' in search_utils_module.COMMON_EXCLUDED_URI_PARAMS

    def test_skip_default_facets_not_treated_as_field_filter(self):
        from webob.multidict import MultiDict

        builder = object.__new__(SearchBuilder)
        request = DummyRequest({})
        request.normalized_params = MultiDict([('type', 'Item'), ('skip_default_facets', 'true')])

        prepared_terms = builder.prepare_search_term(request)

        assert 'embedded.skip_default_facets' not in prepared_terms


class TestCompoundSearchBuildQuerySkipsDefaultFacets:
    """ Fix #6: once #5 lands, compound_search's per-block `/build_query`
        subrequest can pass `skip_default_facets=true` - `/build_query` only
        ever returns `query['query']` back to execute_filter_set, and default
        facet construction only ever writes `query['aggs']`, so this is free
        and does not change the extracted query. Only requests routed to
        BUILD_QUERY_URL should get the flag - regular `/search/` subrequests
        (used by the single-filter-block and flags-only paths) must be
        unaffected, since those responses' facets are actually used. """

    class FakeSubreq:
        def __init__(self, path):
            self.path = path
            self.headers = {}

    def _capture_subreq_path(self, monkeypatch):
        captured = {}

        def fake_make_search_subreq(request, path):
            captured['path'] = path
            return self.FakeSubreq(path)

        monkeypatch.setattr(compound_search_module, 'make_search_subreq', fake_make_search_subreq)
        return captured

    def test_build_query_route_gets_skip_default_facets_flag(self, monkeypatch):
        captured = self._capture_subreq_path(monkeypatch)

        compound_search_module.CompoundSearchBuilder.build_subreq_from_single_query(
            request=object(), query='?type=Item',
            route=compound_search_module.CompoundSearchBuilder.BUILD_QUERY_URL,
            from_=0, to=10
        )

        assert 'skip_default_facets=true' in captured['path']
        # original param must survive alongside the new flag
        assert 'type=Item' in captured['path']

    def test_search_route_unaffected(self, monkeypatch):
        captured = self._capture_subreq_path(monkeypatch)

        compound_search_module.CompoundSearchBuilder.build_subreq_from_single_query(
            request=object(), query='?type=Item', route='/search/', from_=0, to=10
        )

        assert 'skip_default_facets' not in captured['path']
        assert 'type=Item' in captured['path']


class TestSchemaForFieldCache:
    """ Fix #7: `schema_for_field`'s per-request memoization never actually
        persisted the cache dict onto the request (the `getattr(..., {})`
        default is never `None`, so the `if cache is None` initialization
        branch never ran) - every call re-crawled the schema. """

    def test_cache_persists_on_request_after_first_call(self, monkeypatch):
        calls = []

        def fake_crawl_schema(types, field, schema):
            calls.append(field)
            return {'type': 'string'}

        monkeypatch.setattr(search_utils_module, 'crawl_schema', fake_crawl_schema)

        request = DummyRequest({}, registry={'types': {'Item': DummyTypeInfo()}})
        assert not hasattr(request, '_field_schema_cache')

        first = search_utils_module.schema_for_field('status', request, ['Item'])
        assert hasattr(request, '_field_schema_cache')
        assert len(calls) == 1

        second = search_utils_module.schema_for_field('status', request, ['Item'])
        assert len(calls) == 1  # cache hit - no second crawl
        assert first == second == {'type': 'string'}


class TestGroupFacetTermsNoStrayPrint:
    """ Fix #8: a stray `print(result_facet)` in `group_facet_terms` dumped
        the full grouped facet structure to stdout on every response using a
        `group_by_field` facet. """

    def test_no_print_call_in_group_facet_terms_source(self):
        import inspect

        source = inspect.getsource(SearchBuilder.group_facet_terms)
        assert 'print(' not in source
