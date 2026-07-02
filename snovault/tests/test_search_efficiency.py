"""
Unit tests for targeted efficiency fixes in the search query builder:

    1. compound_search's multi-block path restricting `_source` to `embedded.*`
    2. default per-field facets being skipped for non-'embedded' search frames
    3. `list_source_fields` no longer fetching unused `embedded.*` for
       frame=object/raw

These exercise the pure query-construction logic on `SearchBuilder` directly
(via a minimally-populated instance) rather than through a full pyramid app +
Elasticsearch stack, since none of these paths touch ES itself - only the
lucene query dict that would eventually be sent to it. There is no existing
ES-backed test coverage for `compound_search` in this repo (it is exercised
downstream), so its fix is validated the same way: asserting on the
constructed query dict, without invoking `execute_filter_set` end-to-end.
"""

import pytest

from snovault.search.search import SearchBuilder


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
