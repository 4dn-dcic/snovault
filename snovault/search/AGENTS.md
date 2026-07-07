# snovault/search/ — agent notes

Reference for anyone (agent or human) working in this subsystem. Not a changelog — see
"Historical detail" below for that.

## Module map

- **`search.py`** — `SearchBuilder`, the `/search` view. Owns request bootstrapping
  (`_bootstrap_query`), `_source` field selection (`list_source_fields`), facet
  initialization (`initialize_facets`), pagination (`set_pagination`,
  `get_all_subsequent_results`/`execute_search_for_all_results` for `limit=all`), and
  response formatting (`format_results`, `format_facets`, `_format_results`).
  Delegates actual filter/aggregation DSL construction to `LuceneBuilder`.
- **`lucene_builder.py`** — `LuceneBuilder`, a stateless collection of static/class
  methods that turn snovault's intermediate filter/facet representation into real
  Elasticsearch query DSL. Entry points: `build_filters` (the query itself),
  `build_facets` (aggregations), `verify_search_has_permissions` (defense-in-depth
  check that permission filters weren't stripped mid-construction).
- **`search_utils.py`** — shared helpers used by both of the above: `execute_search`
  (ES call + exception→`HTTPBadRequest` translation), `execute_streaming_search`
  (search_after-based streaming, bypasses `SearchBuilder` entirely),
  `schema_for_field` (schema lookup + per-request cache), `build_sort_dicts`,
  `find_nested_path`, and shared constants like `COMMON_EXCLUDED_URI_PARAMS`.
- **`compound_search.py`** — `CompoundSearchBuilder`, the `/compound_search` and
  `/build_query` views (FilterSet-driven multi-block search — AND/OR combinations of
  independent query blocks, e.g. for saved filter sets). Builds each block's lucene
  query via a Pyramid subrequest to `/build_query`, then combines them with
  `LuceneBuilder.compound_search` and executes via `SearchBuilder.from_search`
  (which skips normal bootstrapping — see gotchas).

Call flow for an ordinary `/search`: `search.py` view → `SearchBuilder.build_search_query()`
→ `LuceneBuilder.build_filters`/`build_facets` → `search_utils.execute_search` → ES →
`SearchBuilder.format_results`/`format_facets`.

## Key concepts

- **Frames** (`embedded`/`object`/`raw`, URL param `frame=`) select which propsheet
  variant of a document to return. `_source` is scoped to match: `embedded.*` for the
  default frame, `object.*`/`properties.*` (raw→properties) for the others — fetching
  more than the requested frame is pure waste, since `_format_results` only ever reads
  one frame per hit.
- **Facets/aggregations** run in a `global` ES aggregation context
  (`{'all_items': {'global': {}, 'aggs': {...}}}`), meaning each facet's cost scales
  with the *whole index*, not the current page or filtered result set. This is why
  facet computation is the dominant cost of `/search` for richly-faceted types, and why
  it's worth being careful about when facets are actually computed vs. discarded.
  `format_facets` only returns non-empty results for `frame == 'embedded'` — every other
  frame pays for aggregations it then throws away unless explicitly gated.
- **`skip_default_facets`** (URL param) skips the default per-field facet loop entirely,
  falling back to only computing facets explicitly requested via `additional_facet=`.
  Added for callers (e.g. peek-metadata) that need one cheap stat/aggregation without
  paying for the full per-type facet fan-out. See gotchas — this was broken for a long
  time.
- **Compound search's per-block subrequest model**: each filter block in a FilterSet is
  turned into its own Pyramid subrequest to `/build_query`, which runs the full
  `SearchBuilder` query-construction pipeline just to extract `query['query']` — the
  `aggs` portion of that pipeline's output is discarded per block. `execute_filter_set`
  then combines all blocks' `query['query']` via `LuceneBuilder.compound_search` (OR/AND)
  and runs the combined query through a `SearchBuilder` built via `from_search`, which
  skips normal bootstrapping (including `_source` scoping — must be set explicitly).
- **`limit=all` pagination** walks the full result set via repeated `from_`/`size`
  requests (`execute_search_for_all_results` → `get_all_subsequent_results`), not a
  scroll/search_after cursor. This is O(N²) server-side cost for large result sets and
  is bounded by `SEARCH_MAX` (100_000, `snovault/util.py`) — beyond that ES raises
  `HTTPBadRequest` mid-stream. `execute_streaming_search` in `search_utils.py` (used by
  streaming/bulk-download endpoints, not by `SearchBuilder` itself) is the correct
  O(N) `search_after`-based primitive if you need to walk a large result set — prefer
  it over `limit=all` for new bulk-consumption code paths.

## Known sharp edges / gotchas

- **`skip_default_facets` was silently broken for a long time**: it was documented as a
  URL query param but never added to `COMMON_EXCLUDED_URI_PARAMS`
  (`search_utils.py`), so passing it as an actual query param got parsed as a field
  filter on a nonexistent field and silently matched zero documents. Now fixed — but the
  general lesson is that this exclusion list is easy to forget when adding any new
  control param; if you add one, add it here too, or it'll quietly behave like a broken
  filter instead of a no-op.
- **A subsequent-page query body is not automatically safe to reuse from page 1.**
  `limit=all`'s pagination loop used to resend the *exact same query dict* — including
  the full default-facet `aggs` block — on every page, even though each facet
  aggregation runs in the whole-index `global` context (so it costs the same on every
  page) and nothing downstream ever reads aggregations past the first page. Before
  copying a query dict for a follow-up request, check what that specific request
  actually needs — don't assume "same query, different `from_`" is free.
- **A cache that's only ever read with a truthy-looking default never actually caches
  anything.** `schema_for_field`'s per-request cache used
  `getattr(request, '_field_schema_cache', {})` — since `{}` is not `None`, the
  "initialize if unset" branch never ran, so the cache dict was always a throwaway
  local and the attribute was never persisted onto the request. If you add a
  per-request cache, verify with a test that the attribute actually survives across two
  calls, not just that the second call returns the right value (which it will, from
  the throwaway dict, even when nothing is actually being cached across calls).
- **Floating-point epsilon direction in boundary math is easy to get backwards.**
  `LuceneBuilder.canonicalize_bounds`/`range_includes_zero` nudge exclusive (`gt`/`lt`)
  range bounds by a tiny epsilon so an inclusive comparison can still distinguish them
  from inclusive (`gte`/`lte`) bounds — but only near a pivot of exactly zero (the
  epsilon is swallowed by float64 precision everywhere else). Getting the nudge
  direction wrong here is a real, subtle bug class: it doesn't show up in ordinary
  values, only at an exact zero boundary. Assert boundary behavior explicitly in tests
  (`gt: 0` must exclude zero, `gte: 0` must include it, etc.) — don't trust it "looks
  right" from reading the arithmetic.
- **Boolean dedup guards using `or` where `and` was meant** are an easy mistake and easy
  to miss in review: `CompoundSearchBuilder._add_type_to_flag_if_needed`'s "is this
  already present" check used `not in flags or lower_not in flags` (true almost always,
  since the second clause fires whenever `flags` isn't already all-lowercase),
  defeating the dedup it was meant to provide. When writing a "skip if already present"
  guard across multiple representations of the same thing (case variants, etc.), the
  guard needs `and` (absent in *every* representation), not `or`.

## Historical detail

Two efficiency audits and their fixes are the source of most of the above (query
`_source`/facet waste findings, the `skip_default_facets` and pagination-`aggs` bugs,
and the two latent boundary bugs). The full reports have file:line evidence, benchmarks,
and reasoning that didn't make it into this summary — ask your supervisor for the ES
query efficiency audit reports (there were two passes) if you need that depth.

## Test coverage map

`snovault/tests/` (no ES/live pyramid app required — these test pure query-construction
and caching logic via minimally-populated `SearchBuilder`/`LuceneBuilder` instances and
monkeypatched ES calls):

- `test_search_efficiency.py` — the `_source`/frame-gating fixes, `limit=all` `aggs`
  stripping, `skip_default_facets` exclusion, compound_search's `/build_query`
  `skip_default_facets` flag, `schema_for_field` caching, the removed stray `print()`.
- `test_search_utils.py` — `find_nested_path`, `build_sort_dicts`, `execute_search`'s
  exception handling, `execute_streaming_search`'s pagination, and other
  `search_utils.py` leaf functions.
- `test_lucene_builder.py` — `handle_range_filters`, `canonicalize_bounds`/
  `range_includes_zero`, `construct_nested_sub_queries`, and other `LuceneBuilder`
  leaf methods.
- `test_compound_search.py` — `CompoundSearchBuilder`'s pure string/validation helpers
  (`combine_query_strings`, `_add_type_to_flag_if_needed`, `validate_flag`/
  `validate_filter_block`, `extract_filter_set_from_search_body`).
- `test_search_builder_helpers.py` — `SearchBuilder` helpers not covered elsewhere:
  `set_pagination`, `build_initial_columns`, `format_extra_aggregations`,
  `group_facet_terms`, `_format_results`.

`compound_search.py`'s end-to-end behavior (actual ES execution via `execute_filter_set`)
has **no test coverage in this repo** — there's no ES fixture wired up at the unit-test
level here. It's presumably exercised downstream (smaht-portal/cgap-portal); if you
change `execute_filter_set` or anything it calls, double-check those repos' test suites.
