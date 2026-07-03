"""
Unit tests for snovault.predicates -- the pyramid view predicates used in route
configuration. Correctness matters (they decide whether a view matches a request)
and the module had no direct test. Lightweight fakes stand in for context/request.
"""
from types import SimpleNamespace

import pytest

from ..predicates import SubpathSegmentsPredicate, AdditionalPermissionPredicate


pytestmark = [pytest.mark.unit]


class TestSubpathSegmentsPredicate:

    def test_int_val_is_normalized_to_singleton(self):
        pred = SubpathSegmentsPredicate(2, config=None)
        assert pred.val == frozenset({2})

    def test_iterable_val_is_stored_as_frozenset(self):
        pred = SubpathSegmentsPredicate([0, 1, 2], config=None)
        assert pred.val == frozenset({0, 1, 2})

    def test_text_is_sorted_and_readable(self):
        pred = SubpathSegmentsPredicate([2, 0, 1], config=None)
        assert pred.text() == 'subpath_segments in [0, 1, 2]'

    def test_phash_matches_text(self):
        pred = SubpathSegmentsPredicate(1, config=None)
        assert pred.phash() == pred.text()

    @pytest.mark.parametrize('subpath,expected', [
        ((), False),
        (('a',), True),
        (('a', 'b'), False),
    ])
    def test_call_matches_on_subpath_length(self, subpath, expected):
        pred = SubpathSegmentsPredicate(1, config=None)
        request = SimpleNamespace(subpath=subpath)
        assert pred(context=None, request=request) is expected

    def test_call_matches_any_allowed_length(self):
        pred = SubpathSegmentsPredicate([0, 2], config=None)
        assert pred(None, SimpleNamespace(subpath=())) is True
        assert pred(None, SimpleNamespace(subpath=('a',))) is False
        assert pred(None, SimpleNamespace(subpath=('a', 'b'))) is True


class TestAdditionalPermissionPredicate:

    def test_text_and_phash(self):
        pred = AdditionalPermissionPredicate('edit', config=None)
        assert pred.text() == "additional_permission = 'edit'"
        assert pred.phash() == pred.text()

    def test_call_delegates_to_has_permission(self):
        calls = []

        def has_permission(perm, ctx):
            calls.append((perm, ctx))
            return True

        pred = AdditionalPermissionPredicate('edit', config=None)
        context = object()
        request = SimpleNamespace(has_permission=has_permission)
        assert pred(context, request) is True
        assert calls == [('edit', context)]

    def test_call_returns_falsey_permission_result(self):
        pred = AdditionalPermissionPredicate('view', config=None)
        request = SimpleNamespace(has_permission=lambda perm, ctx: False)
        assert pred(object(), request) is False
