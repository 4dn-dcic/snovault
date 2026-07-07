"""
Unit tests for snovault.cache.ManagerLRUCache -- the per-transaction LRU cache
layered on pyramid's threadlocal stack (used for embed/collection caches on hot
read paths). Locks the degraded no-op behavior outside a request, capacity
resolution from registry settings, and the transaction-completion flush.
No services required.
"""
import pytest

from pyramid.threadlocal import manager

from ..cache import ManagerLRUCache


pytestmark = [pytest.mark.unit]


class FakeRegistry:

    def __init__(self, settings=None):
        self.settings = settings or {}


@pytest.fixture
def threadlocal_registry():
    """ Push a fake pyramid threadlocal frame so the cache has somewhere to live. """
    registry = FakeRegistry()
    manager.push({'registry': registry})
    yield registry
    manager.pop()


class TestOutsideRequest:
    """ With no pyramid threadlocal stack, every operation degrades to a no-op. """

    def test_cache_is_none(self):
        assert ManagerLRUCache('t.none').cache is None

    def test_get_returns_default_and_set_is_noop(self):
        cache = ManagerLRUCache('t.noop')
        cache['key'] = 'value'
        assert cache.get('key', 'default') == 'default'
        assert 'key' not in cache


class TestWithinRequest:

    def test_set_get_contains_delete(self, threadlocal_registry):
        cache = ManagerLRUCache('t.basic')
        cache['key'] = 'value'
        assert cache.get('key') == 'value'
        assert 'key' in cache
        del cache['key']
        assert cache.get('key', 'missing') == 'missing'

    def test_capacity_defaults_when_unconfigured(self, threadlocal_registry):
        cache = ManagerLRUCache('t.sized', default_capacity=2)
        assert cache.cache.capacity == 2

    def test_capacity_read_from_registry_settings(self, threadlocal_registry):
        threadlocal_registry.settings['t.configured.capacity'] = '7'
        cache = ManagerLRUCache('t.configured', default_capacity=100)
        assert cache.cache.capacity == 7

    def test_after_completion_flushes_cache(self, threadlocal_registry):
        # The ISynchronizer hook must drop the cache when a transaction
        # completes, so retried transactions never see stale entries.
        cache = ManagerLRUCache('t.flush')
        cache['key'] = 'value'
        cache.afterCompletion(transaction=None)
        assert cache.get('key', 'flushed') == 'flushed'
