"""
Unit tests for snovault.etag -- the view decorators that attach ETags and drive
conditional-GET (HTTP 304) handling. This is caching-correctness code and had no
direct test. Lightweight fakes stand in for the pyramid request/response.
"""
from types import SimpleNamespace

import pytest
from pyramid.httpexceptions import HTTPNotModified

from ..etag import etag_app_version, etag_app_version_effective_principals


pytestmark = [pytest.mark.unit]


APP_VERSION = 'v1.2.3'


class FakeCacheControl:
    def __init__(self):
        self.private = None
        self.max_age = None
        self.must_revalidate = None


def make_request(if_none_match=(), effective_principals=None):
    response = SimpleNamespace(etag=None, cache_control=FakeCacheControl())
    registry = SimpleNamespace(settings={'snovault.app_version': APP_VERSION})
    return SimpleNamespace(
        registry=registry,
        if_none_match=if_none_match,
        effective_principals=effective_principals or [],
        response=response,
    )


class TestEtagAppVersion:

    def test_sets_etag_and_returns_result(self):
        request = make_request()
        wrapped = etag_app_version(lambda context, req: 'the-result')
        result = wrapped(context=None, request=request)
        assert result == 'the-result'
        assert request.response.etag == APP_VERSION

    def test_raises_not_modified_when_etag_matches(self):
        request = make_request(if_none_match=[APP_VERSION])
        called = []
        wrapped = etag_app_version(lambda context, req: called.append(1))
        with pytest.raises(HTTPNotModified):
            wrapped(context=None, request=request)
        # The underlying view must NOT run when we short-circuit with 304.
        assert called == []


class TestEtagAppVersionEffectivePrincipals:

    def test_etag_includes_sorted_principals(self):
        request = make_request(effective_principals=['b', 'a', 'c'])
        wrapped = etag_app_version_effective_principals(lambda context, req: 'ok')
        result = wrapped(context=None, request=request)
        assert result == 'ok'
        assert request.response.etag == APP_VERSION + ' ' + 'a b c'

    def test_sets_private_no_cache_headers(self):
        request = make_request(effective_principals=['x'])
        wrapped = etag_app_version_effective_principals(lambda context, req: 'ok')
        wrapped(context=None, request=request)
        cc = request.response.cache_control
        assert cc.private is True
        assert cc.max_age == 0
        assert cc.must_revalidate is True

    def test_raises_not_modified_on_matching_principal_etag(self):
        principals = ['a', 'b']
        etag = APP_VERSION + ' ' + 'a b'
        request = make_request(if_none_match=[etag], effective_principals=principals)
        called = []
        wrapped = etag_app_version_effective_principals(
            lambda context, req: called.append(1))
        with pytest.raises(HTTPNotModified):
            wrapped(context=None, request=request)
        assert called == []

    def test_different_principals_produce_different_etag(self):
        # Guards against cache poisoning across principal sets: distinct principals
        # must never collide on the same ETag.
        wrapped = etag_app_version_effective_principals(lambda context, req: 'ok')
        r1 = make_request(effective_principals=['a'])
        r2 = make_request(effective_principals=['a', 'admin'])
        wrapped(None, r1)
        wrapped(None, r2)
        assert r1.response.etag != r2.response.etag
