"""
Fixtures  for pyramid, embedding
"""

import pytest

from dcicutils.qa_utils import notice_pytest_fixtures
from pyramid.request import apply_request_extensions
from pyramid.testing import setUp, tearDown
from pyramid.threadlocal import manager
from .toolfixtures import registry


notice_pytest_fixtures(registry)


@pytest.yield_fixture
def config():
    yield setUp()
    tearDown()


@pytest.yield_fixture
def threadlocals(request, dummy_request, registry):
    notice_pytest_fixtures(request, dummy_request, registry)
    manager.push({'request': dummy_request, 'registry': registry})
    yield manager.get()
    manager.pop()


@pytest.fixture
def dummy_request(root, registry, app):
    request = app.request_factory.blank('/dummy')
    request.root = root
    request.registry = registry
    request._stats = {}
    request.invoke_subrequest = app.invoke_subrequest
    apply_request_extensions(request)
    return request
