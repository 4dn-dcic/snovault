"""
Regression tests for the self-registration endpoint create_unauthorized_user.

The endpoint runs with request.remote_user = 'EMBED' (permission = restricted_fields) while
validating/creating the new User, so any caller-submitted field not explicitly filtered out
would be written through as-is - letting a caller self-assign privileged fields such as
"groups": ["admin"] on their own new account. These tests confirm the whitelist filtering in
create_unauthorized_user strips such fields while preserving normal registration behavior.
"""
import json
from unittest import mock

import pytest
import transaction as transaction_management

from ..authentication import create_unauthorized_user
from dcicutils.qa_utils import notice_pytest_fixtures


def _set_request_json(request, payload):
    request.body = json.dumps(payload).encode('utf-8')
    request.content_type = 'application/json'


def _mock_recaptcha_success():
    response = mock.Mock()
    response.json.return_value = {'success': True}
    return mock.patch('snovault.authentication.requests.get', return_value=response)


@pytest.fixture
def registration_request(dummy_request, threadlocals):
    notice_pytest_fixtures(threadlocals)
    dummy_request.method = 'POST'
    dummy_request.context = dummy_request.root
    dummy_request.registry.settings['g.recaptcha.secret'] = 'dummy-recaptcha-secret'
    return dummy_request


def test_create_unauthorized_user_strips_privileged_fields(registration_request, testapp, transaction):
    """ A registrant who submits privileged fields (groups, status) alongside their
        legitimate registration info must not have those fields applied to the new User -
        only the whitelisted self-registration fields (plus the server-forced
        was_unauthorized=True) should land.
    """
    notice_pytest_fixtures(transaction)
    email = 'sneaky-registrant@example.com'
    payload = {
        'email': email,
        'first_name': 'Sneaky',
        'last_name': 'Registrant',
        'groups': ['admin'],
        'status': 'deleted',
        'g-recaptcha-response': 'dummy-response',
    }
    _set_request_json(registration_request, payload)
    registration_request._auth0_authenticated = email

    with _mock_recaptcha_success():
        result = create_unauthorized_user(None, registration_request)

    assert result['status'] == 'success'
    transaction_management.commit()

    item_uri = result['@graph'][0]
    user = testapp.get('{}?frame=object'.format(item_uri)).json

    assert user['email'] == email
    assert user['first_name'] == 'Sneaky'
    assert user['last_name'] == 'Registrant'
    assert user['was_unauthorized'] is True
    # Privileged fields the registrant tried to self-assign must not have landed.
    assert 'groups' not in user
    assert user['status'] == 'current'  # schema default, NOT the submitted 'deleted'


def test_create_unauthorized_user_normal_registration_unaffected(registration_request, testapp, transaction):
    """ A normal registration with no extra/privileged fields must still succeed with the
        expected fields applied.
    """
    notice_pytest_fixtures(transaction)
    email = 'normal-registrant@example.com'
    payload = {
        'email': email,
        'first_name': 'Normal',
        'last_name': 'Registrant',
        'g-recaptcha-response': 'dummy-response',
    }
    _set_request_json(registration_request, payload)
    registration_request._auth0_authenticated = email

    with _mock_recaptcha_success():
        result = create_unauthorized_user(None, registration_request)

    assert result['status'] == 'success'
    transaction_management.commit()

    item_uri = result['@graph'][0]
    user = testapp.get('{}?frame=object'.format(item_uri)).json

    assert user['email'] == email
    assert user['first_name'] == 'Normal'
    assert user['last_name'] == 'Registrant'
    assert user['was_unauthorized'] is True
