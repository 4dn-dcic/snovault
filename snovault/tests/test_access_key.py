"""
Regression tests for a privilege-escalation vulnerability in the AccessKey schema.

The AccessKey collection ACL allows any Authenticated (non-admin) user to POST a new
access key (`(Allow, Authenticated, 'add')` in snovault/types/access_key.py). The
`access_key_add` view only defaults the `user` field to the requesting user when the
client does not supply one explicitly. Historically, the `user` property on
access_key.json had no `"permission": "restricted_fields"` restriction (unlike its
sibling admin-only fields `access_key_id`, `secret_access_key_hash`, and
`expiration_date`), so a non-admin user could explicitly set `"user"` in the POST body
to another user's uuid and mint valid API credentials for that other user (including an
admin), fully bypassing per-user authorization.
"""

import webtest


def _create_user(testapp, first_name, last_name, email):
    item = {
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
    }
    res = testapp.post_json('/users', item, status=201)
    return res.json['@graph'][0]


def _testapp_for_user(app, user):
    """ Build a TestApp authenticated (via the `remoteuser` multiauth policy) as the
        given (non-admin) real user, so `request.effective_principals` includes a real
        `userid.<uuid>` principal, matching how a genuine logged-in non-admin user
        would appear to the application. """
    # Note: REMOTE_USER is the *raw* value seen by RemoteUserAuthenticationPolicy;
    # NamespacedAuthenticationPolicy itself prepends the 'remoteuser.' namespace
    # prefix (see snovault.authentication.NamespacedAuthenticationPolicy), so it
    # must NOT be included here (compare to the 'TEST'/'TEST_AUTHENTICATED' values
    # used by the testapp/authenticated_testapp fixtures in testappfixtures.py).
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': user['uuid'],
    }
    return webtest.TestApp(app, environ)


def test_access_key_user_field_is_restricted_for_non_admin(app, testapp):
    """
    A non-admin authenticated user must NOT be able to create an access key on behalf
    of another user by supplying an explicit "user" field in the POST body. With the
    `user` field correctly marked `"permission": "restricted_fields"`, this is rejected
    at schema-validation time with a 422 referencing the 'user' field, and no such key
    is ever persisted.

    NOTE on `?render=false`: without the fix, POSTing without `render=false` happens to
    return a 403 instead of a 2xx, but for an unrelated reason -- `collection_add`
    synchronously renders the newly-created item back to the caller (`as_user=True`),
    which 403s because the item is now owned (`role.owner`) by the victim, not the
    attacker; the resulting non-2xx response then makes pyramid_tm's default commit
    veto abort the whole transaction, incidentally rolling back the malicious write too.
    That's an accidental side effect of the default rendering behavior, not an actual
    fix, so it under-tests the vulnerability. Passing `render=false` (a normal,
    supported client option, see `crud_views.collection_add`/`render_item`) skips that
    synchronous render and isolates exactly the bug this test targets: whether the
    schema/permission layer itself rejects the attacker-supplied `user` field. Confirmed
    empirically: against the pre-fix schema, `POST /access-keys?render=false
    {"user": "<victim-uuid>"}` returns 201 and persists an access key (complete with a
    usable `secret_access_key` handed back to the attacker) attributed to the victim.
    """
    attacker = _create_user(testapp, 'Eve', 'Attacker', 'eve.attacker@example.com')
    victim = _create_user(testapp, 'Victor', 'Victim', 'victor.victim@example.com')
    attacker_testapp = _testapp_for_user(app, attacker)

    res = attacker_testapp.post_json(
        '/access-keys?render=false',
        {'user': victim['uuid']},
        status='*',
    )

    assert res.status_int == 422, (
        "Expected the malicious request (non-admin setting AccessKey.user to another "
        "user's uuid) to be rejected with 422, got %s: %r" % (res.status_int, res.json)
    )
    errors = res.json['errors']
    assert any('user' in str(error.get('name', '')) for error in errors), (
        "Expected a validation error referencing the restricted 'user' field, "
        "got: %r" % errors
    )

    # Authoritative check: confirm (as admin) that no access key was ever persisted
    # that attributes itself to the victim user.
    listing = testapp.get(
        '/access-keys/@@listing?limit=all&frame=object', status=200
    ).json
    victim_path_suffix = '/%s/' % victim['uuid']
    leaked = [
        item for item in listing['@graph']
        if item.get('user') == victim['uuid']
        or str(item.get('user', '')).endswith(victim_path_suffix)
    ]
    assert not leaked, (
        "Found access key(s) attributed to the victim user, created by a non-admin "
        "attacker via the AccessKey.user privilege-escalation path: %r" % leaked
    )


def test_access_key_defaults_to_requesting_user(app, testapp):
    """
    Sanity/control check: a non-admin user creating an access key *without* specifying
    `user` still works, and the resulting key is correctly attributed to themselves
    (the existing, intended default-assignment behavior in access_key_add must keep
    working after locking down the `user` field).
    """
    attacker = _create_user(testapp, 'Alice', 'Selfservice', 'alice.selfservice@example.com')
    attacker_testapp = _testapp_for_user(app, attacker)

    res = attacker_testapp.post_json('/access-keys', {}, status=201)
    access_key = res.json['@graph'][0]
    assert access_key['user'].strip('/').split('/')[-1] == attacker['uuid']
