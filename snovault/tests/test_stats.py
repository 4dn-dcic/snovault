from webob.compat import parse_qsl_text

from dcicutils.qa_utils import known_bug_expected


def test_query_params(testapp):

    # One actual bug was seeing format=%EF%BF%BD%27 but I haven't been able to reproduce that in captivity yet.
    # Another is UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc0 in position 1: invalid start byte
    # -kmp

    with known_bug_expected(jira_ticket="C4-887", fixed=False, error_class=UnicodeEncodeError):
        weird_text = b'\xef\xbf\xbd\x22'.decode('utf-8')
        url = '/foo?format=' + weird_text
        r = testapp.get(url, status=400)
        assert r.status_code == 400

    with known_bug_expected(jira_ticket="C4-887", fixed=False, error_class=UnicodeDecodeError):
        url = "/foo?format=%e2%c0"
        r = testapp.get(url, status=400)
        assert r.status_code == 400

    with known_bug_expected(jira_ticket="C4-887", fixed=False, error_class=UnicodeDecodeError):
        url = "/foo?format=%ff%ff"
        r = testapp.get(url, status=400)
        assert r.status_code == 400

    with known_bug_expected(jira_ticket="C4-887", fixed=False, error_class=UnicodeEncodeError):
        bullet_character = b'\xe2\x80\xa2'.decode('utf-8')
        url = f"/foo?format={bullet_character}"
        r = testapp.get(url, status=400)
        assert r.status_code == 400
