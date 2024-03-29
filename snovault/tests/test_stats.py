from dcicutils.qa_utils import known_bug_expected


def test_query_param_unicode_decode_error_c4_887_regression(testapp):
    """
    Attempts to provoke UnicodeDecodeError problems with strange characters in URL query params.
    """

    # In C4-887, an actual failing URL seemed to involve using format=%EF%BF%BD%27 in Sentry,
    # although that may be just how Sentry presented the URL. I couldn't get that URL to work
    # in testing, but I got similar ones, and I'm pretty sure the bug fix is addressing those
    # problems. We think these were all variations on a theme generated by pen testing.
    # -kmp 14-Sep-2022

    with known_bug_expected(jira_ticket="C4-887", fixed=True, error_class=UnicodeDecodeError):
        # One of the actual error messages seen in logs was:
        #     UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc0 in position 1: invalid start byte
        # and this URL is constructed from that as a clue. -kmp 14-Sep-2022
        url = "/foo?format=%e2%c0"
        r = testapp.get(url, status=400)
        assert r.status_code == 400

    with known_bug_expected(jira_ticket="C4-887", fixed=True, error_class=UnicodeDecodeError):
        url = "/foo?format=%ff%ff"
        r = testapp.get(url, status=400)
        assert r.status_code == 400


def test_query_param_unicode_encode_error_c4_887_regression(testapp):
    """
    Attempts to provoke UnicodeEncodeError problems with strange characters in URL query params.
    """

    # Although C4-887 did not complain of UnicodeEncodeEror issues, these came up while trying to find
    # ways to reproduce the actual error. They were all addressed in the same way.

    with known_bug_expected(jira_ticket="C4-887", fixed=True, error_class=UnicodeEncodeError):
        weird_text = b'\xef\xbf\xbd\x22'.decode('utf-8')
        url = f"/foo?format={weird_text}"
        r = testapp.get(url, status=400)
        assert r.status_code == 400

    with known_bug_expected(jira_ticket="C4-887", fixed=True, error_class=UnicodeEncodeError):
        bullet_character = b'\xe2\x80\xa2'.decode('utf-8')
        url = f"/foo?format={bullet_character}"
        r = testapp.get(url, status=400)
        assert r.status_code == 400
