import pytest
import webtest
import boto3

from base64 import b64decode
from dcicutils.ff_utils import parse_s3_bucket_and_key_url
from moto import mock_s3
from unittest import mock
from .. import attachment as attachment_module
from ..attachment import file_type, guess_mime_type, system_mime_type, fallback_mime_type, DEFAULT_FALLBACK_MIME_TYPE


# Test for blob storage

RED_DOT = """data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA
AAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO
9TXL0Y4OHwAAAABJRU5ErkJggg=="""

BLUE_DOT = """data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA
oAAAAKAQMAAAC3/F3+AAAACXBIWXMAAA7DAAAOwwHHb6hkAA
AAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAgY0hSTQ
AAeiYAAICEAAD6AAAAgOgAAHUwAADqYAAAOpgAABdwnLpRPA
AAAANQTFRFALfvPEv6TAAAAAtJREFUCB1jYMAHAAAeAAEBGN
laAAAAAElFTkSuQmCC"""


@pytest.fixture
def testing_download(testapp):
    url = '/testing-downloads/'
    item = {
        'attachment': {
            'download': 'red-dot.png',
            'href': RED_DOT,
        },
        'attachment2': {
            'download': 'blue-dot.png',
            'href': BLUE_DOT,
        },
    }
    res = testapp.post_json(url, item, status=201)
    return res.location


class TestAttachment:
    """ Tests attachments with RDBBlobStorage """

    @staticmethod
    def test_download_create(testapp, testing_download):
        res = testapp.get(testing_download)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']

        assert attachment['href'] == '@@download/attachment/red-dot.png'
        assert attachment['type'] == 'image/png'
        assert attachment['width'] == 5
        assert attachment['height'] == 5
        assert attachment['md5sum'] == 'b60ab2708daec7685f3d412a5e05191a'
        url = testing_download + '/' + attachment['href']
        res = testapp.get(url)
        assert res.content_type == 'image/png'
        assert res.body == b64decode(RED_DOT.split(',', 1)[1])

        assert attachment2['href'] == '@@download/attachment2/blue-dot.png'
        assert attachment2['type'] == 'image/png'
        assert attachment2['width'] == 10
        assert attachment2['height'] == 10
        assert attachment2['md5sum'] == '013f03aa088adb19aa226c3439bda179'
        url = testing_download + '/' + attachment2['href']
        res = testapp.get(url)
        assert res.content_type == 'image/png'
        assert res.body == b64decode(BLUE_DOT.split(',', 1)[1])

    @staticmethod
    def test_download_update(testapp, testing_download):
        item = {
            'attachment': {
                'download': 'blue-dot.png',
                'href': BLUE_DOT,
            },
            'attachment2': {
                'download': 'red-dot.png',
                'href': RED_DOT,
            },
        }
        testapp.put_json(testing_download, item, status=200)
        res = testapp.get(testing_download)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']

        assert attachment['href'] == '@@download/attachment/blue-dot.png'
        url = testing_download + '/' + attachment['href']
        res = testapp.get(url)
        assert res.content_type == 'image/png'
        assert res.body == b64decode(BLUE_DOT.split(',', 1)[1])

        assert attachment2['href'] == '@@download/attachment2/red-dot.png'
        url = testing_download + '/' + attachment2['href']
        res = testapp.get(url)
        assert res.content_type == 'image/png'
        assert res.body == b64decode(RED_DOT.split(',', 1)[1])

    @staticmethod
    def test_download_update_no_change(testapp, testing_download):
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': '@@download/attachment/red-dot.png',
            },
            'attachment2': {
                'download': 'blue-dot.png',
                'href': '@@download/attachment2/blue-dot.png',
            },
        }
        testapp.put_json(testing_download, item, status=200)

        res = testapp.get(testing_download)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']
        assert attachment['href'] == '@@download/attachment/red-dot.png'
        assert attachment2['href'] == '@@download/attachment2/blue-dot.png'

    @staticmethod
    def test_download_update_one(testapp, testing_download):
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': '@@download/attachment/red-dot.png',
            },
            'attachment2': {
                'download': 'red-dot.png',
                'href': RED_DOT,
            },
        }
        testapp.put_json(testing_download, item, status=200)

        res = testapp.get(testing_download)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']

        assert attachment['href'] == '@@download/attachment/red-dot.png'
        url = testing_download + '/' + attachment['href']
        res = testapp.get(url)
        assert res.content_type == 'image/png'
        assert res.body == b64decode(RED_DOT.split(',', 1)[1])

        assert attachment2['href'] == '@@download/attachment2/red-dot.png'
        url = testing_download + '/' + attachment2['href']
        res = testapp.get(url)
        assert res.content_type == 'image/png'
        assert res.body == b64decode(RED_DOT.split(',', 1)[1])

    @staticmethod
    def test_download_update_with_premade_href(testapp):
        """
        Test the functionality of PATCHing the attachment information directly
        (which is usually generated by the propsheet)
        """
        post_res = testapp.post_json('/testing-downloads/', {}, status=201)
        item_uuid = post_res.json['@graph'][0]['uuid']
        res = testapp.get('/testing-downloads/' + item_uuid).follow()
        # downloading will not work!
        with pytest.raises(webtest.AppError) as excinfo:
            testapp.get('/testing-downloads/' + item_uuid + '/@@download/attachment/blue-dot.png')
        assert 'Cannot find downloads' in str(excinfo.value)

        assert 'attachment' not in res.json
        patch_body = {
            'attachment': {
                'download': 'blue-dot.png',
                'href': '@@download/attachment/blue-dot.png',
            }
        }
        res2 = testapp.patch_json('/testing-downloads/' + item_uuid, patch_body, status=200)
        attachment = res2.json['@graph'][0]['attachment']
        assert attachment['href'] == '@@download/attachment/blue-dot.png'

    @staticmethod
    def test_download_remove_one(testapp, testing_download):
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': '@@download/attachment/red-dot.png',
            },
        }
        testapp.put_json(testing_download, item, status=200)

        res = testapp.get(testing_download)
        assert 'attachment' in res.json
        assert 'attachment2' not in res.json

        url = testing_download + '/@@download/attachment2/red-dot.png'
        testapp.get(url, status=404)

    @staticmethod
    @pytest.mark.parametrize(
        'href',
        [
            '@@download/attachment/another.png',
            'http://example.com/another.png',
        ])
    def test_download_update_bad_change(testapp, testing_download, href):
        item = {'attachment': {
            'download': 'red-dot.png',
            'href': href,
        }}
        testapp.put_json(testing_download, item, status=422)

    @staticmethod
    @pytest.mark.parametrize(
        'href',
        [
            'http://example.com/another.png',
            'data:image/png;base64,NOT_BASE64',
            'data:image/png;NOT_A_PNG',
            'data:text/plain;asdf',
        ])
    def test_download_create_bad_change(testapp, href):
        url = '/testing-downloads/'
        item = {'attachment': {
            'download': 'red-dot.png',
            'href': href,
        }}
        testapp.post_json(url, item, status=422)

    @staticmethod
    def test_download_create_wrong_extension(testapp):
        url = '/testing-downloads/'
        item = {'attachment': {
            'download': 'red-dot.jpg',
            'href': RED_DOT,
        }}
        testapp.post_json(url, item, status=422)

    @staticmethod
    def test_download_create_w_wrong_md5sum(testapp):
        url = '/testing-downloads/'
        item = {'attachment': {
            'download': 'red-dot.jpg',
            'href': RED_DOT,
            'md5sum': 'deadbeef',
        }}
        testapp.post_json(url, item, status=422)


@pytest.mark.filterwarnings('ignore:stream argument is deprecated')
class TestAttachmentEncrypted:
    """ TODO: It would be great if this class could be elegantly merged with the previous one.
        Note though that the tests do differ in some subtle ways. For example, when using
        s3blobs, we follow s3 redirects via pre-signed URLs - these URLs are parsed via
        helper function and acquired through boto3 in order to interact through moto. -Will Jan 4 2022
    """
    url = '/testing-downloads/'

    def _testing_encrypted_download(self, testapp):  # not a test, just does some setup
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': RED_DOT,
            },
            'attachment2': {
                'download': 'blue-dot.png',
                'href': BLUE_DOT,
            },
        }
        res = testapp.post_json(self.url, item, status=201)
        return res.location

    def create_bucket_and_post_download(self, testapp):
        """ Bootstraps moto and creates some data to work with. """
        blob_bucket = 'encoded-4dn-blobs'  # note that this bucket exists but is mocked out here
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=blob_bucket)
        return self._testing_encrypted_download(testapp)

    @staticmethod
    def _get_attachment_from_s3(client, url):
        """ Uses ff_utils.parse_s3_bucket_and_key_url to parse the s3 URL in our data into it's
            bucket, key pairs and acquires/reads the content.
        """
        bucket, key = parse_s3_bucket_and_key_url(url)
        return client.get_object(Bucket=bucket, Key=key)['Body'].read()

    def attachment_is_red_dot(self, client, url):
        """ Fails assertion if the url is not the RED_DOT """
        content = self._get_attachment_from_s3(client, url)
        assert content == b64decode(RED_DOT.split(',', 1)[1])

    def attachment_is_blue_dot(self, client, url):
        """ Fails assertion if the url is not the BLUE_DOT """
        content = self._get_attachment_from_s3(client, url)
        assert content == b64decode(BLUE_DOT.split(',', 1)[1])

    @mock_s3
    def test_download_create_encrypted(self, encrypted_testapp):
        loc = self.create_bucket_and_post_download(encrypted_testapp)
        res = encrypted_testapp.get(loc)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']

        assert attachment['href'] == '@@download/attachment/red-dot.png'
        assert attachment['type'] == 'image/png'
        assert attachment['width'] == 5
        assert attachment['height'] == 5
        assert attachment['md5sum'] == 'b60ab2708daec7685f3d412a5e05191a'
        url = loc + '/' + attachment['href']

        res = encrypted_testapp.get(url)  # follow redirect to s3
        assert res.content_type == 'application/json'
        self.attachment_is_red_dot(boto3.client('s3'), res.location)

        assert attachment2['href'] == '@@download/attachment2/blue-dot.png'
        assert attachment2['type'] == 'image/png'
        assert attachment2['width'] == 10
        assert attachment2['height'] == 10
        assert attachment2['md5sum'] == '013f03aa088adb19aa226c3439bda179'
        url = loc + '/' + attachment2['href']
        res = encrypted_testapp.get(url)
        assert res.content_type == 'application/json'
        self.attachment_is_blue_dot(boto3.client('s3'), res.location)

    @mock_s3
    def test_download_update_encrypted(self, encrypted_testapp):
        item = {
            'attachment': {
                'download': 'blue-dot.png',
                'href': BLUE_DOT,
            },
            'attachment2': {
                'download': 'red-dot.png',
                'href': RED_DOT,
            },
        }
        loc = self.create_bucket_and_post_download(encrypted_testapp)
        encrypted_testapp.put_json(loc, item, status=200)
        res = encrypted_testapp.get(loc)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']

        assert attachment['href'] == '@@download/attachment/blue-dot.png'
        url = loc + '/' + attachment['href']
        res = encrypted_testapp.get(url)
        self.attachment_is_blue_dot(boto3.client('s3'), res.location)

        assert attachment2['href'] == '@@download/attachment2/red-dot.png'
        url = loc + '/' + attachment2['href']
        res = encrypted_testapp.get(url)
        assert res.content_type == 'application/json'
        self.attachment_is_red_dot(boto3.client('s3'), res.location)

    @mock_s3
    def test_download_update_no_change_encrypted(self, encrypted_testapp):
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': '@@download/attachment/red-dot.png',
            },
            'attachment2': {
                'download': 'blue-dot.png',
                'href': '@@download/attachment2/blue-dot.png',
            },
        }
        loc = self.create_bucket_and_post_download(encrypted_testapp)
        encrypted_testapp.put_json(loc, item, status=200)

        res = encrypted_testapp.get(loc)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']
        assert attachment['href'] == '@@download/attachment/red-dot.png'
        assert attachment2['href'] == '@@download/attachment2/blue-dot.png'

    @mock_s3
    def test_download_update_one_encrypted(self, encrypted_testapp):
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': '@@download/attachment/red-dot.png',
            },
            'attachment2': {
                'download': 'red-dot.png',
                'href': RED_DOT,
            },
        }
        loc = self.create_bucket_and_post_download(encrypted_testapp)
        encrypted_testapp.put_json(loc, item, status=200)

        res = encrypted_testapp.get(loc)
        attachment = res.json['attachment']
        attachment2 = res.json['attachment2']

        assert attachment['href'] == '@@download/attachment/red-dot.png'
        url = loc + '/' + attachment['href']
        res = encrypted_testapp.get(url)
        self.attachment_is_red_dot(boto3.client('s3'), res.location)

        assert attachment2['href'] == '@@download/attachment2/red-dot.png'
        url = loc + '/' + attachment2['href']
        res = encrypted_testapp.get(url)
        assert res.content_type == 'application/json'
        self.attachment_is_red_dot(boto3.client('s3'), res.location)

    @mock_s3
    def test_download_update_with_premade_href_encrypted(self, encrypted_testapp):
        """
        Test the functionality of PATCHing the attachment information directly
        (which is usually generated by the propsheet)
        """
        post_res = encrypted_testapp.post_json(self.url, {}, status=201)
        item_uuid = post_res.json['@graph'][0]['uuid']
        res = encrypted_testapp.get(self.url + item_uuid).follow()
        # downloading will not work!
        with pytest.raises(webtest.AppError) as excinfo:
            encrypted_testapp.get(self.url + item_uuid + '/@@download/attachment/blue-dot.png')
        assert 'Cannot find downloads' in str(excinfo.value)

        assert 'attachment' not in res.json
        patch_body = {
            'attachment': {
                'download': 'blue-dot.png',
                'href': '@@download/attachment/blue-dot.png',
            }
        }
        res2 = encrypted_testapp.patch_json(self.url + item_uuid, patch_body, status=200)
        attachment = res2.json['@graph'][0]['attachment']
        assert attachment['href'] == '@@download/attachment/blue-dot.png'

    @mock_s3
    def test_download_remove_one_encrypted(self, encrypted_testapp):
        item = {
            'attachment': {
                'download': 'red-dot.png',
                'href': '@@download/attachment/red-dot.png',
            },
        }
        loc = self.create_bucket_and_post_download(encrypted_testapp)
        encrypted_testapp.put_json(loc, item, status=200)

        res = encrypted_testapp.get(loc)
        assert 'attachment' in res.json
        assert 'attachment2' not in res.json

        url = loc + '/@@download/attachment2/red-dot.png'
        encrypted_testapp.get(url, status=404)

    @mock_s3
    @pytest.mark.parametrize(
        'href',
        [
            '@@download/attachment/another.png',
            'http://example.com/another.png',
        ])
    def test_download_update_bad_change_encrypted(self, encrypted_testapp, href):
        item = {'attachment': {
            'download': 'red-dot.png',
            'href': href,
        }}
        loc = self.create_bucket_and_post_download(encrypted_testapp)
        encrypted_testapp.put_json(loc, item, status=422)

    @mock_s3
    @pytest.mark.parametrize(
        'href',
        [
            'http://example.com/another.png',
            'data:image/png;base64,NOT_BASE64',
            'data:image/png;NOT_A_PNG',
            'data:text/plain;asdf',
        ])
    def test_download_create_bad_change_encrypted(self, encrypted_testapp, href):
        item = {'attachment': {
            'download': 'red-dot.png',
            'href': href,
        }}
        encrypted_testapp.post_json(self.url, item, status=422)

    @mock_s3
    def test_download_create_wrong_extension_encrypted(self, encrypted_testapp):
        item = {'attachment': {
            'download': 'red-dot.jpg',
            'href': RED_DOT,
        }}
        encrypted_testapp.post_json(self.url, item, status=422)

    @mock_s3
    def test_download_create_w_wrong_md5sum_encrypted(self, encrypted_testapp):
        item = {'attachment': {
            'download': 'red-dot.jpg',
            'href': RED_DOT,
            'md5sum': 'deadbeef',
        }}
        encrypted_testapp.post_json(self.url, item, status=422)


def test_file_type():

    assert file_type("/xyz/foo") == ""
    assert file_type("/xyz/foo.abc") == ".abc"
    assert file_type("/xyz/foo.abc.def") == ".def"

    assert file_type("xyz/foo.abc") == ".abc"
    assert file_type("foo.abc") == ".abc"


def test_system_mime_type():
    check_mime_type(system_mime_type, unknown=None)


def test_fallback_mime_type():
    check_mime_type(fallback_mime_type, unknown=None)


def test_guess_mime_type():
    check_mime_type(guess_mime_type, unknown=DEFAULT_FALLBACK_MIME_TYPE)
    with mock.patch.object(attachment_module, "system_mime_type") as mock_system_mime_type:
        mock_system_mime_type.return_value = None
        assert mock_system_mime_type.call_count == 0
        check_mime_type(guess_mime_type, unknown=DEFAULT_FALLBACK_MIME_TYPE)
        assert mock_system_mime_type.call_count > 0


def check_mime_type(guesser, *, unknown):
    assert guesser("/xyz/foo.txt") == "text/plain"
    assert guesser("/xyz/foo.text") == "text/plain"
    assert guesser("/xyz/foo.csv") == "text/csv"
    assert guesser("/xyz/foo.tsv") == "text/tab-separated-values"
    assert guesser("/xyz/foo.xlsx") == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert guesser("/xyz/foo.xls") == "application/vnd.ms-excel"
    assert guesser("/xyz/foo.docx") == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    assert guesser("/xyz/foo.doc") == "application/msword"
    assert guesser("/xyz/foo.htm") == "text/html"
    assert guesser("/xyz/foo.html") == "text/html"
    assert guesser("/xyz/foo.json") == "application/json"

    # There is no recommende extension for "application/ld+json"
    # assert guesser(...) == "application/ld+json"

    # Random type just returns None
    assert guesser("/xyz/foo.xyzzy") is unknown
