import pytest
from .test_attachment import testing_download  # noQA fixture import
from ..drs import REQUIRED_FIELDS


class TestDRSAPI:
    """ Class for testing the DRS implementation - uses TestingDownload as it implements
        the @@download scheme
    """
    BASE_URL = 'http://localhost:80/'

    def test_drs_get_object(self, testapp, testing_download):  # noQA fixture
        """ Tests basic structure about a drs object """
        res = testapp.get(testing_download)
        drs_object_uri = res.json['accession']
        drs_object_uuid = res.json['uuid']
        testapp.options(f'/ga4gh/drs/v1/objects/{drs_object_uri}',
                        headers={'Content-Type': 'application/json'}, status=204)
        drs_object_1 = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}').json
        for key in REQUIRED_FIELDS:
            assert key in drs_object_1
        assert drs_object_1['self_uri'] == f'drs://localhost:80/{drs_object_uri}'
        assert (drs_object_1['access_methods'][0]['access_url']['url']
                == f'{self.BASE_URL}{drs_object_uuid}/@@download')
        assert (drs_object_1['access_methods'][0]['access_id'] == 'http')

        # failure cases
        testapp.get(f'/ga4gh/drs/v1/objects/not_a_uri', status=404)

        # @@drs case
        drs_object_2 = testapp.get(f'/{drs_object_uri}/@@drs')
        for key in REQUIRED_FIELDS:
            assert key in drs_object_2

    def test_drs_get_object_url(self, testapp, testing_download):  # noQA fixture
        """ Tests extracting URL through ga4gh pathway """
        res = testapp.get(testing_download)
        drs_object_uri = res.json['uuid']

        # standard URI with meaningful access_id, discarded
        drs_object_download = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}/access/https').json
        assert drs_object_download == {
            'url': f'{self.BASE_URL}{drs_object_uri}/@@download'
        }

        # /access/ method
        drs_object_download = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}/access/').json
        assert drs_object_download == {
            'url': f'{self.BASE_URL}{drs_object_uri}/@@download'
        }

        # standard URI with nonsense access id, still discarded
        drs_object_download = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}/access/blah').json
        assert drs_object_download == {
            'url': f'{self.BASE_URL}{drs_object_uri}/@@download'
        }

        # /access method
        drs_object_download = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}/access').json
        assert drs_object_download == {
            'url': f'{self.BASE_URL}{drs_object_uri}/@@download'
        }

    def test_drs_get_object_failure(self, testapp, testing_download):  # noQA fixture
        """ Tests a bunch of bunk URLs """
        res = testapp.get(testing_download)
        drs_object_uri = res.json['uuid']

        with pytest.raises(Exception):
            testapp.get(f'/ga4gh/drs/v1/objects/not_a_uri/access/https')
        with pytest.raises(Exception):
            testapp.get(f'/ga4gh/drs/v1/objects/access/https')
        with pytest.raises(Exception):
            testapp.get(f'/ga4gh/drs/v1/objects/access/')
        with pytest.raises(Exception):
            testapp.get(f'/ga4gh/drs/v1/objects/access')
        with pytest.raises(Exception):
            testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}/accesss/https')

    def test_drs_get_object_returns_json(self, testapp, htmltestapp, testing_download):  # noQA fixture
        """ Tests that even with an htmltestapp, JSON is returned """
        res = testapp.get(testing_download)
        drs_object_uri = res.json['uuid']
        resp = htmltestapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}')
        assert resp.content_type == 'application/json'
