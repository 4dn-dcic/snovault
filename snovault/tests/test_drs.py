import pytest
from pyramid.exceptions import HTTPNotFound
from .test_attachment import testing_download  # noQA fixture import


class TestDRSAPI:
    """ Class for testing the DRS implementation - uses TestingDownload as it implements
        the @@download scheme
    """

    REQUIRED_FIELDS = [
        'id',
        'created_time',
        'drs_id',
        'self_uri'
    ]

    def test_drs_get_object(self, testapp, testing_download):
        """ Tests basic structure about a drs object """
        res = testapp.get(testing_download)
        drs_object_uri = res.json['uuid']
        drs_object = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}')
        for key in self.REQUIRED_FIELDS:
            assert key in drs_object
        assert drs_object['self_uri'] == 'drs://localhost:80/ga4gh/drs/v1/objects/211826d0-8f5e-4d83-b86a-6cabb3cfeff1'

        # failure cases
        testapp.get(f'/ga4gh/drs/v1/objects/not_a_uri', status=404)
