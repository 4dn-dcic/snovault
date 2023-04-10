import pytest
from .test_attachment import testing_download


class TestDRSAPI:
    """ Class for testing the DRS implementation - uses TestingDownload as it implements
        the @@download scheme
    """

    REQUIRED_FIELDS = [
        'id',
        'created_time',
        'drs_id'
    ]

    def test_drs_get_object(self, testapp, testing_download):
        """ Tests basic structure about a drs object """
        res = testapp.get(testing_download)
        drs_object_uri = res.json['uuid']
        drs_object = testapp.get(f'/ga4gh/drs/v1/objects/{drs_object_uri}')
        for key in self.REQUIRED_FIELDS:
            assert key in drs_object
