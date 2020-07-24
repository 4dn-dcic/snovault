import pytest
from ..elasticsearch.indexer_utils import filter_invalidation_scope


class TestInvalidationScope:

    TESTING_LINK_TARGET_SNO = 'TestingLinkTargetElasticSearch'
    TESTING_LINK_TARGET_ELASTICSEARCH = 'TestingLinkTargetElasticSearch'

    @staticmethod
    def build_invalidated_and_secondary_objects(number_to_generate, item_type):
        invalidated = []
        secondary = set()
        for i in range(number_to_generate):
            _id = 'uuid' + str(i)
            invalidated.append((_id, item_type))
            secondary.add(_id)
        return invalidated, secondary

    @pytest.mark.parametrize('item_type', [TESTING_LINK_TARGET_SNO, TESTING_LINK_TARGET_ELASTICSEARCH])
    def test_invalidation_scope_basic(self, testapp, item_type):
        """ Uses some test data to test base invalidation scope cases """
        registry = testapp.app.registry
        diff = [
            item_type + '.name'
        ]
        invalidated = [  # not touched so can remain the same
            ('uuid1', 'TestingLinkSourceSno')
        ]
        secondary = {'uuid1'}
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 0  # since name field is embedded, we don't care about this

        # override diff to instead correspond to a modification to status field, which is embedded
        diff = [item_type + '.status']
        secondary = {'uuid1'}
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 1

        # given 10 uuids of the same type, it should still not filter any out
        invalidated, secondary = self.build_invalidated_and_secondary_objects(10, item_type)
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 10
