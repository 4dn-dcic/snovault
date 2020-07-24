import pytest
import mock
from ..elasticsearch.indexer_utils import filter_invalidation_scope


# Mocked uuids
UUID1 = 'UUID1'
UUID2 = 'UUID2'
UUID3 = 'UUID3'
UUID4 = 'UUID4'
UUID5 = 'UUID5'

# Mocked item types
ITEM_A = 'Item_A'
ITEM_B = 'Item_B'
ITEM_C = 'Item_C'
ITEM_D = 'Item_D'
ITEM_E = 'Item_E'


@pytest.fixture
def test_link_source_schema():
    return {
        'link_one': {
            'type': 'string',
            'linkTo': ITEM_C
        },
        'link_two': {
            'type': 'string',
            'linkTo': ITEM_D
        },
        'link_three': {
            'type': 'string',
            'linkTo': ITEM_E
        }
    }


class TestInvalidationScope:

    # actual snovault types, used in basic test
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
            (UUID1, 'TestingLinkSourceSno')
        ]
        secondary = {UUID1}
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 0  # since name field is embedded, we don't care about this

        # override diff to instead correspond to a modification to status field, which is embedded
        diff = [item_type + '.status']
        secondary = {UUID1}
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 1

        # given 10 uuids of the same type, it should still not filter any out
        invalidated, secondary = self.build_invalidated_and_secondary_objects(10, item_type)
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 10

    @pytest.mark.parametrize('item_type,embedded_field', [(ITEM_A, 'name'), (ITEM_B, 'key')])
    def test_invalidation_scope_complicated(self, testapp, item_type, test_link_source_schema, embedded_field):
        """ A more complicated test with sophisticated mocks.

            Item_A (uuid1):
                * link_one: linkTo C (uuid3) (embeds name)
                * link_two: linkTo D (uuid4) (embeds key)
                * link_three: linkTo E (uuid5) (embeds value)

            Item_B (uuid2):
                * link_one: linkTo C (uuid3) (embeds key)
                * link_two: linkTo D (uuid4) (embeds value)
                * link_three: linkTo E (uuid5) (embeds name)
        """
        registry = testapp.app.registry
        diff1 = [  # this diff should only invalidate Item_A
            'Item_C.name'
        ]
        invalidated = [
            (UUID1, ITEM_A)
        ]
        secondary = {UUID1}
        with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_properties',
                        return_value=test_link_source_schema):
            with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_embedded_list',
                            return_value=['link_one.name']):
                filter_invalidation_scope(registry, diff1, invalidated, secondary)
                assert len(secondary) == 1
            with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_embedded_list',
                            return_value=['link_one.' + embedded_field]):
                filter_invalidation_scope(registry, diff1, invalidated, secondary)
                if embedded_field == 'name':
                    assert len(secondary) == 1
                else:
                    assert len(secondary) == 0
