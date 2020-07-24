import pytest
import mock
import copy
from contextlib import contextmanager
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
ITEM_F = 'Item_F'


@contextmanager
def mock_schema_and_embedded_list(schema, embedded_list):
    """ Quick wrapper for a common operation in this testing """
    with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_properties',
                    return_value=schema):
        with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_embedded_list',
                        return_value=embedded_list):
            yield


@pytest.fixture
def test_link_source_schema():
    """ Structure, combined with above:
            Item_A (uuid1):
                    * link_one: linkTo C (uuid3) (embeds name)
                    * link_two: linkTo D (uuid4) (embeds key)
                    * link_three: linkTo E (uuid5) (embeds value)

            Item_B (uuid2):
                * link_one: linkTo C (uuid3) (embeds key)
                * link_two: linkTo D (uuid4) (embeds value)
                * link_three: linkTo E (uuid5) (embeds name)
    """
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
        },
        'link_four': {
            'type': 'array',
            'items': {
                'type': 'string',
                'linkTo': ITEM_F
            }
        }
    }


class TestInvalidationScope:

    # actual snovault types, used in basic test
    TESTING_LINK_TARGET_SNO = 'TestingLinkTargetElasticSearch'
    TESTING_LINK_TARGET_ELASTICSEARCH = 'TestingLinkTargetElasticSearch'

    @staticmethod
    def build_invalidated_and_secondary_objects(number_to_generate, item_type):
        """ Helper that returns 2-tuple of invalidated,secondary intermediary structure

        :param number_to_generate: number of uuids to generate
        :param item_type: what type they should be
        :return: invalidated (list), secondary (set)
        """
        invalidated = []
        secondary = set()
        for i in range(number_to_generate):
            _id = 'uuid' + str(i)
            invalidated.append((_id, item_type))
            secondary.add(_id)
        return invalidated, secondary

    @staticmethod
    def run_test_and_reset_secondary(registry, diff, invalidated, secondary, expected):
        """ Helper method that copies and filters the secondary list, checking that the
            expected number of uuids are there.
        """
        secondary_copy = copy.deepcopy(secondary)
        filter_invalidation_scope(registry, diff, invalidated, secondary_copy)
        assert len(secondary_copy) == expected

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
        # since name field is not embedded, we don't care about this edit
        self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 0)

        # override diff to instead correspond to a modification to status field, which is embedded
        diff = [item_type + '.status']
        self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

        # given 10 uuids of the same type, it should still not filter any out
        invalidated, secondary = self.build_invalidated_and_secondary_objects(10, item_type)
        filter_invalidation_scope(registry, diff, invalidated, secondary)
        assert len(secondary) == 10

    @pytest.mark.parametrize('item_type,embedded_field', [(ITEM_A, ['link_one.name']), (ITEM_B, ['link_one.key'])])
    def test_invalidation_scope_complicated(self, testapp, item_type, test_link_source_schema, embedded_field):
        """ Runs two scenarios:
                1. We edited field 'name' on ITEM_C, invalidating ITEM_A since it embeds 'name'
                2. We edited field 'name' on ITEM_C, NOT invalidating ITEM_B since it embeds 'key'
        """
        registry = testapp.app.registry
        diff = [  # this diff should only invalidate Item_A
            'Item_C.name'
        ]
        invalidated = [
            (UUID1, item_type)
        ]
        secondary = {UUID1}
        with mock_schema_and_embedded_list(test_link_source_schema, embedded_field):
            filter_invalidation_scope(registry, diff, invalidated, secondary)
            if embedded_field[0] == 'link_one.name':
                assert len(secondary) == 1
            else:
                assert len(secondary) == 0

    @pytest.mark.parametrize('item_type,embedded_list',
                             [(ITEM_A, ['link_one.name', 'link_two.key', 'link_three.value']),
                              (ITEM_B, ['link_one.key', 'link_two.value', 'link_three.name'])])
    def test_invalidation_scope_many_unseen_changes(self, testapp, item_type, embedded_list, test_link_source_schema):
        """ Similar test to above except multiple modifications are made in the diff,
            NONE of which should impact the invalidation scope.
        """
        registry = testapp.app.registry
        diff = [  # this diff should invalidate NONE, so secondary should be cleared
            'Item_C.value',
            'Item_D.name',
            'Item_E.key'
        ]
        invalidated = [
            (UUID1, item_type)
        ]
        secondary = {UUID1}
        with mock_schema_and_embedded_list(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 0)

    @pytest.mark.parametrize('item_type,embedded_list', [
        (ITEM_A, ['link_one.name', 'link_two.key', 'link_three.value']),
        (ITEM_B, ['link_one.key', 'link_two.value', 'link_three.name'])])
    def test_invalidation_scope_many_changes(self, testapp, item_type, embedded_list, test_link_source_schema):
        """ Again similar to the above test except this time the diff will trigger an
            invalidation on both item types.
        """
        registry = testapp.app.registry
        diff = [  # this diff invalidates BOTH
            'Item_C.name',
            'Item_D.value'
        ]
        invalidated = [
            (UUID1, item_type)
        ]
        secondary = {UUID1}
        with mock_schema_and_embedded_list(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

    @pytest.mark.parametrize('diff,embedded_list',
                             [(['Item_C.name', 'Item_X.value'], ['link_one.value']),
                              (['Item_D.value', 'Item_E.key'], ['link_two.name', 'link_three.value']),
                              (['Item_C.key', 'Item_D.key', 'Item_E.key'], ['link_one.value', 'link_two.name'])])
    def test_invalidation_scope_negative_diffs(self, testapp, test_link_source_schema, diff, embedded_list):
        """ Tests a few possible edit + embedding_list combinations that should NOT result
            in re-indexing (so the added item_type is removed from the invalidation scope).
        """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)  # item type doesn't matter here
        ]
        secondary = {UUID1}
        with mock_schema_and_embedded_list(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 0)

    @pytest.mark.parametrize('diff,embedded_list',
                             [(['Item_C.value'], ['link_one.value']),
                              (['Item_C.value'], ['link_one.*']),
                              (['Item_C.name'], ['link_one.value', 'link_two.*']),
                              (['Item_F.name'], ['link_four.name']),
                              (['Item_F.name'], ['link_four.*']),
                              (['Item_D.key', 'Item_F.value'], ['link_two.value', 'link_four.*']),
                              (['Item_D.key', 'Item_F.value'], ['link_two.key', 'link_four.key'])])
    def test_invalidation_scope_positive_diffs(self, testapp, test_link_source_schema, diff, embedded_list):
        """ Tests a few possible edit + embedding_list combinations that should result in
            invalidation.
        """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)  # item type doesn't matter here
        ]
        secondary = {UUID1}
        with mock_schema_and_embedded_list(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)
