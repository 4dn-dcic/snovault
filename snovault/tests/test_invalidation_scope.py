import pytest
import mock
import copy
from contextlib import contextmanager
from ..elasticsearch.indexer_utils import filter_invalidation_scope


# Mocked uuids
UUID1 = 'UUID1'
UUID2 = 'UUID2'

# Mocked item types
ITEM_A = 'Item_A'
ITEM_B = 'Item_B'
ITEM_C = 'Item_C'
ITEM_D = 'Item_D'
ITEM_E = 'Item_E'
ITEM_F = 'Item_F'


@contextmanager
def invalidation_scope_mocks(schema, embedded_list, base_types=None):
    """ Quick wrapper for a common operation in this testing - will patch base_types as well if specified """
    with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_properties',
                    return_value=schema):
        with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_embedded_list',
                        return_value=embedded_list):
            if base_types is not None:
                with mock.patch('snovault.elasticsearch.indexer_utils.extract_base_types',
                                return_value=base_types):
                    yield
            else:
                yield


@pytest.fixture
def test_link_source_schema():
    """ Intended schema structure. Meant to test multiple different modification scenarios
            Item_A (uuid1):
                * link_one: linkTo C
                * link_two: linkTo D
                * link_three: linkTo E
                * link_four: array of linkTo F
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


@pytest.fixture
def test_parent_type_schema():
    """ Intended schema structure to test schemas that linkTo base types
            Item_B (uuid2):
                * link_one: linkTo C where C is a parent class of D
                * link_two: linkTo E where E is a parent class of F

    """
    return {
        'link_one': {
            'type': 'string',
            'linkTo': ITEM_C
        },
        'link_two': {
            'type': 'string',
            'linkTo': ITEM_E
        },
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
            ITEM_C + '.name'
        ]
        invalidated = [
            (UUID1, item_type)
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(test_link_source_schema, embedded_field):
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
            ITEM_C + '.value',
            ITEM_D + '.name',
            ITEM_E + '.key'
        ]
        invalidated = [
            (UUID1, item_type)
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(test_link_source_schema, embedded_list):
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
            ITEM_C + '.name',
            ITEM_D + '.value'
        ]
        invalidated = [
            (UUID1, item_type)
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

    @pytest.mark.parametrize('diff,embedded_list',
                             [([ITEM_C + '.name', 'Item_X.value'], ['link_one.value']),
                              # ([ITEM_C + '.valuevalue'], ['link_one.value']), XXX: test case that doesn't work
                              ([ITEM_D + '.value', ITEM_E + '.key'], ['link_two.name', 'link_three.value']),
                              ([ITEM_C + '.key', ITEM_D + '.key', ITEM_E + '.key'], ['link_one.value', 'link_two.name']),
                              ([ITEM_E + '.key.name'], ['link_three.key.value'])])
    def test_invalidation_scope_negative_diffs(self, testapp, test_link_source_schema, diff, embedded_list):
        """ Tests a few possible edit + embedding_list combinations that should NOT result
            in re-indexing (so the added item_type is removed from the invalidation scope).
        """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)  # item type doesn't matter here
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 0)

    @pytest.mark.parametrize('diff,embedded_list',
                             [([ITEM_C + '.value'], ['link_one.value']),
                              ([ITEM_C + '.value'], ['link_one.*']),
                              ([ITEM_C + '.name'], ['link_one.value', 'link_two.*']),
                              ([ITEM_F + '.name'], ['link_four.name']),
                              ([ITEM_F + '.name'], ['link_four.*']),
                              ([ITEM_D + '.key', ITEM_F + '.value'], ['link_two.value', 'link_four.*']),
                              ([ITEM_D + '.key', ITEM_F + '.value'], ['link_two.key', 'link_four.key']),
                              ([ITEM_E + '.key.name'], ['link_three.*']),
                              ([ITEM_E + '.key.name'], ['link_three.key.*']),
                              ([ITEM_E + '.key.name'], ['link_three.key.name'])])
    def test_invalidation_scope_positive_diffs(self, testapp, test_link_source_schema, diff, embedded_list):
        """ Tests a few possible edit + embedding_list combinations that should result in
            invalidation.
        """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)  # item type doesn't matter here
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(test_link_source_schema, embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

    @pytest.mark.parametrize('diff,embedded_list,base_types', [
        ([ITEM_C + '.value'], ['link_one.value'], [ITEM_D])
    ])
    def test_invalidation_scope_base_types(self, testapp, diff, embedded_list, base_types, test_parent_type_schema):
        """ Runs some tests that involve base type resolution """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(test_parent_type_schema, embedded_list, base_types=base_types):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)