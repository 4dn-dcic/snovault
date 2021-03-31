import pytest
import mock
import copy
from contextlib import contextmanager
from ..elasticsearch.indexer_utils import filter_invalidation_scope, determine_child_types


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
def type_properties_mock(schema):
    with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_properties',
                    return_value=schema):
        yield


@contextmanager
def type_embedded_list_mock(embedded_list):
    with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_embedded_list',
                    return_value=embedded_list):
        yield


@contextmanager
def type_default_diff_mock(use_default_diff):
    if use_default_diff:
        yield
    else:
        with mock.patch('snovault.elasticsearch.indexer_utils.extract_type_default_diff',
                        return_value=[]):
            yield


@contextmanager
def type_base_types_mock(base_types):
    if base_types is not None:
        with mock.patch('snovault.elasticsearch.indexer_utils.extract_base_types',
                        return_value=base_types):
            yield
    else:
        yield


@contextmanager
def type_child_types_mock(child_types):
    if child_types is not None:
        with mock.patch('snovault.elasticsearch.indexer_utils.determine_child_types',
                        return_value=child_types):
            yield
    else:
        yield


@contextmanager
def invalidation_scope_mocks(*, schema, embedded_list, base_types=None, child_types=None, use_default_diff=False):
    """ Quick wrapper for a common operation in this testing (mocking the appropriate things to test this
        without a data model) """
    with type_properties_mock(schema):
        with type_embedded_list_mock(embedded_list):
            with type_default_diff_mock(use_default_diff):
                with type_base_types_mock(base_types):
                    with type_child_types_mock(child_types):
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
            'type': 'object',
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


@pytest.fixture
def test_parent_type_object_schema():
    """ Intended schema structure to test schemas that have an object or array of object field
        that contains a linkTo.
    """
    return {
        'single_object': {
            'type': 'object',
            'properties': {
                'alias': {
                    'type': 'string'
                },
                'link_one': {
                    'type': 'string',
                    'linkTo': ITEM_C
                }
            }
        },
        'many_objects': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'alias': {
                        'type': 'string'
                    },
                    'link_two': {
                        'type': 'string',
                        'linkTo': ITEM_E
                    }
                }
            }
        }
    }


class TestInvalidationScopeUnit:

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
        with invalidation_scope_mocks(schema=test_link_source_schema, embedded_list=embedded_field):
            filter_invalidation_scope(registry, diff, invalidated, secondary)
            if embedded_field[0] == 'link_one.name':
                assert len(secondary) == 1
            else:
                assert len(secondary) == 0

    @pytest.mark.parametrize('diff,embedded_list,expected',
                             [([ITEM_C + '.value'], ['single_object.link_one.value'], 1),  # value matches
                              ([ITEM_C + '.name'], ['single_object.link_one.value'], 0),  # name does not match embed
                              ([ITEM_E + '.name'], ['many_objects.link_two.name'], 1),  # name matches
                              ([ITEM_E + '.name'], ['many_objects.link_two.*'], 1),  # * matches
                              ([ITEM_C + '.*'], ['many_objects.link_two.*'], 0),  # * does not match embed
                              ([ITEM_E + '.value'], ['many_objects.link_two.name'], 0),  # value does not match embed
                              ([ITEM_E + '.value'], ['single_object.link_one.value'], 0),  # diff does not match field
                              ([ITEM_C + '.name'], ['single_object.link_one.*'], 1),  # * matches
                              ([ITEM_C + '.name'], ['many_objects.link_two.*'], 0),  # * does not match embed
                              ([ITEM_C + '.name', ITEM_C + '.value', ITEM_C + '.classification'],
                               ['single_object.link_one.classification'], 1),  # larger diff
                              ([ITEM_C + '.name', ITEM_C + '.value', ITEM_C + '.classification'],
                               ['many_objects.link_two.classification'], 0),  # larger diff w/o embed
                              ([ITEM_E + '.name', ITEM_E + '.value', ITEM_E + '.classification'],
                               ['many_objects.link_two.classification'], 1),

                              ])
    def test_invalidation_scope_object(self, testapp, test_parent_type_object_schema, diff, embedded_list, expected):
        """ Tests that modifying an item that is an object-like linkTo remains in the invalidation scope. """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, 'dummy_type')  # this type doesn't matter
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(schema=test_parent_type_object_schema, embedded_list=embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, expected)

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
        with invalidation_scope_mocks(schema=test_link_source_schema, embedded_list=embedded_list):
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
        with invalidation_scope_mocks(schema=test_link_source_schema, embedded_list=embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

    @pytest.mark.parametrize('diff,embedded_list',
                             [([ITEM_C + '.name', 'Item_X.value'], ['link_one.value']),
                              ([ITEM_D + '.value', ITEM_E + '.key'], ['link_two.name', 'link_three.value']),
                              ([ITEM_C + '.key', ITEM_D + '.key', ITEM_E + '.key'], ['link_one.value', 'link_two.name'])])
    def test_invalidation_scope_negative_diffs(self, testapp, test_link_source_schema, diff, embedded_list):
        """ Tests a few possible edit + embedding_list combinations that should NOT result
            in re-indexing (so the added item_type is removed from the invalidation scope).
        """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)  # item type doesn't matter here
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(schema=test_link_source_schema, embedded_list=embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 0)

    @pytest.mark.parametrize('diff,embedded_list',
                             [([ITEM_C + '.value'], ['link_one.value']),
                              ([ITEM_C + '.value'], ['link_one.*']),
                              ([ITEM_D + '.key', ITEM_F + '.value'], ['link_two.key', 'link_four.key'])])
    def test_invalidation_scope_positive_diffs(self, testapp, test_link_source_schema, diff, embedded_list):
        """ Tests a few possible edit + embedding_list combinations that should result in
            invalidation.
        """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)  # item type doesn't matter here
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(schema=test_link_source_schema, embedded_list=embedded_list):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

    @pytest.mark.parametrize('diff,embedded_list,base_types,child_types', [
        ([ITEM_D + '.value'], ['link_one.value'], [ITEM_C], [ITEM_D])
    ])
    def test_invalidation_scope_base_types(self, testapp, diff, embedded_list, base_types, child_types,
                                           test_parent_type_schema):
        """ Runs some tests that involve base type resolution """
        registry = testapp.app.registry
        invalidated = [
            (UUID1, ITEM_A)
        ]
        secondary = {UUID1}
        with invalidation_scope_mocks(schema=test_parent_type_schema, embedded_list=embedded_list,
                                      base_types=base_types, child_types=child_types):
            self.run_test_and_reset_secondary(registry, diff, invalidated, secondary, 1)

    def test_invalidation_scope_get_child_types(self, testapp):
        """ Tests that we can resolve child types correctly. """
        item_child_types = determine_child_types(testapp.app.registry, 'Item')
        assert item_child_types == ['AbstractItemTestSecondSubItem', 'AbstractItemTestSubItem',  # all types defined
                                    'EmbeddingTest', 'NestedEmbeddingContainer', 'NestedObjectLinkTarget',
                                    'TestingBiogroupSno', 'TestingBiosampleSno', 'TestingBiosourceSno',
                                    'TestingCalculatedProperties', 'TestingDependencies', 'TestingDownload',
                                    'TestingIndividualSno', 'TestingLinkAggregateSno', 'TestingLinkSourceSno',
                                    'TestingLinkTargetElasticSearch', 'TestingLinkTargetSno', 'TestingMixins',
                                    'TestingNestedEnabled', 'TestingPostPutPatchSno', 'TestingServerDefault']
        abstract_item_type_child_types = determine_child_types(testapp.app.registry, 'AbstractItemTest')
        assert abstract_item_type_child_types == ['AbstractItemTestSecondSubItem', 'AbstractItemTestSubItem']


###########################################################
# Tests based on actual views defined in testing_views.py #
###########################################################


@pytest.fixture
def invalidation_scope_individual_data():
    return [
        {
            'full_name': 'Jane Doe',
            'uid': 'abc123',
            'specimen': 'blood'
        },
        {
            'full_name': 'Unknown Contributor',
            'uid': 'UNK1',
            'specimen': 'skin'
        },
    ]


@pytest.fixture
def invalidation_scope_biosample_data():
    return [
        {
            'identifier': 'SNOID123',
            'quality': 100,
            'ranking': 1,
            'alias': '123',
            'contributor': 'Unknown Contributor'
        },
        {
            'identifier': 'SNOID456',
            'quality': 98,
            'ranking': 2,
            'alias': '456',
            'contributor': 'Jane Doe'
        },
        {
            'identifier': 'SNOID789',
            'quality': 95,
            'ranking': 3,
            'alias': '789',
            'contributor': 'Jane Doe'
        }
    ]


@pytest.fixture
def invalidation_scope_biosource_data():
    return [
        {
            'identifier': 'SNOIDABC',
            'samples': ['SNOID123', 'SNOID456', 'SNOID789']
        },
        {
            'identifier': 'SNOIDDEF',
            'samples': ['SNOID456', 'SNOID789'],
            'sample_objects': [
                {
                    'notes': 'A note about SNOID456',
                    'associated_sample': 'SNOID456'
                }
            ],
            'contributor': 'Jane Doe'
        }
    ]


@pytest.fixture
def invalidation_scope_biogroup_data():
    return [{
        'name': 'test-group',
        'sources': 'SNOIDDEF'
    }]


@pytest.fixture
def invalidation_scope_workbook(testapp, invalidation_scope_biosample_data, invalidation_scope_biosource_data,
                                invalidation_scope_biogroup_data, invalidation_scope_individual_data):
    """ Posts 2 individuals, 3 biosamples, 2 biosources and 1 biogroup for integrated testing. """
    groups, sources, samples = [], [], []
    for indiv in invalidation_scope_individual_data:
        testapp.post_json('/TestingIndividualSno', indiv, status=201)
    for biosample in invalidation_scope_biosample_data:
        res = testapp.post_json('/TestingBiosampleSno', biosample, status=201).json['@graph'][0]
        samples.append(res)
    for biosource in invalidation_scope_biosource_data:
        res = testapp.post_json('/TestingBiosourceSno', biosource, status=201).json['@graph'][0]
        sources.append(res)
    for biogroup in invalidation_scope_biogroup_data:
        res = testapp.post_json('/TestingBiogroupSno', biogroup, status=201).json['@graph'][0]
        groups.append(res)
    return groups, sources, samples


class TestingInvalidationScopeIntegrated:

    @staticmethod
    def runtest(testapp, diff, invalidated, secondary, expected):
        __test__ = False
        filter_invalidation_scope(testapp.app.registry, diff, invalidated, secondary)
        assert len(secondary) == expected

    def test_invalidation_scope_integrated_simple_modification(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulated patching the identifier field on biosample. Because identifier
            is an embed for biosource (direct) and also an embed for biogroup (*), all 3 items should be
            invalidated.
        """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingBiosampleSno.identifier']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in sources + groups]
        secondary = {obj['@id'] for obj in sources + groups}
        self.runtest(testapp, diff, invalidated, secondary, 3)

    def test_invalidation_scope_integrated_many_modifications(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulated patching the identifier field on biosample. Because identifier
            is an embed for biosource (direct) and also an embed for biogroup (*), all 3 items should be
            invalidated.
        """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingBiosampleSno.alias', 'TestingBiosampleSno.ranking', 'TestingBiosampleSno.quality']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in sources + groups]
        secondary = {obj['@id'] for obj in sources + groups}
        self.runtest(testapp, diff, invalidated, secondary, 3)  # quality + * will pick up all

    def test_invalidation_scope_integrated_partly_invisible_modification(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulates patching the ranking field on biosample. This field is not a
            direct embed on biosource, so those items should not be invalidated - only biogroup.
        """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingBiosampleSno.ranking']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in sources + groups]
        secondary = {obj['@id'] for obj in sources + groups}
        self.runtest(testapp, diff, invalidated, secondary, 1)

    def test_invalidation_scope_integrated_wholly_invisible_modification(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulates patching the identifier field on biosource. This field is not a
            direct embed on biogroup, so nothing is invalidated.
        """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingBiosourceSno.identifier']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in groups]
        secondary = {obj['@id'] for obj in groups}
        self.runtest(testapp, diff, invalidated, secondary, 1)  # answer is 1 because of default_diff

    def test_invalidation_scope_integrated_depth3_modification_matches(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulates patches the alias on biosource. This field is a direct embed on biosource
            under 'sample_objects.associated_sample.alias' - thus only biosources should be invalidated - but since
            we cannot differentiate this edit by field, all 3 are invalidated.
        """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingBiosampleSno.alias']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in sources + groups]
        secondary = {obj['@id'] for obj in sources + groups}
        self.runtest(testapp, diff, invalidated, secondary, 3)

    def test_invalidation_scope_integrated_depth4_modification_partial(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulates patches the specimen on individual. Since only biosample embeds this
            via *, so biosources should be pruned.
        """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingIndividualSno.specimen']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in sources + samples]
        secondary = {obj['@id'] for obj in sources + samples}
        self.runtest(testapp, diff, invalidated, secondary, 3)

    def test_invalidation_scope_integrated_depth4_modification_full(self, testapp, invalidation_scope_workbook):
        """ Integrated test that simulates patches the full_name on individual. """
        groups, sources, samples = invalidation_scope_workbook
        diff = ['TestingIndividualSno.full_name']
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in sources + samples]
        secondary = {obj['@id'] for obj in sources + samples}
        self.runtest(testapp, diff, invalidated, secondary, 2)

    @pytest.mark.parametrize('diff,expected',
                             [(['TestingIndividualSno.uid'], 1),  # only biogroup is in scope
                              (['TestingIndividualSno.full_name'], 3),  # biosource + biogroup
                              (['TestingIndividualSno.specimen'], 4),  # biosamples + biogroup
                              (['TestingIndividualSno.uid', 'TestingIndividualSno.specimen',
                                'TestingIndividualSno.full_name'], 6),  # all care
                              ])
    def test_invalidation_scope_integrated_all(self, testapp, invalidation_scope_workbook, diff, expected):
        """ Integrated test that simulates many diffs checking that the right # of items were invalidated. """
        groups, sources, samples = invalidation_scope_workbook
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in groups + sources + samples]
        secondary = {obj['@id'] for obj in groups + sources + samples}
        self.runtest(testapp, diff, invalidated, secondary, expected)

    def test_invalidation_scope_default_diff(self, testapp, invalidation_scope_workbook):
        """ Integrated test verifies that default_diff is added correctly
            ie: diff should not trigger invalidation, but default_diff will
        """
        groups, sources, samples = invalidation_scope_workbook
        invalidated = [(obj['@id'], obj['@type'][0]) for obj in groups + sources + samples]
        secondary = {obj['@id'] for obj in groups + sources + samples}
        self.runtest(testapp, ['TestingBiosourceSno.identifier'], invalidated, secondary, 1)
