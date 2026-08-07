import pytest

from dcicutils.qa_utils import notice_pytest_fixtures

from ..loadxl import load_all_gen


pytestmark = [pytest.mark.setone, pytest.mark.working]


COLLECTION_URL = '/testing-post-put-patch-sno/'
ITEM_TYPE = 'testing_post_put_patch_sno'

EXISTING_ITEM = {
    'uuid': 'a8a1e6dc-2ea2-4b5b-8d0d-0d1b06e3cd44',
    'required': 'original required value',
}


@pytest.fixture
def loadxl_item(testapp, external_tx):
    notice_pytest_fixtures(external_tx)
    res = testapp.post_json(COLLECTION_URL, EXISTING_ITEM, status=201)
    return res.json['@graph'][0]['@id']


def run_load_all_gen(testapp, store, **kwargs):
    """ Drains the load_all_gen generator, returning its (decoded) yielded lines.

        noset_last_modified because the testing item types here do not mixin last_modified.
    """
    return [line.decode('utf-8') if isinstance(line, bytes) else line
            for line in load_all_gen(testapp, store, None, from_json=True,
                                     noset_last_modified=True, **kwargs)]


def test_loadxl_validate_only_with_patch_only_does_not_persist(testapp, loadxl_item):
    """ validate_only + patch_only skips round one entirely, so every item is handled by the
        round two PATCH. That PATCH must not write anything - previously it did, while also
        sending skip_links, which is exactly the "claims validation-only but persists" case.
    """
    store = {ITEM_TYPE: [{'uuid': EXISTING_ITEM['uuid'],
                          'required': 'mutated by validate_only run',
                          'simple1': 'mutated by validate_only run'}]}
    output = run_load_all_gen(testapp, store, overwrite=True, validate_only=True, patch_only=True,
                              skip_links=True)

    assert not any(line.startswith('ERROR:') for line in output)
    assert any(line.startswith('CHECK:') for line in output)
    assert not any(line.startswith('PATCH:') for line in output)

    # nothing was written
    res = testapp.get(loadxl_item)
    assert res.json['required'] == EXISTING_ITEM['required']
    assert res.json['simple1'] == 'simple1 default'
    revisions = testapp.get(loadxl_item + '@@revision-history').json['revisions']
    assert len(revisions) == 1


def test_loadxl_validate_only_without_skip_links_also_does_not_persist(testapp, loadxl_item):
    """ Same boundary, without skip_links: validate_only must still be non-persisting. """
    store = {ITEM_TYPE: [{'uuid': EXISTING_ITEM['uuid'],
                          'required': 'mutated by validate_only run'}]}
    output = run_load_all_gen(testapp, store, overwrite=True, validate_only=True, patch_only=True)

    assert not any(line.startswith('ERROR:') for line in output)
    assert testapp.get(loadxl_item).json['required'] == EXISTING_ITEM['required']


def test_loadxl_normal_patch_round_still_persists(testapp, loadxl_item):
    """ The ordinary (not validate_only) load path is unchanged and still writes. """
    store = {ITEM_TYPE: [{'uuid': EXISTING_ITEM['uuid'],
                          'required': 'updated by loadxl',
                          'simple1': 'updated by loadxl'}]}
    output = run_load_all_gen(testapp, store, overwrite=True, patch_only=True)

    assert not any(line.startswith('ERROR:') for line in output)
    assert any(line.startswith('PATCH:') for line in output)
    res = testapp.get(loadxl_item)
    assert res.json['required'] == 'updated by loadxl'
    assert res.json['simple1'] == 'updated by loadxl'


def test_loadxl_validate_only_creates_nothing(testapp, external_tx):
    """ validate_only over an item that does not exist yet validates via check_only POST and
        creates nothing.
    """
    notice_pytest_fixtures(external_tx)
    uuid = 'b7b1e6dc-2ea2-4b5b-8d0d-0d1b06e3cd45'
    store = {ITEM_TYPE: [{'uuid': uuid, 'required': 'never written'}]}
    output = run_load_all_gen(testapp, store, overwrite=True, validate_only=True, skip_links=True)

    assert not any(line.startswith('ERROR:') for line in output)
    assert any(line.startswith('CHECK:') for line in output)
    testapp.get(f'/{uuid}', status=404)
