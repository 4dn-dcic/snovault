"""
Unit tests for snovault.schema_formats -- the pure regex validators used to
recognize UUIDs and accessions. These are correctness-sensitive (they gate
identity resolution) and previously had no direct unit test.
"""
import uuid as uuid_module

import pytest

from ..schema_formats import is_uuid, is_accession
from ..server_defaults import GET_ACCESSION_PREFIX, ACCESSION_TEST_PREFIX


pytestmark = [pytest.mark.unit]


class TestIsUuid:

    def test_canonical_uuid(self):
        assert is_uuid(str(uuid_module.uuid4())) is True

    def test_uuid_without_dashes(self):
        # Python's UUID (and this checker) tolerate the dash-free form.
        assert is_uuid(str(uuid_module.uuid4()).replace('-', '')) is True

    def test_uuid_with_braces(self):
        assert is_uuid('{' + str(uuid_module.uuid4()) + '}') is True

    def test_uuid_is_case_insensitive(self):
        u = str(uuid_module.uuid4()).upper()
        assert is_uuid(u) is True

    def test_too_short_is_rejected(self):
        assert is_uuid('c78da883') is False

    def test_empty_is_rejected(self):
        assert is_uuid('') is False

    def test_non_hex_is_rejected(self):
        assert is_uuid('not-a-uuid-value-here') is False

    def test_trailing_garbage_still_matches(self):
        # Documents a KNOWN laxness: the checker uses re.match (not fullmatch),
        # so trailing characters after a valid prefix are not rejected. Pinning
        # this so a future tightening to fullmatch is a deliberate, visible change.
        assert is_uuid(str(uuid_module.uuid4()) + 'ZZZ') is True


class TestIsAccession:

    def test_real_prefix_accession(self):
        # Nine [1-9A-Z] characters following the project accession prefix.
        assert is_accession(GET_ACCESSION_PREFIX() + 'ABC123XYZ') is True

    def test_test_prefix_numeric_suffix(self):
        # TST + 2-letter code + 4 digits + 3 digits.
        assert is_accession(ACCESSION_TEST_PREFIX + 'BS1234567') is True

    def test_test_prefix_alpha_suffix(self):
        # TST + 2-letter code + 4 digits + 3 uppercase letters.
        assert is_accession(ACCESSION_TEST_PREFIX + 'ES1234ABC') is True

    def test_unknown_string_is_not_accession(self):
        assert is_accession('HELLO') is False

    def test_lowercase_is_not_accession(self):
        assert is_accession(GET_ACCESSION_PREFIX().lower() + 'abc123xyz') is False

    def test_test_prefix_with_unknown_code_is_rejected(self):
        # 'ZZ' is not in ACCESSION_TEST_CODES.
        assert is_accession(ACCESSION_TEST_PREFIX + 'ZZ1234567') is False

    def test_wrong_length_is_rejected(self):
        assert is_accession(GET_ACCESSION_PREFIX() + 'ABC12') is False
