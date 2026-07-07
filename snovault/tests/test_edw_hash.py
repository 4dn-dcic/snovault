import pytest

from ..edw_hash import EDWHash


pytestmark = [pytest.mark.setone, pytest.mark.working]


TEST_HASHES = {
    "test": "Jnh+8wNnELksNFVbxkya8RDrxJNL13dUWTXhp5DCx/quTM2/cYn7azzl2Uk3I2zc",
    "test2": "sh33L5uQeLr//jJULb7mAnbVADkkWZrgcXx97DCacueGtEU5G2HtqUv73UTS0EI0",
    "testing100" * 10: "5rznDSIcDPd/9rjom6P/qkJGtJSV47y/u5+KlkILROaqQ6axhEyVIQTahuBYerLG",
}


@pytest.mark.parametrize(('password', 'pwhash'), TEST_HASHES.items())
def test_edw_hash(password, pwhash):
    assert EDWHash.hash(password) == pwhash


def test_edw_hash_verify_roundtrip():
    assert EDWHash.verify('test', EDWHash.hash('test')) is True
    assert EDWHash.verify('wrong', EDWHash.hash('test')) is False


def test_edw_hash_bytes_and_str_secrets_agree():
    assert EDWHash.hash(b'test') == EDWHash.hash('test')


def test_edw_hash_password_too_long():
    # salted = salt_before + secret + salt_after + NUL must fit in salt_base
    max_secret_len = (len(EDWHash.salt_base)
                      - len(EDWHash.salt_before)
                      - len(EDWHash.salt_after)
                      - 1)
    assert max_secret_len == 454
    EDWHash.hash('x' * max_secret_len)  # exactly at the limit is fine
    with pytest.raises(ValueError, match='Password too long'):
        EDWHash.hash('x' * (max_secret_len + 1))
