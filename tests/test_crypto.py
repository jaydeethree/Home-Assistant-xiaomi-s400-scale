"""Tests for Mi Home v2 BLE crypto helpers."""

from __future__ import annotations

import hashlib
import hmac

from custom_components.xiaomi_s400_scale.crypto import (
    decrypt_cmtp,
    derive_login_keys,
    hmac_sha256,
)

from .conftest import TEST_TOKEN_BYTES, encrypt_cmtp_message, make_session_keys


def test_derive_login_keys_and_repr_redaction() -> None:
    """Test deterministic HKDF derivation and that SessionKeys hides secret material."""
    app_rand = bytes(range(16))
    dev_rand = bytes(range(16, 32))
    keys1 = derive_login_keys(TEST_TOKEN_BYTES, app_rand, dev_rand)
    keys2 = derive_login_keys(TEST_TOKEN_BYTES, app_rand, dev_rand)

    assert len(keys1.dev_key) == 16
    assert len(keys1.app_key) == 16
    assert len(keys1.dev_iv) == 4
    assert len(keys1.app_iv) == 4
    assert keys1.dev_key == keys2.dev_key
    assert keys1.app_key == keys2.app_key
    assert keys1.dev_iv == keys2.dev_iv
    assert keys1.app_iv == keys2.app_iv
    assert keys1.dev_key != keys1.app_key

    # Ensure repr() never leaks session keys or IVs into debug logs or tracebacks
    rendered = repr(keys1)
    assert rendered == "SessionKeys()"
    assert keys1.dev_key.hex() not in rendered
    assert keys1.app_key.hex() not in rendered


def test_hmac_sha256() -> None:
    """Test HMAC-SHA256 helper matches stdlib hmac."""
    key = b"secret-key-12345"
    msg = b"payload-bytes"
    expected = hmac.new(key, msg, hashlib.sha256).digest()
    assert hmac_sha256(key, msg) == expected


def test_decrypt_cmtp_roundtrip_and_errors() -> None:
    """Test AES-CCM CMTP decryption for valid, short, tampered, and mismatched frames."""
    keys = make_session_keys()
    plaintext = b"\x12\x20\x67\x00\x07\x08\x01\x00\x01\x01\x00\x05\xa0728,1"
    encrypted = encrypt_cmtp_message(keys, plaintext, iteration=42)

    assert decrypt_cmtp(keys, encrypted) == plaintext

    # Inputs shorter than 2B iter + 4B tag return None
    assert decrypt_cmtp(keys, b"") is None
    assert decrypt_cmtp(keys, b"\x01\x00\xaa\xbb\xcc") is None

    # Tampered tag returns None
    tampered = bytearray(encrypted)
    tampered[-1] ^= 0xFF
    assert decrypt_cmtp(keys, bytes(tampered)) is None

    # Tampered iteration counter (nonce mismatch) returns None
    wrong_iter = bytearray(encrypted)
    wrong_iter[0] ^= 0x01
    assert decrypt_cmtp(keys, bytes(wrong_iter)) is None
