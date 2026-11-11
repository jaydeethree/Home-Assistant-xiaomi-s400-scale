"""Shared fixtures and helpers for Xiaomi S400 Scale tests."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import patch

from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
import pytest

from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import BluetoothServiceInfoBleak

from custom_components.xiaomi_s400_scale.crypto import SessionKeys, derive_login_keys
from custom_components.xiaomi_s400_scale.protocol import MIBEACON_SERVICE_UUID

TEST_ADDRESS = "AA:BB:CC:11:22:33"
TEST_SOURCE = "AA:BB:CC:DD:EE:01"
TEST_NAME = "Mijia Scale S400 2233"
TEST_TOKEN_HEX = "00112233445566778899aabb"
TEST_TOKEN_BYTES = bytes.fromhex(TEST_TOKEN_HEX)

# MiBeacon v5 service data for Xiaomi Scale S400 (product_id=0x30D9, cnt=22, dummy reversed MAC)
S400_MIBEACON_BYTES = bytes.fromhex("1059d93016332211ccbbaa")

# Non-S400 MiBeacon v5 service data (product_id=0x0001)
OTHER_MIBEACON_BYTES = bytes.fromhex("1059010016332211ccbbaa")

# Real CMTP decrypted payloads captured from hardware
REAL_LIVE_PLAINTEXT = bytes.fromhex("122067000708010001010005a03732382c31")
REAL_FINAL_PLAINTEXT = bytes.fromhex(
    "5d2068000708020001020050a0302c302c312c3732382c302c322c"
    "313739343334323237392c302c302c302c302c302c302c302c302c"
    "302c302c302c302c302c302c302c302c302c302c302c302c302c30"
    "2c302c353338302c34393430"
)


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    enable_custom_integrations: None,
    enable_bluetooth: None,
) -> Generator[None]:
    """Enable custom integrations and mock Bluetooth stack for all tests."""
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.SETTLE_DELAY_SECONDS", 0.0
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.SETTLE_DELAY_SECONDS", 0.0
        ),
    ):
        yield


def make_ble_device(
    address: str = TEST_ADDRESS,
    name: str | None = TEST_NAME,
) -> BLEDevice:
    """Create a bleak BLEDevice for tests."""
    return BLEDevice(address=address, name=name, details=None)


def make_service_info(
    *,
    address: str = TEST_ADDRESS,
    name: str = TEST_NAME,
    service_data: dict[str, bytes] | None = None,
    connectable: bool = True,
    rssi: int = -64,
    source: str = TEST_SOURCE,
    time_mono: float | None = None,
) -> BluetoothServiceInfoBleak:
    """Create a BluetoothServiceInfoBleak matching a Xiaomi S400 Scale."""
    if service_data is None:
        service_data = {MIBEACON_SERVICE_UUID: S400_MIBEACON_BYTES}
    adv_time = bluetooth.MONOTONIC_TIME() if time_mono is None else time_mono
    device = make_ble_device(address=address, name=name or None)
    adv = AdvertisementData(
        local_name=name or None,
        manufacturer_data={},
        service_data=service_data,
        service_uuids=list(service_data),
        tx_power=None,
        rssi=rssi,
        platform_data=(),
    )
    return BluetoothServiceInfoBleak(
        name=name,
        address=address,
        rssi=rssi,
        manufacturer_data={},
        service_data=service_data,
        service_uuids=list(service_data),
        source=source,
        device=device,
        advertisement=adv,
        connectable=connectable,
        time=adv_time,
        tx_power=None,
    )


def make_session_keys(token: bytes = TEST_TOKEN_BYTES) -> SessionKeys:
    """Create deterministic SessionKeys for tests."""
    app_rand = bytes(range(16))
    dev_rand = bytes(range(16, 32))
    return derive_login_keys(token, app_rand, dev_rand)


def encrypt_cmtp_message(
    keys: SessionKeys,
    plaintext: bytes,
    iteration: int = 1,
) -> bytes:
    """Encrypt a plaintext payload into the device->app CMTP wire format."""
    it = iteration.to_bytes(2, "little")
    nonce = keys.dev_iv + bytes(4) + it + bytes(2)
    ct = keys.dev_cipher.encrypt(nonce, plaintext, None)
    return it + ct


def make_parcel_frames(payload: bytes, chunk_size: int = 18) -> list[bytes]:
    """Split a payload into a Mi Home 2-byte-framed parcel sequence (header + chunks)."""
    if not payload:
        return [bytes.fromhex("000000000000")]
    chunks = [payload[i : i + chunk_size] for i in range(0, len(payload), chunk_size)]
    count = len(chunks)
    header = b"\x00\x00\x00\x00" + count.to_bytes(2, "little")
    frames = [header]
    for idx, chunk in enumerate(chunks, start=1):
        frames.append(idx.to_bytes(2, "little") + chunk)
    return frames
