"""Tests for Mi Home v2 BLE login handshake and NotifyHub."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock, patch

from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic
import pytest

from custom_components.xiaomi_s400_scale.auth import (
    AuthError,
    BadTokenError,
    DisconnectedError,
    NotifyHub,
    login,
)
from custom_components.xiaomi_s400_scale.crypto import derive_login_keys, hmac_sha256
from custom_components.xiaomi_s400_scale.protocol import (
    AVDTP,
    CFM_LOGIN_ERR,
    CFM_LOGIN_OK,
    CMD_LOGIN,
    CMD_SEND_INFO,
    CMD_SEND_KEY,
    RCV_OK,
    RCV_RDY,
    UPNP,
)

from .conftest import TEST_TOKEN_BYTES, make_parcel_frames


async def test_notify_hub_queues_and_disconnect() -> None:
    """Test NotifyHub routing, discard_queues, and disconnect sentinel propagation."""
    hub = NotifyHub()
    char = MagicMock(spec=BleakGATTCharacteristic)
    cb = hub.make_callback(AVDTP)

    cb(char, bytearray(b"hello"))
    assert await hub.queue(AVDTP).get() == b"hello"

    # Discarding removes queue and causes callback to become a no-op
    hub.discard_queues(AVDTP)
    assert AVDTP not in hub.queues
    cb(char, bytearray(b"ignored"))
    assert AVDTP not in hub.queues

    # Marking disconnected wakes existing queues and pre-populates future queues
    q_existing = hub.queue(UPNP)
    hub.mark_disconnected()
    assert await q_existing.get() is None
    q_future = hub.queue(AVDTP)
    assert await q_future.get() is None


def _build_handshake_client(
    hub: NotifyHub,
    *,
    device_token: bytes = TEST_TOKEN_BYTES,
    dev_rand: bytes = bytes(range(16, 32)),
    final_upnp: bytes | None = CFM_LOGIN_OK,
    custom_on_write: Callable[[str, bytes], bool] | None = None,
) -> BleakClient:
    """Create a mock BleakClient that speaks the device side of Mi Home v2 login."""
    client = MagicMock(spec=BleakClient)
    phase = "init"
    app_rand = b""
    dev_frames: list[bytes] = []
    rem_frames: list[bytes] = []
    app_buf = bytearray()
    info_frames = 0

    async def _write_gatt_char(uuid: str, data: bytes, response: bool = False) -> None:
        nonlocal phase, app_rand, dev_frames, rem_frames, info_frames
        assert response is False
        if custom_on_write is not None and custom_on_write(uuid, data):
            return

        avdtp_q = hub.queue(AVDTP)
        upnp_q = hub.queue(UPNP)

        if uuid == UPNP and data == CMD_LOGIN:
            phase = "await_send_key"
            return

        if uuid == AVDTP:
            if phase == "await_send_key" and data == CMD_SEND_KEY:
                phase = "recv_app_rand"
                avdtp_q.put_nowait(RCV_RDY)
                return

            if phase == "recv_app_rand":
                # 1 frame for 16B app_rand
                app_rand = bytes(data[2:])
                avdtp_q.put_nowait(RCV_OK)
                # Device sends dev_rand parcel header; waits for RCV_RDY
                all_dev_frames = make_parcel_frames(dev_rand)
                dev_frames = all_dev_frames[1:]
                phase = "send_dev_rand"
                avdtp_q.put_nowait(all_dev_frames[0])
                return

            if phase == "send_dev_rand" and data == RCV_RDY:
                for frame in dev_frames:
                    avdtp_q.put_nowait(frame)
                phase = "ack_dev_rand"
                return

            if phase == "ack_dev_rand" and data == RCV_OK:
                keys = derive_login_keys(device_token, app_rand, dev_rand)
                remote_info = hmac_sha256(keys.dev_key, dev_rand + app_rand)
                all_rem_frames = make_parcel_frames(remote_info)
                rem_frames = all_rem_frames[1:]
                phase = "send_remote_info"
                avdtp_q.put_nowait(all_rem_frames[0])
                return

            if phase == "send_remote_info" and data == RCV_RDY:
                for frame in rem_frames:
                    avdtp_q.put_nowait(frame)
                phase = "ack_remote_info"
                return

            if phase == "ack_remote_info" and data == RCV_OK:
                phase = "await_send_info"
                return

            if phase == "await_send_info" and data == CMD_SEND_INFO:
                phase = "recv_our_info"
                info_frames = 0
                avdtp_q.put_nowait(RCV_RDY)
                return

            if phase == "recv_our_info":
                app_buf.extend(data[2:])
                info_frames += 1
                if info_frames == 2:
                    avdtp_q.put_nowait(RCV_OK)
                    if final_upnp is not None:
                        upnp_q.put_nowait(final_upnp)
                    phase = "done"

    client.write_gatt_char.side_effect = _write_gatt_char
    return client


async def test_login_success() -> None:
    """Test full Mi Home v2 BLE login handshake succeeds and discards auth queues."""
    hub = NotifyHub()
    client = _build_handshake_client(hub)

    with patch("custom_components.xiaomi_s400_scale.auth.asyncio.sleep"):
        keys = await login(client, TEST_TOKEN_BYTES, hub, timeout=1.0)

    assert len(keys.dev_key) == 16
    assert UPNP not in hub.queues
    assert AVDTP not in hub.queues


async def test_login_bad_token_raises_bad_token_error() -> None:
    """Test wrong BLE token triggers BadTokenError on device HMAC mismatch."""
    hub = NotifyHub()
    wrong_device_token = bytes.fromhex("ff" * 12)
    client = _build_handshake_client(hub, device_token=wrong_device_token)

    with (
        patch("custom_components.xiaomi_s400_scale.auth.asyncio.sleep"),
        pytest.raises(BadTokenError, match="HMAC mismatch"),
    ):
        await login(client, TEST_TOKEN_BYTES, hub, timeout=1.0)


@pytest.mark.parametrize(
    ("final_code", "expected_msg"),
    [
        (CFM_LOGIN_ERR, "CFM_LOGIN_ERR"),
        (bytes.fromhex("e1000000"), "auth error code"),
        (bytes.fromhex("99000000"), "unexpected login response"),
    ],
)
async def test_login_device_rejection_codes(
    final_code: bytes, expected_msg: str
) -> None:
    """Test device rejection opcodes on UPNP raise AuthError."""
    hub = NotifyHub()
    client = _build_handshake_client(hub, final_upnp=final_code)

    with (
        patch("custom_components.xiaomi_s400_scale.auth.asyncio.sleep"),
        pytest.raises(AuthError, match=expected_msg),
    ):
        await login(client, TEST_TOKEN_BYTES, hub, timeout=1.0)


async def test_login_validation_timeout_and_disconnect_errors() -> None:
    """Test invalid token length, timeout, disconnect, and malformed frame errors."""
    hub = NotifyHub()
    client = MagicMock(spec=BleakClient)

    with pytest.raises(AuthError, match="token must be 12 bytes"):
        await login(client, b"short", hub, timeout=0.05)

    # Timeout waiting for RCV_RDY
    with pytest.raises(AuthError, match="timeout waiting for RCV_RDY"):
        await login(client, TEST_TOKEN_BYTES, hub, timeout=0.02)

    # Disconnect sentinel raises DisconnectedError
    hub_disc = NotifyHub()
    hub_disc.mark_disconnected()
    with pytest.raises(DisconnectedError, match="device disconnected"):
        await login(client, TEST_TOKEN_BYTES, hub_disc, timeout=0.1)

    # Bad RCV_RDY opcode
    hub_bad_rdy = NotifyHub()

    def _bad_rdy(uuid: str, data: bytes) -> bool:
        if uuid == AVDTP and data == CMD_SEND_KEY:
            hub_bad_rdy.queue(AVDTP).put_nowait(b"\xff\xff")
            return True
        return False

    client_bad_rdy = _build_handshake_client(hub_bad_rdy, custom_on_write=_bad_rdy)
    with pytest.raises(AuthError, match="expected RCV_RDY after CMD_SEND_KEY"):
        await login(client_bad_rdy, TEST_TOKEN_BYTES, hub_bad_rdy, timeout=0.5)

    # Wrong dev_rand length (8 bytes instead of 16)
    hub_short_rand = NotifyHub()
    client_short_rand = _build_handshake_client(hub_short_rand, dev_rand=b"\x01" * 8)
    with (
        patch("custom_components.xiaomi_s400_scale.auth.asyncio.sleep"),
        pytest.raises(AuthError, match="bad dev rand len 8"),
    ):
        await login(client_short_rand, TEST_TOKEN_BYTES, hub_short_rand, timeout=0.5)

    # Malformed multiframe header and out-of-sequence multiframe data
    hub_bad_hdr = NotifyHub()

    def _bad_hdr(uuid: str, data: bytes) -> bool:
        if uuid == AVDTP and data[:2] == b"\x01\x00":
            hub_bad_hdr.queue(AVDTP).put_nowait(RCV_OK)
            hub_bad_hdr.queue(AVDTP).put_nowait(b"\x99\x99")
            return True
        return False

    client_bad_hdr = _build_handshake_client(hub_bad_hdr, custom_on_write=_bad_hdr)
    with (
        patch("custom_components.xiaomi_s400_scale.auth.asyncio.sleep"),
        pytest.raises(AuthError, match="unexpected first frame"),
    ):
        await login(client_bad_hdr, TEST_TOKEN_BYTES, hub_bad_hdr, timeout=0.5)

    hub_seq = NotifyHub()

    def _bad_seq(uuid: str, data: bytes) -> bool:
        if uuid == AVDTP and data == RCV_RDY:
            # Skip frame 1 and send frame 2
            hub_seq.queue(AVDTP).put_nowait(b"\x02\x00" + b"x" * 16)
            return True
        return False

    client_seq = _build_handshake_client(hub_seq, custom_on_write=_bad_seq)
    with (
        patch("custom_components.xiaomi_s400_scale.auth.asyncio.sleep"),
        pytest.raises(AuthError, match="frame out of sequence"),
    ):
        await login(client_seq, TEST_TOKEN_BYTES, hub_seq, timeout=0.5)
