"""Mi Home BLE login handshake for Xiaomi BLE scales."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import hmac
import logging
import secrets

from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic

from .crypto import SessionKeys, derive_login_keys, hmac_sha256
from .protocol import (
    AUTH_ERRORS,
    AVDTP,
    CFM_LOGIN_ERR,
    CFM_LOGIN_OK,
    CMD_LOGIN,
    CMD_SEND_INFO,
    CMD_SEND_KEY,
    RCV_OK,
    RCV_RDY,
    UPNP,
    FrameSequenceError,
    ParcelReassembler,
)

_LOGGER = logging.getLogger(__name__)


class AuthError(RuntimeError):
    """Base error during Mi Home v2 BLE login."""


class DisconnectedError(AuthError):
    """BLE connection dropped during login or streaming."""


class BadTokenError(AuthError):
    """HMAC verification failed — token is wrong or has rotated."""


class NotifyHub:
    """Bridges BleakClient notify callbacks to per-UUID asyncio.Queues."""

    def __init__(self) -> None:
        self.queues: dict[str, asyncio.Queue[bytes | None]] = {}
        self._disconnected = False

    def queue(self, uuid: str) -> asyncio.Queue[bytes | None]:
        if uuid not in self.queues:
            q: asyncio.Queue[bytes | None] = asyncio.Queue()
            if self._disconnected:
                q.put_nowait(None)
            self.queues[uuid] = q
        return self.queues[uuid]

    def discard_queues(self, *uuids: str) -> None:
        """Stop queuing notifications and release queues for the given UUIDs."""
        for uuid in uuids:
            self.queues.pop(uuid, None)

    def mark_disconnected(self) -> None:
        """Wake all waiting queues with a None sentinel on BLE disconnect."""
        self._disconnected = True
        for q in self.queues.values():
            q.put_nowait(None)

    def make_callback(
        self, uuid: str
    ) -> Callable[[BleakGATTCharacteristic, bytearray], None]:
        self.queue(uuid)

        def cb(_char: BleakGATTCharacteristic, data: bytearray) -> None:
            if (q := self.queues.get(uuid)) is not None:
                q.put_nowait(bytes(data))

        return cb


async def _wait(
    q: asyncio.Queue[bytes | None], timeout: float, label: str = "notification"
) -> bytes:
    try:
        data = await asyncio.wait_for(q.get(), timeout=timeout)
    except TimeoutError as exc:
        raise AuthError(f"timeout waiting for {label}") from exc
    if data is None:
        raise DisconnectedError(f"device disconnected while waiting for {label}")
    return data


async def _write(client: BleakClient, uuid: str, data: bytes) -> None:
    _LOGGER.debug("-> %s %s", uuid[:8], data.hex(" "))
    await client.write_gatt_char(uuid, data, response=False)


async def _write_parcel(
    client: BleakClient,
    uuid: str,
    data: bytes,
    chunk_size: int = 18,
    frame_delay: float = 0.05,
) -> None:
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        n = i // chunk_size + 1
        framed = bytes([n & 0xFF, (n >> 8) & 0xFF]) + chunk
        _LOGGER.debug("-> %s parcel frame %d (%dB)", uuid[:8], n, len(chunk))
        await client.write_gatt_char(uuid, framed, response=False)
        await asyncio.sleep(frame_delay)


async def _recv_multiframe(
    client: BleakClient, q: asyncio.Queue[bytes | None], timeout: float = 5.0
) -> bytes:
    reassembler = ParcelReassembler()
    first = await _wait(q, timeout, "multiframe header")
    if len(first) < 6 or first[:3] != b"\x00\x00\x00":
        raise AuthError(f"unexpected first frame: {first.hex()}")
    ack, msg = reassembler.feed(first)
    if ack is not None:
        await _write(client, AVDTP, ack)
    if msg is not None:
        return msg
    while True:
        frame = await _wait(q, timeout, "multiframe data")
        try:
            ack, msg = reassembler.feed(frame)
        except FrameSequenceError as exc:
            raise AuthError(str(exc)) from exc
        if ack is not None:
            await _write(client, AVDTP, ack)
        if msg is not None:
            return msg


async def login(
    client: BleakClient, token: bytes, hub: NotifyHub, timeout: float = 5.0
) -> SessionKeys:
    """Execute Mi Home v2 LOGIN."""
    if len(token) != 12:
        raise AuthError(f"token must be 12 bytes, got {len(token)}")

    avdtp_q = hub.queue(AVDTP)
    upnp_q = hub.queue(UPNP)

    app_rand = secrets.token_bytes(16)

    await _write(client, UPNP, CMD_LOGIN)
    await _write(client, AVDTP, CMD_SEND_KEY)
    rdy = await _wait(avdtp_q, timeout, "RCV_RDY after CMD_SEND_KEY")
    if rdy != RCV_RDY:
        raise AuthError(f"expected RCV_RDY after CMD_SEND_KEY, got {rdy.hex()}")
    await _write_parcel(client, AVDTP, app_rand)
    ok = await _wait(avdtp_q, timeout, "RCV_OK after app_rand")
    if ok != RCV_OK:
        raise AuthError(f"expected RCV_OK, got {ok.hex()}")

    dev_rand = await _recv_multiframe(client, avdtp_q, timeout)
    if len(dev_rand) != 16:
        raise AuthError(f"bad dev rand len {len(dev_rand)}")

    remote_info = await _recv_multiframe(client, avdtp_q, timeout)
    if len(remote_info) != 32:
        raise AuthError(f"bad remote_info len {len(remote_info)}")

    keys = derive_login_keys(token, app_rand, dev_rand)
    expected_remote = hmac_sha256(keys.dev_key, dev_rand + app_rand)
    if not hmac.compare_digest(remote_info, expected_remote):
        raise BadTokenError("HMAC mismatch — token is wrong or has rotated")

    our_info = hmac_sha256(keys.app_key, app_rand + dev_rand)

    await _write(client, AVDTP, CMD_SEND_INFO)
    rdy = await _wait(avdtp_q, timeout, "RCV_RDY for SEND_INFO")
    if rdy != RCV_RDY:
        raise AuthError(f"expected RCV_RDY for SEND_INFO, got {rdy.hex()}")
    await _write_parcel(client, AVDTP, our_info)
    ok = await _wait(avdtp_q, timeout, "RCV_OK for SEND_INFO")
    if ok != RCV_OK:
        raise AuthError(f"expected RCV_OK for SEND_INFO, got {ok.hex()}")

    result = await _wait(upnp_q, timeout, "CFM_LOGIN result")
    if result == CFM_LOGIN_ERR:
        raise AuthError(f"device rejected login (CFM_LOGIN_ERR: {result.hex()})")
    if result in AUTH_ERRORS:
        raise AuthError(f"device returned auth error code: {result.hex()}")
    if result != CFM_LOGIN_OK:
        raise AuthError(f"unexpected login response: {result.hex()}")

    hub.discard_queues(UPNP, AVDTP)
    return keys
