"""Mi Home BLE protocol constants and payload parser for Xiaomi S400 Scale."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

# Auth service (0xFE95) characteristic UUIDs
UPNP = "00000010-0000-1000-8000-00805f9b34fb"   # client -> device commands
AVDTP = "00000019-0000-1000-8000-00805f9b34fb"  # bidirectional auth data
AVCTP = "00000017-0000-1000-8000-00805f9b34fb"

# Encrypted command channel (post-login)
VEND1A = "0000001a-0000-1000-8000-00805f9b34fb"  # app -> device encrypted
CMTP = "0000001b-0000-1000-8000-00805f9b34fb"    # device -> app encrypted
VEND1C = "0000001c-0000-1000-8000-00805f9b34fb"

# MiBeacon advertisement service
MIBEACON_SERVICE_UUID = "0000fe95-0000-1000-8000-00805f9b34fb"

# Commands written to UPNP (4-byte little-endian opcode)
CMD_LOGIN = bytes.fromhex("24000000")

# Framing headers written to AVDTP (3B fixed + 1B type + 2B frame_count_LE)
CMD_SEND_KEY = bytes.fromhex("0000000b0100")
CMD_SEND_INFO = bytes.fromhex("0000000a0200")

# Receiver acks
RCV_RDY = bytes.fromhex("00000101")
RCV_OK = bytes.fromhex("00000100")

# Auth result codes (on UPNP)
CFM_LOGIN_OK = bytes.fromhex("21000000")
CFM_LOGIN_ERR = bytes.fromhex("23000000")
AUTH_ERRORS = {
    bytes.fromhex("e0000000"),
    bytes.fromhex("e1000000"),
    bytes.fromhex("e2000000"),
    bytes.fromhex("e3000000"),
}


@dataclass
class ScaleEvent:
    """Either a 'live' frame (weight updating) or 'final' (full measurement dump)."""

    type: Literal["live", "final"]
    weight_kg: float | None = None
    stable: bool = False
    impedance_ohm: float | None = None
    impedance_low_ohm: float | None = None
    profile_id: int | None = None
    timestamp: int | None = None
    raw_plaintext: bytes | None = None


def parse_cmtp_payload(pt: bytes) -> dict[str, Any] | None:
    """Parse a decrypted CMTP plaintext into structured fields.

    Format (observed on yunmai.scales.ms104 / S400):
        LL 20 CC 00 07 08 PP 00 01 PP 00 SS a0 <ascii_csv>
    Live (2 fields): "weight_x10,stable_flag"
    Final (32 fields): "0,0,profile,weight_x10,stable,2,unix_ts,0..0,imp_x10,imp_low_x10"
    """
    idx = pt.find(b"\xa0")
    if idx < 0:
        return None
    s = pt[idx + 1 :].decode("ascii", errors="replace").rstrip("\x00").strip()
    parts = s.split(",")

    def _i(p: str) -> int | None:
        return int(p) if p.lstrip("-").isdigit() else None

    if len(parts) == 2:
        w = _i(parts[0])
        return {
            "type": "live",
            "weight_kg": w / 10 if w is not None else None,
            "stable": parts[1] == "1",
        }
    if len(parts) >= 8:
        w = _i(parts[3])
        imp = _i(parts[-2])
        imp_low = _i(parts[-1])
        return {
            "type": "final",
            "profile_id": _i(parts[2]),
            "weight_kg": w / 10 if w is not None else None,
            "stable": parts[4] == "1",
            "timestamp": _i(parts[6]),
            "impedance_ohm": imp / 10 if imp is not None else None,
            "impedance_low_ohm": imp_low / 10 if imp_low is not None else None,
        }
    return None
