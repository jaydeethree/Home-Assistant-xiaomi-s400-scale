"""Tests for Mi Home parcel reassembly and S400 CMTP payload parsing."""

from __future__ import annotations

import pytest

from custom_components.xiaomi_s400_scale.protocol import (
    FINAL_CSV_FIELD_COUNT,
    RCV_OK,
    RCV_RDY,
    FrameSequenceError,
    ParcelReassembler,
    parse_cmtp_payload,
)

from .conftest import REAL_FINAL_PLAINTEXT, REAL_LIVE_PLAINTEXT, make_parcel_frames


def test_parcel_reassembler_single_and_multiframe() -> None:
    """Test reassembling single-frame, multi-frame, and empty parcels."""
    reassembler = ParcelReassembler()

    # Data before any header is ignored
    assert reassembler.feed(b"\x01\x00hello") == (None, None)

    # Zero-frame header immediately completes with empty bytes
    assert reassembler.feed(bytes.fromhex("000000000000")) == (RCV_OK, b"")

    # Multi-frame message (40 bytes -> 3 chunks of 18, 18, 4)
    payload = bytes(range(40))
    frames = make_parcel_frames(payload, chunk_size=18)
    assert len(frames) == 4

    assert reassembler.feed(frames[0]) == (RCV_RDY, None)
    assert reassembler.feed(frames[1]) == (None, None)
    # Duplicate frame 1 is ignored without corrupting state
    assert reassembler.feed(frames[1]) == (None, None)
    assert reassembler.feed(frames[2]) == (None, None)
    assert reassembler.feed(frames[3]) == (RCV_OK, payload)
    assert reassembler.expected == 0


def test_parcel_reassembler_out_of_sequence_resets_and_raises() -> None:
    """Test skipped frame number raises FrameSequenceError and resets state."""
    reassembler = ParcelReassembler()
    frames = make_parcel_frames(b"A" * 30, chunk_size=10)

    assert reassembler.feed(frames[0]) == (RCV_RDY, None)
    with pytest.raises(FrameSequenceError, match="expected 1, got 2"):
        reassembler.feed(frames[2])

    assert reassembler.expected == 0
    assert reassembler.next_frame == 1


def test_parse_cmtp_live_payload_real_and_edge_cases() -> None:
    """Test parsing real S400 live frames and rejecting malformed live frames."""
    event = parse_cmtp_payload(REAL_LIVE_PLAINTEXT)
    assert event is not None
    assert event.type == "live"
    assert event.weight_kg == 72.8
    assert event.stable is True

    # Unstable 0.0 kg live frame
    prefix = bytes(12) + b"\xa0"
    zero_event = parse_cmtp_payload(prefix + b"0,0\x00")
    assert zero_event is not None
    assert zero_event.type == "live"
    assert zero_event.weight_kg == 0.0
    assert zero_event.stable is False

    # 0xA0 marker appearing after offset 12
    shifted_event = parse_cmtp_payload(bytes(15) + b"\xa02000,1")
    assert shifted_event is not None
    assert shifted_event.weight_kg == 200.0
    assert shifted_event.stable is True

    # Invalid cases
    assert parse_cmtp_payload(b"short") is None
    assert parse_cmtp_payload(bytes(16)) is None  # missing 0xA0
    assert parse_cmtp_payload(prefix + b"728,2") is None  # invalid stable flag
    assert parse_cmtp_payload(prefix + b"2001,1") is None  # > 200.0 kg
    assert parse_cmtp_payload(prefix + b"-10,0") is None  # negative
    assert parse_cmtp_payload(prefix + "٧٢٨,1".encode()) is None  # non-ASCII digits


def test_parse_cmtp_final_payload_real_and_edge_cases() -> None:
    """Test parsing real 32-field S400 final frames and rejecting truncated/bad ones."""
    event = parse_cmtp_payload(REAL_FINAL_PLAINTEXT)
    assert event is not None
    assert event.type == "final"
    assert event.weight_kg == 72.8
    assert event.stable is False
    assert event.impedance_ohm == 538.0
    assert event.impedance_low_ohm == 494.0
    assert event.profile_id == 1
    assert event.device_timestamp == 1794342279

    prefix = bytes(12) + b"\xa0"
    fields = ["0"] * FINAL_CSV_FIELD_COUNT
    fields[2] = "3"
    fields[3] = "654"
    fields[4] = "1"
    fields[6] = "1700000000"
    fields[30] = ""  # non-digit impedance -> None
    fields[31] = ""
    no_imp = parse_cmtp_payload(prefix + ",".join(fields).encode("ascii"))
    assert no_imp is not None
    assert no_imp.weight_kg == 65.4
    assert no_imp.stable is True
    assert no_imp.profile_id == 3
    assert no_imp.impedance_ohm is None
    assert no_imp.impedance_low_ohm is None

    # Wrong CSV field counts (31 or 33 fields) must be rejected
    assert parse_cmtp_payload(prefix + ",".join(fields[:31]).encode("ascii")) is None
    assert parse_cmtp_payload(prefix + ",".join([*fields, "0"]).encode("ascii")) is None

    # Zero weight in final frame rejected
    zero_w = list(fields)
    zero_w[3] = "0"
    assert parse_cmtp_payload(prefix + ",".join(zero_w).encode("ascii")) is None

    # Missing/invalid device_timestamp or stable flag rejected
    bad_ts = list(fields)
    bad_ts[6] = "abc"
    assert parse_cmtp_payload(prefix + ",".join(bad_ts).encode("ascii")) is None

    bad_stable = list(fields)
    bad_stable[4] = "9"
    assert parse_cmtp_payload(prefix + ",".join(bad_stable).encode("ascii")) is None
