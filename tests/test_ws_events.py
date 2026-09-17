"""
Test regresi nama EVENT WebSocket SDK binance-sdk-spot.

Bug nyata: gateway mendaftar event koneksi "reconnected" dan "closed",
padahal SDK hanya menerima {'ping', 'open', 'reconnect', 'pong', 'close',
'error'} -> ValueError saat startup di mode testnet/live (bot mati
sebelum sempat berlangganan stream).

Test ini memvalidasi SEMUA nama event yang dipakai gateway (baik level
koneksi maupun level stream) terhadap daftar yang didukung SDK yang
SEDANG TERPASANG — jadi kalau versi SDK berubah daftar eventnya,
test ini langsung memberi tahu sebelum bot crash di lapangan.
"""

import inspect
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

pytest.importorskip("binance_common")   # SDK tidak wajib utk test lain

from binance_common.websocket import SUPPORTED_CONNECTION_EVENTS  # noqa: E402

import bot.exchange.binance_gateway as gw  # noqa: E402

_SRC = inspect.getsource(gw)


def test_event_koneksi_harus_didukung_sdk():
    events = re.findall(r'on_connection\(\s*"(\w+)"', _SRC)
    assert events, "tidak menemukan registrasi on_connection di gateway"
    tidak_valid = [e for e in events if e not in SUPPORTED_CONNECTION_EVENTS]
    assert not tidak_valid, (
        f"Event koneksi tidak didukung SDK: {tidak_valid}. "
        f"Yang valid: {sorted(SUPPORTED_CONNECTION_EVENTS)}")


def test_event_stream_hanya_message():
    # level stream (.on) SDK hanya mendukung "message"
    events = re.findall(r'\.on\(\s*"(\w+)"', _SRC)
    assert events, "tidak menemukan registrasi .on() di gateway"
    tidak_valid = [e for e in events if e != "message"]
    assert not tidak_valid, f"Event stream tidak didukung SDK: {tidak_valid}"
