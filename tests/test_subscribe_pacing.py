"""
Test pacing + retry tahan-reset pada BinanceGateway.subscribe().

Latar (terverifikasi empiris): Binance memutus koneksi WS yang menerima
> 5 pesan masuk per detik. Subscribe 69 simbol x 4 stream beruntun di
jaringan cepat menembus batas itu -> ClientConnectionResetError dan bot
crash / stream tidak pernah terpasang. Perbaikan: jeda antar langganan
(pacing) + retry bila koneksi direset.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.exchange import binance_gateway as gw_mod  # noqa: E402


class FakeHandle:
    def __init__(self, name):
        self.name = name
        self.callbacks = []

    def on(self, event, cb):
        self.callbacks.append((event, cb))


class FakeStreams:
    """Meniru websocket_streams SDK; bisa disuruh gagal N kali lalu sukses."""

    def __init__(self, fail_plan=None, refuse_plan=None, exc_factory=None):
        # fail_plan:  dict label -> jumlah kegagalan (raise) awal
        # refuse_plan: dict label -> jumlah penolakan diam (return None) awal
        self.fail_plan = fail_plan or {}
        self.refuse_plan = refuse_plan or {}
        self.exc_factory = exc_factory or (
            lambda: ConnectionResetError("uji: reset"))
        self.attempts = {}   # label -> jumlah percobaan
        self.handles = []
        self.levels_seen = []          # levels depth yang diminta per langganan
        self.create_connection_calls = 0

    async def create_connection(self):
        self.create_connection_calls += 1
        return self

    async def _attempt(self, label):
        self.attempts[label] = self.attempts.get(label, 0) + 1
        n = self.attempts[label]
        if n <= self.refuse_plan.get(label, 0):
            return None            # SDK menolak diam-diam (koneksi sibuk)
        if n <= self.fail_plan.get(label, 0):
            raise self.exc_factory()
        h = FakeHandle(label)
        self.handles.append(h)
        return h

    async def kline(self, symbol, interval):
        return await self._attempt(f"{symbol}:kline")

    async def agg_trade(self, symbol):
        return await self._attempt(f"{symbol}:aggTrade")

    async def partial_book_depth(self, symbol, levels):
        self.levels_seen.append(levels)
        return await self._attempt(f"{symbol}:depth")

    async def ticker(self, symbol):
        return await self._attempt(f"{symbol}:ticker")

    async def list_subscribe(self):
        names = [h.name.replace(":kline", "@kline_1m")
                       .replace(":aggTrade", "@aggTrade")
                       .replace(":depth", "@depth20")
                       .replace(":ticker", "@ticker")
                 for h in self.handles]
        return {"result": names, "id": "uji"}


def _make_gateway(fake_streams, depth_levels=10):
    """Gateway tanpa jaringan: isi atribut internal secara langsung."""
    gw = gw_mod.BinanceGateway.__new__(gw_mod.BinanceGateway)
    gw._depth_levels = depth_levels
    gw._client = types.SimpleNamespace(websocket_streams=fake_streams)
    gw._ws_started = True          # skip create_connection
    gw._stream_handles = []
    gw._subscribed_symbols = []
    gw._cbs = {"candle": lambda *a: None, "trade": lambda *a: None,
               "book": lambda *a: None, "ticker": lambda *a: None}
    gw._watchdog_task = None
    return gw


def _noop_cb(*a, **kw):
    return None


def _cancel_wd(gw):
    if gw._watchdog_task:
        gw._watchdog_task.cancel()
        gw._watchdog_task = None


async def _subscribe(gw, symbols):
    await gw.subscribe(symbols=symbols, on_candle=_noop_cb,
                       on_trade=_noop_cb, on_book=_noop_cb,
                       on_ticker=_noop_cb)
    if gw._watchdog_task:
        gw._watchdog_task.cancel()


@pytest.fixture(autouse=True)
def _cepat(monkeypatch):
    """Percepat test: tanpa jeda pacing/retry sungguhan."""
    monkeypatch.setattr(gw_mod.BinanceGateway, "SUBSCRIBE_PACE_S", 0.0)
    monkeypatch.setattr(gw_mod.BinanceGateway, "SUBSCRIBE_RETRY_WAIT_S", 0.0)


class TestPacing:
    def test_jeda_antar_langganan(self, monkeypatch):
        """Total durasi >= jumlah_langganan x pace (pacing benar-benar jalan)."""
        monkeypatch.setattr(gw_mod.BinanceGateway, "SUBSCRIBE_PACE_S", 0.02)
        fs = FakeStreams()
        gw = _make_gateway(fs)
        t0 = time.monotonic()
        asyncio.run(_subscribe(gw, ["AAAUSDT", "BBBUSDT"]))
        dur = time.monotonic() - t0
        # 2 simbol x 4 stream = 8 langganan, masing2 diawali jeda 0.02s
        assert len(fs.handles) == 8
        assert dur >= 8 * 0.02

    def test_semua_stream_terpasang(self):
        fs = FakeStreams()
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        labels = sorted(h.name for h in fs.handles)
        assert labels == ["aaausdt:aggTrade", "aaausdt:depth",
                          "aaausdt:kline", "aaausdt:ticker"]
        assert len(gw._stream_handles) == 4


class TestRetry:
    def test_reset_koneksi_ditangani_dengan_retry(self):
        """Stream ke-3 direset 2x server -> diulang sampai sukses."""
        fs = FakeStreams(fail_plan={"aaausdt:depth": 2})
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.attempts["aaausdt:depth"] == 3     # 2 gagal + 1 sukses
        assert len(fs.handles) == 4                  # semua stream terpasang

    def test_error_program_tidak_diulang(self):
        """ValueError (bug) langsung naik, tanpa retry."""
        fs = FakeStreams(exc_factory=lambda: ValueError("bug"))
        fs.fail_plan = {"aaausdt:aggTrade": 1}
        gw = _make_gateway(fs)
        with pytest.raises(ValueError):
            asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.attempts["aaausdt:aggTrade"] == 1  # hanya 1 percobaan

    def test_menyerah_setelah_max_percobaan(self, monkeypatch):
        monkeypatch.setattr(gw_mod.BinanceGateway, "SUBSCRIBE_MAX_ATTEMPTS", 2)
        fs = FakeStreams(fail_plan={"aaausdt:ticker": 99})
        gw = _make_gateway(fs)
        with pytest.raises(ConnectionResetError):
            asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.attempts["aaausdt:ticker"] == 2


class TestPemulihanOutage:
    """Pool koneksi kosong (outage panjang) -> dibangun ulang + lanjut."""

    def test_pool_kosong_dibangun_ulang(self):
        """SDK lepas ValueError 'No WebSocket connections available'
        setelah semua reconnect gagal -> gateway bangun ulang + retry."""
        fs = FakeStreams(
            fail_plan={"aaausdt:kline": 2},
            exc_factory=lambda: ValueError("No WebSocket connections available."))
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        # 2x gagal (pool kosong) + 1x sukses; koneksi dibangun ulang 2x
        assert fs.attempts["aaausdt:kline"] == 3
        assert fs.create_connection_calls == 2
        assert len(fs.handles) == 4

    def test_penolakan_diam_sdk_ditangani(self):
        """SDK return None (koneksi sibuk reconnect) -> diulang, tidak
        crash di handle.on()."""
        fs = FakeStreams(refuse_plan={"aaausdt:depth": 1})
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.attempts["aaausdt:depth"] == 2
        assert len(fs.handles) == 4          # semua stream tetap terpasang

    def test_penolakan_diam_terus_menerus_menyerah(self, monkeypatch):
        monkeypatch.setattr(gw_mod.BinanceGateway, "SUBSCRIBE_MAX_ATTEMPTS", 2)
        fs = FakeStreams(refuse_plan={"aaausdt:aggTrade": 99})
        gw = _make_gateway(fs)
        with pytest.raises(RuntimeError):
            asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.attempts["aaausdt:aggTrade"] == 2


class TestWatchdog:
    def test_watchdog_pulihkan_langganan_mati(self):
        """list_subscribe kosong -> berlangganan ulang semua simbol."""
        fs = FakeStreams()
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        fs.handles.clear()                   # simulasi semua stream hilang
        asyncio.run(gw._watchdog_once())
        _cancel_wd(gw)
        assert len(fs.handles) == 4          # dipasang ulang

    def test_watchdog_list_subscribe_error_juga_pulih(self):
        """list_subscribe raise (pool benar-benar mati) -> tetap pulih."""
        fs = FakeStreams()

        async def boom():
            raise RuntimeError("session closed")
        fs.list_subscribe = boom
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        fs.handles.clear()
        asyncio.run(gw._watchdog_once())
        _cancel_wd(gw)
        assert len(fs.handles) == 4

    def test_watchdog_diam_bila_langganan_sehat(self):
        fs = FakeStreams()
        gw = _make_gateway(fs)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        n = len(fs.handles)
        n_attempts = dict(fs.attempts)
        asyncio.run(gw._watchdog_once())
        _cancel_wd(gw)
        assert len(fs.handles) == n                  # tak ada perubahan
        assert fs.attempts == n_attempts



if __name__ == "__main__":
    import unittest
    unittest.main()


class TestDepthLevels:
    """Kedalaman stream orderbook = data.depth_levels (diteruskan apa adanya)."""

    def test_subscribe_pakai_depth10(self):
        fs = FakeStreams()
        gw = _make_gateway(fs, depth_levels=10)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.levels_seen == [10]

    def test_subscribe_pakai_depth20(self):
        fs = FakeStreams()
        gw = _make_gateway(fs, depth_levels=20)
        asyncio.run(_subscribe(gw, ["AAAUSDT"]))
        assert fs.levels_seen == [20]

    def test_validasi_imbalance_tak_boleh_lebih_dari_depth(self):
        from bot.config import Config, validate
        cfg = Config()
        cfg.data.depth_levels = 10
        cfg.signal.orderbook.imbalance_levels = 15   # > depth_levels
        errs = validate(cfg)
        assert any("imbalance_levels" in e and "depth_levels" in e for e in errs)
        cfg.signal.orderbook.imbalance_levels = 10   # valid lagi
        assert not any("imbalance_levels" in e for e in validate(cfg))


