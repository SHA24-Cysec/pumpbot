"""
Regresi: guard pembagian-nol di ManipulationDetector (wash-trading check).

Data riil pair illiquid memuat rentetan candle dengan volume 0 —
fmean(prev20) = 0.0 membuat `vol_spike` membagi dengan nol dan
meledakkan ZeroDivisionError di tengah grid backtest (dan berpotensi di
jalur LIVE bot juga). Guard: baseline 0 -> vol_spike netral 1.0.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.models import Candle, Ticker24h
from bot.signal_engine.detectors import ManipulationDetector
from bot.utils import now_ms


def _mk_buf(candles_spec):
    """candles_spec: list of (volume, trades) — harga flat 1.0."""
    buf = SymbolBuffer("SEPIUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    n = len(candles_spec)
    for k, (vol, trades) in enumerate(candles_spec):
        t = base - (n - k) * 60_000
        buf.on_candle(Candle(
            open_time=t, close_time=t + 59_999,
            open=1.0, high=1.001, low=0.999, close=1.0,
            volume=vol, quote_volume=vol * 1.0,
            trades=trades, taker_buy_volume=vol * 0.5, closed=True))
    buf.on_ticker(Ticker24h(
        ts=now_ms(), symbol="SEPIUSDT", last_price=1.0,
        price_change_pct=0.5, high=1.01, low=0.99,
        volume=1.0, quote_volume=1.0, trade_count=n, bid=0.999, ask=1.001))
    return buf


class TestZeroVolumeGuard(unittest.TestCase):
    def _cfg(self):
        cfg = Config()
        return cfg

    def test_prev20_semua_volume_nol_tidak_crash(self):
        """30+ candle dengan prev20 volume 0 semua -> netral, bukan crash."""
        # 10 candle terakhir ADA volume, 20 sebelumnya volume 0 semua
        spec = [(0.0, 0)] * 25 + [(5.0, 7)] * 10
        buf = _mk_buf(spec)
        res = ManipulationDetector().score(buf, self._cfg())
        self.assertTrue(res.eligible)
        self.assertEqual(res.details["vol_spike10"], 1.0)   # netral

    def test_semua_volume_nol_tidak_crash(self):
        """Seluruh buffer volume 0 (pair mati) -> tetap harus jalan."""
        buf = _mk_buf([(0.0, 0)] * 40)
        res = ManipulationDetector().score(buf, self._cfg())
        self.assertTrue(res.eligible)
        self.assertEqual(res.details["vol_spike10"], 1.0)

    def test_volume_normal_tidak_berubah_perilaku(self):
        """Baseline sehat -> vol_spike tetap terhitung seperti sebelumnya."""
        spec = [(2.0, 5)] * 25 + [(10.0, 20)] * 10
        buf = _mk_buf(spec)
        res = ManipulationDetector().score(buf, self._cfg())
        self.assertTrue(res.eligible)
        # fmean(last10)=10, fmean(prev20)=2 -> spike = 5.0
        self.assertAlmostEqual(res.details["vol_spike10"], 5.0, places=2)

    def test_close_nol_tidak_crash(self):
        """Data korup close=0 -> rng = 0, bukan ZeroDivisionError."""
        buf = _mk_buf([(1.0, 3)] * 40)
        # suntikkan candle korup terakhir
        buf.on_candle(Candle(
            open_time=now_ms() - 60_000, close_time=now_ms() - 1,
            open=0.0, high=0.0, low=0.0, close=0.0,
            volume=1.0, quote_volume=0.0, trades=1,
            taker_buy_volume=0.5, closed=True))
        res = ManipulationDetector().score(buf, self._cfg())
        # tidak crash sudah cukup; eligibilitas bergantung gate lain
        self.assertIn("range10", res.details)


if __name__ == "__main__":
    import unittest
    unittest.main()
