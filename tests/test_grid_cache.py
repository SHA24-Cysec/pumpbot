"""
Test cache skor grid.py: roundtrip, invalidasi saat data berubah, dan
ketahanan terhadap file cache korup (harus fallback hitung ulang).
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tools", "backtest")))

from bot.models import Candle  # noqa: E402
from tools.backtest import grid  # noqa: E402
from tools.backtest.engine import CandleScore  # noqa: E402


def _mk_candles(n=10):
    return [Candle(open_time=1_700_000_000_000 + i * 60_000,
                   close_time=1_700_000_059_999 + i * 60_000,
                   open=1.0, high=1.1, low=0.9, close=1.0,
                   volume=5.0, quote_volume=5.0, trades=3,
                   taker_buy_volume=2.5, closed=True) for i in range(n)]


class TestGridScoreCache(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.orig_cache_dir = grid.CACHE_DIR
        grid.CACHE_DIR = self.tmp.name

    def tearDown(self):
        grid.CACHE_DIR = self.orig_cache_dir
        self.tmp.cleanup()

    def test_roundtrip(self):
        candles = _mk_candles()
        scores = [CandleScore(ts=c.close_time, entry_idx=i + 1,
                              entry_price=1.0, close=c.close,
                              score=50.0 + i, eligible=True,
                              veto=False, swing_low=0.9)
                  for i, c in enumerate(candles[:-1])]
        grid.save_cached_scores("TESTUSDT", candles, "cfg-x", scores)
        loaded = grid.load_cached_scores("TESTUSDT", candles, "cfg-x")
        self.assertEqual(loaded, scores)

    def test_data_berubah_cache_miss(self):
        candles = _mk_candles()
        scores = [CandleScore(ts=1, entry_idx=1, entry_price=1.0,
                              close=1.0, score=1.0, eligible=True,
                              veto=False, swing_low=1.0)]
        grid.save_cached_scores("TESTUSDT", candles, "cfg-x", scores)
        # jumlah candle berbeda -> kunci beda -> miss
        self.assertIsNone(
            grid.load_cached_scores("TESTUSDT", _mk_candles(9), "cfg-x"))
        # config berbeda -> miss
        self.assertIsNone(
            grid.load_cached_scores("TESTUSDT", candles, "cfg-y"))

    def test_cache_korup_fallback_none(self):
        candles = _mk_candles()
        path = grid._cache_path("TESTUSDT", candles, "cfg-x")
        with open(path, "wb") as f:
            f.write(b"Bukan pickle yang valid")
        self.assertIsNone(
            grid.load_cached_scores("TESTUSDT", candles, "cfg-x"))


if __name__ == "__main__":
    unittest.main()
