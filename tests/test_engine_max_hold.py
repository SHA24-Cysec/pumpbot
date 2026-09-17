"""Test time-stop dan alur 1:2 / BE / trailing pada engine backtest."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tools", "backtest"))

from bot.models import Candle                      # noqa: E402
from tools.backtest.engine import (                # noqa: E402
    CandleScore, Combo, _simulate_symbol, compute_metrics)

T0 = 1_700_000_000_000
MIN = 60_000


def _candle(i, o, h, l, c):
    return Candle(open_time=T0 + i * MIN, close_time=T0 + (i + 1) * MIN - 1,
                  open=o, high=h, low=l, close=c, volume=100.0,
                  quote_volume=10_000.0, trades=50, taker_buy_volume=50.0,
                  closed=True)


def _flat(n=200, price=100.0, start=0):
    return [_candle(i, price, price + 0.1, price - 0.1, price)
            for i in range(start, start + n)]


def _signal(entry_idx=11):
    return [CandleScore(ts=T0 + (entry_idx - 1) * MIN - 1,
                        entry_idx=entry_idx, entry_price=100.0,
                        close=100.0, score=80.0, eligible=True, veto=False,
                        swing_low=98.0)]


# SL 2,5%, TP 2R, BE 1R, trailing 0,5% — tepat alur strategi live.
COMBO = dict(threshold=70, sl_pct=2.5, tp_rr=2.0, be_rr=1.0, trail_pct=0.5)


def _run(combo, candles):
    return _simulate_symbol("TESTUSDT", candles, _signal(), combo,
                            equity=10_000.0, ts0=0, ts1=10**15)


class TestMaxHold:
    def test_nonaktif_perilaku_lama(self):
        tr = _run(Combo(**COMBO, max_hold_min=0), _flat())
        assert len(tr) == 1 and tr[0].reason == "END"

    def test_stall_ditutup_max_hold(self):
        tr = _run(Combo(**COMBO, max_hold_min=60), _flat())
        assert len(tr) == 1
        t = tr[0]
        assert t.reason == "MAX_HOLD"
        umur = (t.exit_ts - (T0 + 11 * MIN)) / MIN
        assert 59 <= umur <= 61
        assert t.pnl < 0

    def test_sl_diutamakan_sebelum_max_hold(self):
        candles = _flat()
        for i in range(11, 16):
            candles[i] = _candle(i, 100, 100.1, 97.0, 97.2)
        tr = _run(Combo(**COMBO, max_hold_min=60), candles)
        assert tr[0].reason == "SL"

    def test_tp_dua_r_tak_terganggu_max_hold(self):
        # Entry sekitar 100,05; 2R dengan SL 2,5% berada sekitar 105,05.
        candles = _flat()
        candles[11] = _candle(11, 100, 106.0, 100, 105.8)
        tr = _run(Combo(**COMBO, max_hold_min=60), candles)
        assert tr[0].reason == "TP"
        assert tr[0].pnl > 0

    def test_be_lalu_trailing_mengunci_profit_sebelum_tp(self):
        candles = _flat()
        # +1R terlewati (~102,55) namun belum mencapai TP 2R (~105,05).
        candles[11] = _candle(11, 100, 103.0, 100.0, 102.8)
        # High 103 membuat trailing sekitar 102,485; candle berikutnya retrace.
        candles[12] = _candle(12, 102.8, 102.9, 102.0, 102.2)
        tr = _run(Combo(**COMBO, max_hold_min=60), candles)
        assert tr[0].reason == "TRAILING"
        assert tr[0].pnl > 0

    def test_metrics_rincian_alasan(self):
        tr = _run(Combo(**COMBO, max_hold_min=60), _flat())
        m = compute_metrics(tr, equity=10_000.0)
        assert m.exit_counts.get("MAX_HOLD") == 1
        assert "MAX_HOLD" in m.exit_pnl_pct
        assert m.avg_hold_min > 50
