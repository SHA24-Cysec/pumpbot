"""
Unit test STOP LOSS / TAKE PROFIT / BREAKEVEN / TRAILING STOP.

Skenario yang diverifikasi:
  * SL awal dari struktur (swing low) maupun persen, dengan pengaman
    min/max jarak.
  * Level TP tunggal (RR), multi target (partial), dan single.
  * Trigger breakeven di TP pertama / gain manual + buffer fee.
  * Trailing stop percent & ATR, sifat monoton (SL tidak pernah turun).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import TPTarget
from bot.models import Candle
from bot.risk_management.stops import (
    atr,
    breakeven_price,
    breakeven_trigger_price,
    initial_stop,
    should_trigger_breakeven,
    should_update_exit_order,
    take_profit_levels,
    trailing_stop_price,
    update_trailing,
)


# ---------------------------------------------------------------------------
# SL awal
# ---------------------------------------------------------------------------

def test_initial_stop_structure():
    # swing 0.95 -> dibatasi max 4% di bawah entry -> 0.96
    s = initial_stop(entry=1.0, swing_low=0.95, mode="structure",
                     percent_pct=2.5, min_stop_pct=0.5, max_stop_pct=4.0)
    assert abs(s - 0.96) < 1e-9


def test_initial_stop_structure_normal():
    # swing 0.972 -> dalam rentang [0.96, 0.995] -> dipakai apa adanya
    s = initial_stop(entry=1.0, swing_low=0.972, mode="structure",
                     percent_pct=2.5, min_stop_pct=0.5, max_stop_pct=4.0)
    assert abs(s - 0.972) < 1e-9


def test_initial_stop_percent():
    s = initial_stop(entry=1.0, swing_low=None, mode="percent",
                     percent_pct=2.5, min_stop_pct=0.5, max_stop_pct=4.0)
    assert abs(s - 0.975) < 1e-9


def test_initial_stop_swing_too_close_clamped():
    # swing 0.999 terlalu dekat -> dinaikkan ke minimal 0.5% (0.995)
    s = initial_stop(entry=1.0, swing_low=0.999, mode="structure",
                     percent_pct=2.5, min_stop_pct=0.5, max_stop_pct=4.0)
    assert abs(s - 0.995) < 1e-9


def test_initial_stop_invalid_entry():
    assert initial_stop(0.0, 0.95, "structure", 2.5, 0.5, 4.0) == 0.0


# ---------------------------------------------------------------------------
# Take profit
# ---------------------------------------------------------------------------

def test_tp_rr_mode():
    tps = take_profit_levels(entry=1.0, stop=0.98, mode="rr", rr=1.5, targets=[])
    assert len(tps) == 1
    assert abs(tps[0]["price"] - 1.03) < 1e-9     # 1.0 + 1.5*0.02
    assert tps[0]["sell_pct"] == 100.0


def test_tp_multi_mode():
    tps = take_profit_levels(entry=1.0, stop=0.98, mode="multi", rr=1.5,
                            targets=[TPTarget(1.2, 50), TPTarget(2.5, 50)])
    assert len(tps) == 2
    assert abs(tps[0]["price"] - 1.012) < 1e-9
    assert abs(tps[1]["price"] - 1.025) < 1e-9
    assert tps[0]["sell_pct"] == 50 and tps[1]["sell_pct"] == 50
    assert tps[0]["price"] < tps[1]["price"]


def test_tp_single_mode():
    tps = take_profit_levels(entry=1.0, stop=0.98, mode="single", rr=1.5,
                            targets=[TPTarget(1.2, 50), TPTarget(2.5, 50)])
    assert len(tps) == 1
    assert tps[0]["sell_pct"] == 100.0
    assert abs(tps[0]["price"] - 1.012) < 1e-9


# ---------------------------------------------------------------------------
# Breakeven
# ---------------------------------------------------------------------------

def test_breakeven_price_covers_fees():
    # buffer 0.25% > 2x fee 0.1% -> pakai 0.25%
    assert abs(breakeven_price(1.0, 0.25, 0.1) - 1.0025) < 1e-9
    # buffer kecil -> minimal pakai 2x fee
    assert abs(breakeven_price(1.0, 0.1, 0.15) - 1.003) < 1e-9


def test_breakeven_trigger_at_one_r_before_two_r_tp():
    # Entry 1.00, SL 0.99 -> BE di 1.01 (+1R), sedangkan TP 2R = 1.02.
    assert breakeven_trigger_price(1.0, 0.99, 1.0) == 1.01
    assert should_trigger_breakeven(1.01, 1.0, 0.99, 1.0) is True
    assert should_trigger_breakeven(1.009, 1.0, 0.99, 1.0) is False


def test_breakeven_trigger_scales_with_stop_distance():
    # R tetap benar meski jarak SL berubah: 2R dari risiko 2% = profit 4%.
    assert breakeven_trigger_price(1.0, 0.98, 1.0) == 1.02
    assert should_trigger_breakeven(1.04, 1.0, 0.98, 2.0) is True


# ---------------------------------------------------------------------------
# Trailing stop
# ---------------------------------------------------------------------------

def test_trailing_percent():
    s = trailing_stop_price(highest=1.10, mode="percent", percent_pct=1.2,
                            atr_value=0.0, atr_multiplier=2.5)
    assert abs(s - 1.10 * 0.988) < 1e-9


def test_trailing_atr():
    s = trailing_stop_price(highest=1.10, mode="atr", percent_pct=1.2,
                            atr_value=0.004, atr_multiplier=2.5)
    assert abs(s - (1.10 - 0.01)) < 1e-9


def test_trailing_never_decreases():
    # kandidat trailing 1.0868 < SL saat ini 1.09 -> SL tetap 1.09
    s = update_trailing(current_sl=1.09, highest=1.10, entry=1.0,
                        mode="percent", percent_pct=1.2, atr_value=0.0,
                        atr_multiplier=2.5)
    assert abs(s - 1.09) < 1e-9
    # kandidat lebih tinggi -> SL naik
    s2 = update_trailing(current_sl=1.05, highest=1.10, entry=1.0,
                         mode="percent", percent_pct=1.2, atr_value=0.0,
                         atr_multiplier=2.5)
    assert abs(s2 - 1.0868) < 1e-4


def test_should_update_exit_order_step():
    # naik 0.1% < step 0.15% -> tidak perlu republish
    assert should_update_exit_order(1.050, 1.051, 0.15) is False
    # naik 1% -> perlu
    assert should_update_exit_order(1.050, 1.060, 0.15) is True
    # turun -> tidak pernah
    assert should_update_exit_order(1.060, 1.050, 0.15) is False


# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------

def _candle(o, h, l, c):
    return Candle(0, 0, o, h, l, c, 100, 100, 10, 50, True)


def test_atr_constant_range():
    # semua candle range 0.02 tanpa gap -> ATR = 0.02
    candles = [_candle(1.0, 1.01, 0.99, 1.0) for _ in range(10)]
    assert abs(atr(candles, 14) - 0.02) < 1e-9


def test_atr_with_gap():
    # candle terakhir gap naik: TR = max(0.02, |1.02-1.0|, |0.98-1.0|) = 0.02... 
    # gunakan gap besar: close 1.0 -> high 1.05, low 1.03
    candles = [_candle(1.0, 1.01, 0.99, 1.0) for _ in range(5)]
    candles.append(_candle(1.0, 1.05, 1.03, 1.04))
    # TR terakhir = max(0.02, |1.05-1.0|=0.05, |1.03-1.0|=0.03) = 0.05
    val = atr(candles, 14)
    assert 0.02 < val <= 0.05


def test_atr_empty():
    assert atr([], 14) == 0.0
