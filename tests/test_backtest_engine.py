"""Test engine simulasi backtest: SL, TP, gap, breakeven, trailing, portofolio."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.models import Candle                                      # noqa: E402
from tools.backtest.engine import (                                # noqa: E402
    Params, compute_metrics, simulate_portfolio, simulate_trade)
from tools.backtest.signals import Entry                           # noqa: E402
from tools.backtest.util import load_backtest_config               # noqa: E402

T0 = 1_700_000_000_000
MIN = 60_000


def candle(i: int, o: float, h: float, low: float, c: float,
           vol: float = 100.0) -> Candle:
    """Buat satu candle sintetis (helper khusus test)."""
    return Candle(open_time=T0 + i * MIN, close_time=T0 + (i + 1) * MIN - 1,
                  open=o, high=h, low=low, close=c, volume=vol,
                  quote_volume=vol * c, trades=50, taker_buy_volume=vol / 2,
                  closed=True)


def flat(i: int, price: float = 100.0) -> Candle:
    """Candle datar di harga tertentu."""
    return candle(i, price, price, price, price)


@pytest.fixture(scope="module")
def cfg():
    """Config asli repo, dimuat lewat helper backtest."""
    return load_backtest_config(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config", "config.yaml"))


def P(**kw) -> Params:
    """Params default untuk test."""
    base = dict(sl_pct=1.0, tp_rr=2.0, be_rr=0.0, be_buffer_pct=0.25,
                trail_pct=0.0, trail_step_pct=0.15, fee_pct=0.1,
                slippage_pct=0.0)
    base.update(kw)
    return Params(**base)


# --------------------------------------------------------------------------
# Trade tunggal
# --------------------------------------------------------------------------

def test_entry_di_open_candle_berikutnya(cfg):
    """Entry memakai OPEN candle entry_idx, bukan close candle sinyal."""
    candles = [flat(0, 90.0), candle(1, 100.0, 100.5, 99.8, 100.2), flat(2)]
    res = simulate_trade(candles, 1, P(), cfg)
    assert res is not None
    assert res.entry == pytest.approx(100.0)
    assert res.ts_entry == candles[1].open_time


def test_sl_kena(cfg):
    """Harga jatuh menyentuh SL 1 persen -> exit di harga stop."""
    candles = [candle(0, 100.0, 100.2, 98.5, 99.0), flat(1, 99.0)]
    res = simulate_trade(candles, 0, P(), cfg)
    assert res.reason == "SL"
    assert res.exit == pytest.approx(99.0)


def test_tp_kena(cfg):
    """Harga naik menyentuh TP 2R (2 persen) -> exit di harga TP."""
    candles = [candle(0, 100.0, 102.5, 99.9, 102.0), flat(1, 102.0)]
    res = simulate_trade(candles, 0, P(), cfg)
    assert res.reason == "TP"
    assert res.exit == pytest.approx(102.0)


def test_sl_dan_tp_candle_sama_sl_menang(cfg):
    """Bila satu candle menyentuh SL dan TP, SL yang dipakai (pesimistis)."""
    candles = [candle(0, 100.0, 103.0, 98.0, 101.0), flat(1, 101.0)]
    res = simulate_trade(candles, 0, P(), cfg)
    assert res.reason == "SL"
    assert res.exit == pytest.approx(99.0)


def test_gap_turun_di_bawah_stop(cfg):
    """Gap turun: exit di open candle yang lebih buruk dari stop."""
    candles = [flat(0, 100.0),
               candle(1, 95.0, 95.2, 94.0, 94.5)]
    # entry di candle 0 (open 100), stop 99; candle 1 membuka di 95
    res = simulate_trade(candles, 0, P(), cfg)
    assert res.reason == "SL"
    assert res.exit == pytest.approx(95.0)


def test_akhir_data_reason_end(cfg):
    """Data habis saat posisi terbuka -> exit di close terakhir, reason END."""
    candles = [flat(0, 100.0), candle(1, 100.0, 100.5, 99.5, 100.4)]
    res = simulate_trade(candles, 0, P(), cfg)
    assert res.reason == "END"
    assert res.exit == pytest.approx(100.4)


def test_breakeven_lalu_trailing_naik_monoton(cfg):
    """Setelah BE aktif, stop trailing hanya naik dan akhirnya mengunci profit."""
    candles = [
        flat(0, 100.0),                       # entry, stop 99
        candle(1, 100.0, 101.2, 100.0, 101.0),  # lewat 1R -> BE
        candle(2, 101.0, 103.0, 100.8, 102.9),  # trailing naik
        candle(3, 102.9, 103.0, 102.0, 102.2),  # pullback kena trail stop
        flat(4, 102.0),
    ]
    params = P(tp_rr=10.0, be_rr=1.0, trail_pct=0.5)
    res = simulate_trade(candles, 0, params, cfg)
    assert res.be_triggered is True
    assert res.reason == "SL"
    # trailing dari high 103 -> 0.5 persen di bawah = 102.485
    assert res.exit == pytest.approx(102.485, rel=1e-6)
    assert res.exit > res.entry


def test_trailing_tidak_aktif_tanpa_breakeven(cfg):
    """trail_pct diisi tapi be_rr=0 -> trailing tidak pernah aktif."""
    candles = [
        flat(0, 100.0),
        candle(1, 100.0, 103.0, 100.0, 102.9),
        candle(2, 102.9, 103.0, 99.5, 99.6),   # jatuh sampai SL awal 99
        flat(3, 99.0),
    ]
    params = P(tp_rr=10.0, be_rr=0.0, trail_pct=0.5)
    res = simulate_trade(candles, 0, params, cfg)
    assert res.reason == "SL"
    assert res.exit == pytest.approx(99.0)   # SL awal, bukan trailing


def test_stop_baru_tidak_berlaku_di_candle_yang_sama(cfg):
    """Stop yang naik pada candle i tidak dipakai untuk candle i itu juga."""
    # Candle 1 naik ke 1R lalu turun ke 100.05 (di atas SL awal 99).
    # Kalau BE dipakai pada candle yang sama, exit akan salah terjadi di sini.
    candles = [
        flat(0, 100.0),
        candle(1, 100.0, 101.5, 100.05, 100.4),
        flat(2, 100.4),
    ]
    params = P(tp_rr=10.0, be_rr=1.0)
    res = simulate_trade(candles, 0, params, cfg)
    assert res.reason == "END"
    assert res.be_triggered is True


# --------------------------------------------------------------------------
# Portofolio
# --------------------------------------------------------------------------

def _entry(sym: str, idx: int, candles: list[Candle]) -> Entry:
    """Bantu membuat Entry untuk test portofolio."""
    return Entry(symbol=sym, entry_idx=idx, ts_entry=candles[idx].open_time,
                 entry_open=candles[idx].open, score=80.0)


def test_hanya_satu_posisi_terbuka(cfg):
    """Entry yang muncul saat posisi lain berjalan harus dilewati."""
    a = [flat(0), candle(1, 100.0, 100.2, 98.0, 98.5), flat(2, 98.5)]
    b = [flat(i) for i in range(4)]
    data = {"AAAUSDT": a, "BBBUSDT": b}
    entries = [_entry("AAAUSDT", 0, a), _entry("BBBUSDT", 0, b)]
    res = simulate_portfolio(entries, data, P(), cfg, start_equity=1000.0,
                             risk_pct=1.0)
    assert len(res.trades) == 1
    assert res.trades[0].symbol == "AAAUSDT"


def test_cooldown_per_simbol(cfg):
    """Entry ulang simbol yang sama dalam masa cooldown harus dilewati."""
    a = [candle(0, 100.0, 100.2, 98.0, 98.5)] + [flat(i, 98.5) for i in range(1, 40)]
    data = {"AAAUSDT": a}
    entries = [_entry("AAAUSDT", 0, a), _entry("AAAUSDT", 5, a)]
    res = simulate_portfolio(entries, data, P(), cfg, start_equity=1000.0,
                             risk_pct=1.0)
    # cooldown config 15 menit; entry kedua hanya 5 menit sesudahnya
    assert len(res.trades) == 1


def test_metrik_dasar():
    """compute_metrics menghitung win rate, PF, dan drawdown dengan benar."""
    from tools.backtest.engine import TradeResult
    t1 = TradeResult("A", 0, 1, 100, 102, 99, 102, "TP", 1, pnl=20.0, r_multiple=2.0)
    t2 = TradeResult("A", 2, 3, 100, 99, 99, 102, "SL", 3, pnl=-10.0, r_multiple=-1.0)
    curve = [(1, 1020.0), (3, 1010.0)]
    m = compute_metrics([t1, t2], curve, 1000.0)
    assert m["trades"] == 2
    assert m["win_rate"] == 50.0
    assert m["profit_factor"] == pytest.approx(2.0)
    assert m["net_return_pct"] == pytest.approx(1.0)
    assert m["max_dd_pct"] == pytest.approx((1020 - 1010) / 1020 * 100, abs=1e-3)


def test_metrik_kosong_tanpa_division_by_zero():
    """Tanpa trade, metrik tetap terdefinisi dan tidak melempar error."""
    m = compute_metrics([], [], 1000.0)
    assert m["trades"] == 0
    assert m["profit_factor"] == 0.0
    assert m["max_dd_pct"] == 0.0
    assert m["net_return_pct"] == 0.0


def test_input_tidak_valid_aman(cfg):
    """Harga nol, indeks di luar batas, dan equity nol tidak melempar error."""
    zero = [candle(0, 0.0, 0.0, 0.0, 0.0)]
    assert simulate_trade(zero, 0, P(), cfg) is None
    assert simulate_trade([flat(0)], 5, P(), cfg) is None
    assert simulate_trade([], 0, P(), cfg) is None
    res = simulate_portfolio([], {}, P(), cfg, start_equity=0.0)
    assert res.trades == []


def test_sl_pct_nol_dijepit_min_stop_pct(cfg):
    """sl_pct 0 dijepit oleh min_stop_pct config agar qty tidak meledak."""
    res = simulate_trade([flat(0), candle(1, 100.0, 100.0, 99.0, 99.2)], 0,
                         P(sl_pct=0.0), cfg)
    assert res.stop_initial == pytest.approx(100.0 * (1 - cfg.stops.min_stop_pct / 100))


def test_slippage_memperburuk_entry_dan_sl(cfg):
    """Slippage menaikkan harga entry dan menurunkan harga exit SL."""
    candles = [candle(0, 100.0, 100.2, 98.0, 98.5), flat(1, 98.5)]
    res = simulate_trade(candles, 0, P(slippage_pct=0.1), cfg)
    assert res.entry == pytest.approx(100.1)
    assert res.reason == "SL"
    assert res.exit < res.stop_initial


def test_cooldown_per_kombinasi_mengalahkan_config(cfg):
    """Params.cooldown_min menimpa signal.cooldown_after_exit_min."""
    candles = [candle(0, 100.0, 101.0, 99.0, 100.5),
               candle(1, 100.5, 101.5, 99.5, 100.8),
               candle(2, 100.8, 99.0, 99.0, 99.5),
               candle(3, 99.5, 100.0, 99.0, 99.8),
               candle(4, 99.8, 100.8, 99.5, 100.5)]
    entries = [Entry("AAAUSDT", 0, T0, 100.0, 80.0),
               Entry("AAAUSDT", 3, T0 + 3 * MIN, 99.5, 80.0)]
    data = {"AAAUSDT": candles}
    base = dict(sl_pct=1.0, tp_rr=3.0, be_rr=0.0, be_buffer_pct=0.2,
                trail_pct=0.0, trail_step_pct=0.15, fee_pct=0.1,
                slippage_pct=0.0)
    tanpa_cd = simulate_portfolio(entries, data, P(**base, cooldown_min=0.0),
                                  cfg, start_equity=1000.0)
    cd_besar = simulate_portfolio(entries, data, P(**base, cooldown_min=999),
                                  cfg, start_equity=1000.0)
    assert tanpa_cd.metrics["trades"] == 2
    assert cd_besar.metrics["trades"] == 1


def test_presorted_hasil_identik_dengan_sort_biasa(cfg):
    """presorted=True tidak mengubah hasil selama input benar-benar terurut."""
    candles = [flat(i) if i % 3 else candle(i, 100.0, 105.0, 99.0, 104.0)
               for i in range(60)]
    entries = [Entry("AAAUSDT", 3 * k, T0 + 3 * k * MIN, 100.0, 80.0)
               for k in range(1, 12)]
    data = {"AAAUSDT": candles}
    base = dict(sl_pct=1.0, tp_rr=2.0, be_rr=1.0, be_buffer_pct=0.2,
                trail_pct=0.0, trail_step_pct=0.15, fee_pct=0.1,
                slippage_pct=0.0)
    a = simulate_portfolio(entries, data, P(**base), cfg, presorted=False)
    b = simulate_portfolio(entries, data, P(**base), cfg, presorted=True)
    assert a.metrics == b.metrics
    # entri acak (tidak terurut) tetap aman di jalur default
    c = simulate_portfolio(list(reversed(entries)), data, P(**base), cfg,
                           presorted=False)
    assert c.metrics == a.metrics
