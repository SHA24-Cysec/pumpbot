"""Test pencarian entry proksi: pruning identik, ticker sintetis, no lookahead."""

from __future__ import annotations

import os
import random
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.models import Candle                                    # noqa: E402
from tools.backtest.signals import (                             # noqa: E402
    scan_all, scan_symbol, synthetic_ticker)
from tools.backtest.util import load_backtest_config             # noqa: E402

T0 = 1_700_000_000_000
MIN = 60_000


@pytest.fixture(scope="module")
def cfg():
    """Config asli repo."""
    return load_backtest_config(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config", "config.yaml"))


def make_candles(n: int = 200, seed: int = 7) -> list[Candle]:
    """Deret candle sintetis acak dengan beberapa lonjakan (helper test)."""
    rng = random.Random(seed)
    price = 100.0
    out: list[Candle] = []
    for i in range(n):
        drift = rng.uniform(-0.3, 0.35)
        if i % 37 == 0:
            drift += rng.uniform(1.0, 2.5)        # lonjakan
        o = price
        c = max(1.0, o * (1 + drift / 100.0))
        h = max(o, c) * (1 + abs(rng.uniform(0, 0.2)) / 100.0)
        low = min(o, c) * (1 - abs(rng.uniform(0, 0.2)) / 100.0)
        vol = rng.uniform(50, 150) * (4.0 if i % 37 == 0 else 1.0)
        out.append(Candle(T0 + i * MIN, T0 + (i + 1) * MIN - 1, o, h, low, c,
                          vol, vol * c, 40, vol / 2, True))
        price = c
    return out


def test_pruning_identik_dengan_tanpa_pruning(cfg):
    """Hasil scan dengan pruning harus persis sama dengan tanpa pruning."""
    candles = make_candles(240)
    for threshold in (0.0, 20.0, 40.0, 60.0, 80.0):
        a = scan_symbol("AAAUSDT", candles, cfg, threshold, prune=True)
        b = scan_symbol("AAAUSDT", candles, cfg, threshold, prune=False)
        assert a == b, f"beda pada threshold {threshold}"


def test_entry_di_open_candle_berikutnya(cfg):
    """Setiap Entry menunjuk candle i+1 dan memakai harga open candle itu."""
    candles = make_candles(240)
    entries = scan_symbol("AAAUSDT", candles, cfg, 10.0)
    assert entries, "seharusnya ada entry pada threshold rendah"
    for e in entries:
        assert e.entry_open == candles[e.entry_idx].open
        assert e.ts_entry == candles[e.entry_idx].open_time
        assert e.entry_idx <= len(candles) - 1


def test_tidak_ada_entry_pada_candle_terakhir(cfg):
    """Candle terakhir tidak punya candle i+1, jadi tidak boleh jadi sinyal."""
    candles = make_candles(120)
    entries = scan_symbol("AAAUSDT", candles, cfg, 0.0)
    assert all(e.entry_idx < len(candles) for e in entries)
    assert all(e.entry_idx >= 1 for e in entries)


def test_ticker_sintetis_pakai_data_masa_lalu_saja():
    """price_change_pct dihitung dari candle sebelumnya, bukan masa depan."""
    candles = make_candles(60)
    tk = synthetic_ticker("AAAUSDT", candles, 30)
    assert tk.last_price == candles[30].close
    assert tk.high == max(c.high for c in candles[:31])
    assert tk.low == min(c.low for c in candles[:31])
    ref = candles[0].open
    assert tk.price_change_pct == pytest.approx(
        (candles[30].close - ref) / ref * 100.0)


def test_scan_all_terurut_waktu(cfg):
    """scan_all menggabungkan banyak simbol dan mengurutkan menurut waktu."""
    data = {"AAAUSDT": make_candles(200, seed=1),
            "BBBUSDT": make_candles(200, seed=2)}
    entries = scan_all(data, cfg, threshold=10.0, workers=1)
    ts = [e.ts_entry for e in entries]
    assert ts == sorted(ts)


def test_data_terlalu_pendek_aman(cfg):
    """Data lebih pendek dari min_candles tidak menghasilkan entry atau error."""
    assert scan_symbol("AAAUSDT", make_candles(5), cfg, 0.0) == []
    assert scan_symbol("AAAUSDT", [], cfg, 0.0) == []


def test_scan_all_panggil_progress_cb_per_simbol(cfg):
    """Callback kemajuan dipanggil tepat satu kali per simbol yang dipindai."""
    data = {"AAAUSDT": make_candles(120, seed=3),
            "BBBUSDT": make_candles(120, seed=4)}
    calls: list[int] = []
    scan_all(data, cfg, threshold=10.0, workers=1, progress_cb=calls.append)
    assert calls == [1, 1]


# --------------------------------------------------------------------------
# RollingTicker: jendela bergulir harus setara dengan synthetic_ticker naif
# --------------------------------------------------------------------------

def _banding_ticker(want, got, i):
    """Samakan semua field; volume cukup mendekati karena fp add/sub."""
    assert got.ts == want.ts, i
    assert got.symbol == want.symbol, i
    assert got.last_price == want.last_price, i
    assert got.price_change_pct == want.price_change_pct, i
    assert got.high == want.high, i
    assert got.low == want.low, i
    assert got.trade_count == want.trade_count, i
    assert got.bid == want.bid and got.ask == want.ask, i
    assert got.volume == pytest.approx(want.volume, rel=1e-12, abs=1e-9), i
    assert got.quote_volume == pytest.approx(want.quote_volume,
                                             rel=1e-12, abs=1e-6), i


def test_rolling_ticker_identik_dengan_naif():
    """RollingTicker.at(i) sama dengan synthetic_ticker di semua indeks."""
    from tools.backtest.signals import RollingTicker
    for interval_min in (1, 5):
        candles = make_candles(700, seed=interval_min)
        roll = RollingTicker("AAAUSDT", candles, interval_min)
        for i in range(len(candles)):
            want = synthetic_ticker("AAAUSDT", candles, i, interval_min)
            _banding_ticker(want, roll.at(i), (interval_min, i))


def test_rolling_ticker_mulai_dari_tengah():
    """Jendela tetap benar bila panggilan pertama tidak dari indeks 0."""
    from tools.backtest.signals import RollingTicker
    candles = make_candles(500, seed=11)
    roll = RollingTicker("AAAUSDT", candles, 1)
    for i in (123, 124, 125, 200, 300, 499):
        want = synthetic_ticker("AAAUSDT", candles, i, 1)
        _banding_ticker(want, roll.at(i), i)


def test_scan_hasil_sama_dengan_ticker_naif(cfg, monkeypatch):
    """Integrasi: scan_symbol hasilnya sama apapun implementasi tickernya."""
    import tools.backtest.signals as sig
    candles = make_candles(400, seed=21)

    class TickerNaif:
        def __init__(self, symbol, candles, interval_min=1):
            self.symbol, self.candles, self.interval_min = (
                symbol, candles, interval_min)

        def at(self, i):
            return synthetic_ticker(self.symbol, self.candles, i,
                                    self.interval_min)

    monkeypatch.setattr(sig, "RollingTicker", TickerNaif)
    ref = scan_symbol("AAAUSDT", candles, cfg, 10.0)
    monkeypatch.undo()
    got = scan_symbol("AAAUSDT", candles, cfg, 10.0)
    assert got == ref


# --------------------------------------------------------------------------
# Batas buffer: hasil scan tidak boleh berubah saat riwayat dipangkas
# --------------------------------------------------------------------------

def test_buffer_cap_menutupi_kebutuhan_semua_detector(cfg):
    """cap >= kebutuhan maksimum ketiga detector candle."""
    from tools.backtest.signals import _buffer_cap
    cap = _buffer_cap(cfg)
    pa = cfg.signal.price_action
    need = max(cfg.signal.volume.ma_period + 1,
               pa.structure_candles,
               pa.breakout_lookback + 2,
               cfg.signal.manipulation.pump_dump_window_min,
               15)
    assert cap >= need


def test_scan_hasil_sama_dengan_buffer_tanpa_batas(cfg, monkeypatch):
    """Entry harus identik antara buffer ramping dan buffer nyaris penuh."""
    import tools.backtest.signals as sig
    candles = make_candles(900, seed=33)
    for threshold in (0.0, 25.0, 50.0):
        ramping = scan_symbol("AAAUSDT", candles, cfg, threshold)
        monkeypatch.setattr(sig, "_buffer_cap", lambda c: len(candles) + 1)
        penuh = scan_symbol("AAAUSDT", candles, cfg, threshold)
        monkeypatch.undo()
        assert ramping == penuh, f"beda pada threshold {threshold}"
