"""
Unit test FILTER ANCHORED VWAP.

Menguji rumus, pemilihan anchor, keputusan gate, edge case, cache, integrasi
engine dan backtest, serta konsistensi config. Gaya pembangun data sintetis
mengikuti tests/test_detectors.py (mk_buf).
"""

import copy
import math
import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import Config, VWAPCfg, validate  # noqa: E402
from bot.data_collector.buffers import SymbolBuffer  # noqa: E402
from bot.models import BookSnapshot, Candle, Ticker24h  # noqa: E402
from bot.signal_engine.engine import SignalEngine  # noqa: E402
from bot.signal_engine.vwap import (  # noqa: E402
    FILTER_DETECTORS,
    VWAPFilter,
    anchored_vwap,
    find_anchor,
    sanitize_candles,
)
from bot.utils import now_ms  # noqa: E402

BASE = 1_700_000_000_000 // 60_000 * 60_000
MIN = 60_000


# ---------------------------------------------------------------------------
# helper
# ---------------------------------------------------------------------------

def mk_candle(i, *, o=1.0, h=None, lo=None, c=None, vol=1.0, qv=None,
              closed=True, base=BASE):
    """Candle 1 menit ke-i (open_time = base + i menit)."""
    c = o if c is None else c
    h = max(o, c) if h is None else h
    lo = min(o, c) if lo is None else lo
    qv = ((h + lo + c) / 3.0 * vol) if qv is None else qv
    return Candle(open_time=base + i * MIN, close_time=base + i * MIN + MIN - 1,
                  open=o, high=h, low=lo, close=c, volume=vol,
                  quote_volume=qv, trades=10, taker_buy_volume=vol / 2,
                  closed=closed)


def mk_buf(candles, last_price=None, symbol="TESTUSDT"):
    """Buffer sintetis berisi daftar candle yang diberikan."""
    buf = SymbolBuffer(symbol, max_candles=1000)
    for c in candles:
        buf.on_candle(c)
    if last_price is not None:
        buf._last_price = last_price
    mid = buf.last_price or 1.0
    buf.on_book(BookSnapshot(ts=now_ms(),
                             bids=[(mid * 0.999, 100.0)] * 20,
                             asks=[(mid * 1.001, 100.0)] * 20))
    buf.on_ticker(Ticker24h(ts=now_ms(), symbol=symbol, last_price=mid,
                            price_change_pct=0.0, high=mid, low=mid,
                            volume=100, quote_volume=100 * mid,
                            trade_count=100, bid=mid, ask=mid))
    if last_price is not None:
        buf._last_price = last_price
    return buf


def vcfg(**kw):
    c = VWAPCfg(enabled=True)
    for k, v in kw.items():
        setattr(c, k, v)
    return c


def cfg_with(**kw):
    cfg = Config()
    cfg.signal.vwap = vcfg(**kw)
    return cfg


def flat(n, *, vol=10.0, price=1.0, start=0):
    """n candle datar dengan volume seragam."""
    return [mk_candle(start + i, o=price, c=price, vol=vol) for i in range(n)]


# ---------------------------------------------------------------------------
# 1. Rumus
# ---------------------------------------------------------------------------

def test_rumus_vwap_tiga_candle_hitung_tangan():
    cs = [
        mk_candle(0, o=1.0, c=1.0, vol=10.0, qv=100.0),
        mk_candle(1, o=1.0, c=2.0, vol=20.0, qv=300.0),
        mk_candle(2, o=2.0, c=3.0, vol=30.0, qv=750.0),
    ]
    vwap, vol, used = anchored_vwap(cs, 0)
    # (100 + 300 + 750) / (10 + 20 + 30) = 1150 / 60
    assert vwap == pytest.approx(1150.0 / 60.0)
    assert vol == pytest.approx(60.0)
    assert used == 3
    # anchor di tengah: (300 + 750) / 50
    assert anchored_vwap(cs, 1)[0] == pytest.approx(1050.0 / 50.0)


def test_fallback_typical_price_saat_quote_volume_nol():
    c = mk_candle(0, o=1.0, h=3.0, lo=1.0, c=2.0, vol=10.0, qv=0.0)
    typical = (3.0 + 1.0 + 2.0) / 3.0
    vwap, vol, used = anchored_vwap([c], 0)
    assert vwap == pytest.approx(typical)
    assert vol == pytest.approx(10.0) and used == 1


# ---------------------------------------------------------------------------
# 2. Anchor pump_start
# ---------------------------------------------------------------------------

def _find(cs, **kw):
    params = dict(anchor_mode="pump_start", anchor_lookback_candles=60,
                  pump_volume_mult=3.0, pump_baseline_candles=20,
                  pump_max_gap_candles=3, no_anchor_action="impulse_low",
                  manual_anchors={}, symbol="TESTUSDT")
    params.update(kw)
    return find_anchor(cs, **params)


def test_pump_start_lonjakan_tunggal():
    cs = flat(40)
    cs[30] = mk_candle(30, o=1.0, c=1.1, vol=100.0)   # lonjakan bullish
    idx, mode, _ = _find(cs)
    assert idx == 30 and mode == "pump_start"


def test_pump_start_rantai_multi_candle_dengan_gap_kecil():
    cs = flat(40)
    for k in (25, 27, 30):
        cs[k] = mk_candle(k, o=1.0, c=1.1, vol=100.0)
    idx, mode, _ = _find(cs)
    assert idx == 25 and mode == "pump_start"


def test_pump_start_lonjakan_lama_terpisah_diabaikan():
    cs = flat(60)
    cs[25] = mk_candle(25, o=1.0, c=1.1, vol=100.0)   # lonjakan lama, gap besar
    cs[50] = mk_candle(50, o=1.0, c=1.1, vol=100.0)
    idx, _, _ = _find(cs)
    assert idx == 50


def test_pump_start_lonjakan_bearish_tidak_dihitung():
    cs = flat(40)
    cs[30] = mk_candle(30, o=1.1, c=1.0, vol=100.0)   # volume besar tapi merah
    idx, mode, _ = _find(cs)
    assert mode == "impulse_low"          # jatuh ke fallback


def test_tanpa_lonjakan_fallback_impulse_low_dan_block():
    cs = flat(40)
    cs[10] = mk_candle(10, o=1.0, c=1.0, lo=0.5, vol=10.0)
    idx, mode, _ = _find(cs, no_anchor_action="impulse_low")
    assert idx == 10 and mode == "impulse_low"
    idx2, _, reason = _find(cs, no_anchor_action="block")
    assert idx2 is None and "diblokir" in reason


# ---------------------------------------------------------------------------
# 3. Anchor impulse_low dan manual
# ---------------------------------------------------------------------------

def test_anchor_impulse_low_memakai_window_sendiri():
    cs = flat(80)
    cs[5] = mk_candle(5, o=1.0, c=1.0, lo=0.1, vol=10.0)     # di luar window 60
    cs[40] = mk_candle(40, o=1.0, c=1.0, lo=0.5, vol=10.0)   # di dalam window
    idx, mode, _ = _find(cs, anchor_mode="impulse_low")
    assert idx == 40 and mode == "impulse_low"


def test_anchor_manual_terdaftar_dan_tidak_terdaftar():
    cs = flat(40)
    ts = cs[20].open_time
    idx, mode, _ = _find(cs, anchor_mode="manual",
                         manual_anchors={"TESTUSDT": ts})
    assert idx == 20 and mode == "manual"
    idx2, mode2, _ = _find(cs, anchor_mode="manual", manual_anchors={})
    assert mode2 == "impulse_low"     # ikut no_anchor_action


def test_anchor_manual_di_luar_window_ikut_no_anchor_action():
    cs = flat(80)
    ts = cs[2].open_time
    idx, _, reason = _find(cs, anchor_mode="manual",
                           manual_anchors={"TESTUSDT": ts},
                           no_anchor_action="block")
    assert idx is None and "di luar window" in reason


# ---------------------------------------------------------------------------
# 4. Keputusan gate
# ---------------------------------------------------------------------------

def _dist_case(dist_pct, **kw):
    cs = flat(30, vol=10.0, price=1.0)
    vwap = 1.0
    buf = mk_buf(cs, last_price=vwap * (1 + dist_pct / 100.0))
    cfg = cfg_with(anchor_mode="impulse_low", **kw)
    return VWAPFilter().score(buf, cfg)


def test_keputusan_tepat_di_batas_min_dan_max_inklusif():
    assert _dist_case(0.0).eligible          # tepat min
    assert _dist_case(8.0).eligible          # tepat max


def test_keputusan_di_bawah_vwap_ditolak():
    r = _dist_case(-1.2)
    assert not r.eligible and "min" in r.details["reason"]


def test_keputusan_di_atas_max_ditolak():
    r = _dist_case(20.0)
    assert not r.eligible and "max" in r.details["reason"]
    assert r.score == 0.0


# ---------------------------------------------------------------------------
# 5. Edge case
# ---------------------------------------------------------------------------

def test_total_volume_nol_diblokir_dan_allow():
    cs = flat(30, vol=0.0)
    buf = mk_buf(cs, last_price=1.0)
    assert not VWAPFilter().score(buf, cfg_with(anchor_mode="impulse_low")).eligible
    r = VWAPFilter().score(buf, cfg_with(anchor_mode="impulse_low",
                                         on_insufficient_data="allow"))
    assert r.eligible


def test_candle_sejak_anchor_kurang_dari_min():
    cs = flat(30)
    cs[29] = mk_candle(29, o=1.0, c=1.0, lo=0.5, vol=10.0)  # anchor di candle terakhir
    buf = mk_buf(cs, last_price=1.0)
    r = VWAPFilter().score(buf, cfg_with(anchor_mode="impulse_low",
                                         min_anchor_candles=3))
    assert not r.eligible and "data kurang" in r.details["reason"]


def test_last_price_nol_dan_nan_diblokir():
    cs = flat(30)
    for bad in (0.0, float("nan"), float("inf")):
        buf = mk_buf(cs, last_price=bad)
        r = VWAPFilter().score(buf, cfg_with(anchor_mode="impulse_low"))
        assert not r.eligible


def test_candle_duplikat_dan_tidak_berurutan_dibuang():
    cs = flat(5, vol=10.0)
    dup = [cs[0], cs[1], cs[1], cs[0], cs[2], cs[3], cs[4]]
    clean = sanitize_candles(dup)
    assert [c.open_time for c in clean] == [c.open_time for c in cs]
    # candle belum close juga dibuang
    open_candle = mk_candle(5, o=1.0, c=9.0, vol=99.0, closed=False)
    assert len(sanitize_candles(cs + [open_candle])) == len(cs)


# ---------------------------------------------------------------------------
# 6. Tanpa look-ahead
# ---------------------------------------------------------------------------

def test_tanpa_look_ahead_candle_masa_depan_tidak_mengubah_keputusan():
    cs = flat(40)
    cs[30] = mk_candle(30, o=1.0, c=1.1, vol=100.0)
    cfg = cfg_with()
    buf_a = mk_buf(cs, last_price=1.05)
    r_a = VWAPFilter().score(buf_a, cfg)
    future = cs + [mk_candle(40 + i, o=5.0, c=5.0, vol=999.0) for i in range(5)]
    # keputusan pada indeks lama dihitung ulang dari prefix yang sama
    buf_b = mk_buf(future[:40], last_price=1.05)
    r_b = VWAPFilter().score(buf_b, cfg)
    assert r_a.details == r_b.details


# ---------------------------------------------------------------------------
# 7. Cache
# ---------------------------------------------------------------------------

def test_cache_hasil_sama_dan_invalid_saat_candle_atau_parameter_berubah():
    cs = flat(40)
    cs[30] = mk_candle(30, o=1.0, c=1.1, vol=100.0)
    cfg = cfg_with()
    buf = mk_buf(cs, last_price=1.05)
    f = VWAPFilter()
    r1 = f.score(buf, cfg)
    r2 = f.score(buf, cfg)                      # cache hit
    fresh = VWAPFilter().score(buf, cfg)        # tanpa cache
    assert r1.details == r2.details == fresh.details

    # candle baru -> cache invalid
    buf.on_candle(mk_candle(40, o=1.05, c=1.06, vol=10.0))
    buf._last_price = 1.06
    r3 = f.score(buf, cfg)
    assert r3.details != r1.details

    # parameter berubah -> cache invalid
    cfg2 = cfg_with(anchor_mode="impulse_low")
    r4 = f.score(buf, cfg2)
    assert r4.details["anchor_mode_used"] == "impulse_low"


def test_cache_tidak_bocor_antar_simbol():
    cfg = cfg_with(anchor_mode="impulse_low")
    a = flat(30, price=1.0)
    b = [mk_candle(i, o=2.0, c=2.0, vol=10.0) for i in range(30)]
    buf_a = mk_buf(a, last_price=1.0, symbol="AAAUSDT")
    buf_b = mk_buf(b, last_price=2.0, symbol="BBBUSDT")
    f = VWAPFilter()
    ra = f.score(buf_a, cfg)
    rb = f.score(buf_b, cfg)
    assert ra.details["vwap"] == pytest.approx(1.0)
    assert rb.details["vwap"] == pytest.approx(2.0)
    assert f.score(buf_a, cfg).details["vwap"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# 8. Integrasi engine dengan collector palsu
# ---------------------------------------------------------------------------

class FakeCollector:
    def __init__(self, buf):
        self._buf = buf
        self.watchlist = [buf.symbol]

    def buffer(self, symbol):
        return self._buf if symbol == self._buf.symbol else None


def _engine_buf():
    """Buffer tren naik yang lolos gate detector positif."""
    buf = SymbolBuffer("TESTUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    price = 1.0
    for k in range(80, 0, -1):
        o = price
        c = price * 1.002
        buf.on_candle(Candle(open_time=base - k * MIN,
                             close_time=base - k * MIN + MIN - 1,
                             open=o, high=c * 1.001, low=o * 0.999, close=c,
                             volume=10.0, quote_volume=10.0 * c, trades=10,
                             taker_buy_volume=6.0, closed=True))
        price = c
    mid = price
    bids = [(mid * (1 - 0.00001 * (i + 1)), 100.0) for i in range(20)]
    asks = [(mid * (1 + 0.00001 * (i + 1)), 100.0) for i in range(20)]
    buf.on_book(BookSnapshot(ts=now_ms(), bids=bids, asks=asks))
    buf.on_ticker(Ticker24h(ts=now_ms(), symbol="TESTUSDT", last_price=mid,
                            price_change_pct=1.0, high=mid, low=mid * 0.9,
                            volume=1000, quote_volume=1000 * mid,
                            trade_count=10000, bid=mid, ask=mid))
    return buf


def test_engine_filter_off_snapshot_identik_dengan_sebelumnya():
    buf = _engine_buf()
    cfg_off = Config()                       # default dataclass: vwap mati
    eng = SignalEngine(cfg_off, FakeCollector(buf))
    snap = eng.evaluate("TESTUSDT")
    assert "vwap" not in snap
    assert set(snap["breakdown"]) == {"orderbook", "trade_flow", "volume",
                                      "whale", "manipulation", "price_action"}
    assert set(snap) >= {"symbol", "ts", "price", "score", "base_score",
                         "eligible", "veto", "breakdown", "manip_details"}


def test_engine_filter_on_gagal_menolak_sinyal():
    buf = _engine_buf()
    cfg = Config()
    cfg.signal.vwap = vcfg(anchor_mode="impulse_low",
                           min_above_pct=50.0, max_above_pct=90.0)
    eng = SignalEngine(cfg, FakeCollector(buf))
    snap = eng.evaluate("TESTUSDT")
    assert snap["vwap"]["passed"] is False
    assert snap["eligible"] is False
    assert "vwap" in snap["reason"]
    ok, _ = eng._should_emit("TESTUSDT", snap)
    assert not ok
    # key skor tidak bertambah
    assert "vwap" not in snap["breakdown"]


def test_engine_filter_on_lolos_masuk_breakdown_signal():
    buf = _engine_buf()
    cfg = Config()
    cfg.signal.vwap = vcfg(anchor_mode="impulse_low",
                           min_above_pct=-5.0, max_above_pct=100.0)
    eng = SignalEngine(cfg, FakeCollector(buf))
    snap = eng.evaluate("TESTUSDT")
    assert snap["vwap"]["passed"] is True
    breakdown = {"scores": snap["breakdown"], "manip": snap["manip_details"],
                 "vwap": snap.get("vwap")}
    assert breakdown["vwap"]["passed"] is True


def test_engine_override_runtime_menang_atas_config():
    buf = _engine_buf()
    cfg = Config()
    cfg.signal.vwap = vcfg(anchor_mode="impulse_low",
                           min_above_pct=50.0, max_above_pct=90.0)
    eng = SignalEngine(cfg, FakeCollector(buf))
    eng.vwap_enabled_override = False
    snap = eng.evaluate("TESTUSDT")
    assert "vwap" not in snap


# ---------------------------------------------------------------------------
# 9. Config
# ---------------------------------------------------------------------------

def test_default_dataclass_vwap_mati():
    assert Config().signal.vwap.enabled is False


def test_semua_key_yaml_vwap_ada_di_dataclass():
    """_build mengabaikan key tak dikenal diam-diam; tes ini menangkap typo."""
    import dataclasses as dc
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "config", "config.yaml")
    raw = yaml.safe_load(open(path, "r", encoding="utf-8"))
    block = (raw.get("signal") or {}).get("vwap") or {}
    assert block, "blok signal.vwap wajib ada di config.yaml"
    known = {f.name for f in dc.fields(VWAPCfg)}
    unknown = set(block) - known
    assert not unknown, f"key VWAP tidak dikenal di YAML: {sorted(unknown)}"


def _errs(**kw):
    cfg = Config()
    cfg.signal.vwap = vcfg(**kw)
    return [e for e in validate(cfg) if "vwap" in e]


def test_validasi_negatif_tiap_aturan():
    assert _errs(anchor_mode="salah")
    assert _errs(no_anchor_action="salah")
    assert _errs(on_insufficient_data="salah")
    assert _errs(anchor_lookback_candles=4)
    assert _errs(anchor_lookback_candles=110, pump_baseline_candles=20)
    assert _errs(pump_volume_mult=1.0)
    assert _errs(pump_baseline_candles=1)
    assert _errs(pump_max_gap_candles=-1)
    assert _errs(min_anchor_candles=0)
    assert _errs(min_above_pct=9.0, max_above_pct=8.0)
    assert _errs(min_above_pct=-6.0)
    assert _errs(max_above_pct=101.0)
    assert _errs(manual_anchors={"X": -1})
    assert _errs(manual_anchors={"X": "abc"})
    assert _errs(enabled="ya")
    assert not _errs()          # default valid


# ---------------------------------------------------------------------------
# 10. Backtest
# ---------------------------------------------------------------------------

def _bt_candles(n=400):
    cs = []
    price = 1.0
    for i in range(n):
        vol = 10.0
        if i % 37 == 0:
            price *= 1.02
            vol = 120.0
        else:
            price *= 1.0005 if i % 3 else 0.9995
        cs.append(mk_candle(i, o=price / 1.001, c=price, vol=vol))
    return cs


def test_backtest_buffer_cap_cukup_untuk_vwap():
    from tools.backtest.signals import _buffer_cap
    cs = _bt_candles(300)
    cfg = cfg_with()
    cap = _buffer_cap(cfg)
    assert cap >= cfg.signal.vwap.anchor_lookback_candles + \
        cfg.signal.vwap.pump_baseline_candles
    full = mk_buf(cs, last_price=cs[-1].close)
    capped = SymbolBuffer("TESTUSDT", max_candles=cap)
    for c in cs:
        capped.on_candle(c)
    capped._last_price = cs[-1].close
    assert VWAPFilter().score(full, cfg).details == \
        VWAPFilter().score(capped, cfg).details


def test_backtest_scan_off_identik_dan_on_subset():
    from tools.backtest.signals import scan_symbol
    cs = _bt_candles(400)
    cfg_off = Config()
    cfg_off.signal.min_candles = 30
    assert cfg_off.signal.vwap.enabled is False
    off = scan_symbol("TESTUSDT", cs, cfg_off, threshold=0.0)
    off2 = scan_symbol("TESTUSDT", cs, cfg_off, threshold=0.0, prune=False)
    assert off == off2 and off

    cfg_on = copy.deepcopy(cfg_off)
    cfg_on.signal.vwap = vcfg(anchor_mode="impulse_low")
    on = scan_symbol("TESTUSDT", cs, cfg_on, threshold=0.0)
    assert set(e.entry_idx for e in on) <= set(e.entry_idx for e in off)


# ---------------------------------------------------------------------------
# 11. Interval 1m
# ---------------------------------------------------------------------------

def test_interval_1m_pump_55_candle_lalu_masih_di_window():
    cs = flat(100)
    k = len(cs) - 55
    cs[k] = mk_candle(k, o=1.0, c=1.1, vol=100.0)
    idx, mode, _ = _find(cs)
    assert mode == "pump_start" and idx == k


def test_interval_1m_pump_65_candle_lalu_di_luar_window():
    cs = flat(100)
    k = len(cs) - 65
    cs[k] = mk_candle(k, o=1.0, c=1.1, vol=100.0)
    idx, mode, _ = _find(cs, no_anchor_action="block")
    assert idx is None and mode == "pump_start"


# ---------------------------------------------------------------------------
# kontrak modul
# ---------------------------------------------------------------------------

def test_filter_tidak_masuk_all_detectors():
    from bot.signal_engine.detectors import ALL_DETECTORS
    assert "vwap" not in ALL_DETECTORS
    assert FILTER_DETECTORS["vwap"] is VWAPFilter
    assert VWAPFilter.name == "vwap"


def test_details_lengkap():
    cs = flat(30)
    buf = mk_buf(cs, last_price=1.0)
    d = VWAPFilter().score(buf, cfg_with(anchor_mode="impulse_low")).details
    for key in ("vwap", "price", "dist_pct", "anchor_open_time",
                "anchor_mode_used", "n_candles", "passed", "reason"):
        assert key in d
    assert math.isfinite(d["vwap"])


# ---------------------------------------------------------------------------
# 12. Toggle runtime (dashboard)
# ---------------------------------------------------------------------------

def test_runtime_params_memuat_toggle_vwap():
    from bot.risk_management.manager import RiskManager
    cfg = Config()
    cfg.signal.vwap = vcfg()
    rm = RiskManager(cfg)
    assert rm.params.vwap_filter_enabled is True
    assert "vwap_filter_enabled" in rm.params.to_dict()
    assert rm.update_params({"vwap_filter_enabled": False}) == []
    assert rm.params.vwap_filter_enabled is False
    assert rm.update_params({"vwap_filter_enabled": "ya"})
