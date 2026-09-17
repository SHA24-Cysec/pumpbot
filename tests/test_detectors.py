"""
Unit test DETECTOR SIGNAL ENGINE.

Memberi makan buffer sintetis lalu memastikan tiap detector memberi skor /
gate / veto yang masuk akal. Ini menjaga logika deteksi tidak diam-diam rusak
saat diedit.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.models import BookSnapshot, Candle, Ticker24h, Trade
from bot.signal_engine.detectors import (
    ManipulationDetector,
    OrderBookDetector,
    PriceActionDetector,
    TradeFlowDetector,
    VolumeDetector,
    WhaleDetector,
)
from bot.utils import now_ms


# ---------------------------------------------------------------------------
# helper pembangun data sintetis
# ---------------------------------------------------------------------------

def mk_buf(n_candles=60, *, start_price=1.0, drift=0.0, vol=1.0,
           trades_per_candle=10, spread_bps=1.0, qty_per_level=100.0):
    """Buffer dengan candle menit berjalan mundur dari sekarang."""
    buf = SymbolBuffer("TESTUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    price = start_price
    for k in range(n_candles, 0, -1):
        o = price
        c = price * (1 + drift)
        hi, lo = max(o, c) * 1.001, min(o, c) * 0.999
        buf.on_candle(Candle(
            open_time=base - k * 60_000, close_time=base - k * 60_000 + 59_999,
            open=o, high=hi, low=lo, close=c, volume=vol, quote_volume=vol * c,
            trades=trades_per_candle, taker_buy_volume=vol * 0.5, closed=True))
        price = c
    mid = price
    tick = mid * spread_bps / 10_000 / 2
    bids = [(mid - tick * (i + 1), qty_per_level) for i in range(20)]
    asks = [(mid + tick * (i + 1), qty_per_level) for i in range(20)]
    buf.on_book(BookSnapshot(ts=now_ms(), bids=bids, asks=asks))
    buf.on_ticker(Ticker24h(ts=now_ms(), symbol="TESTUSDT", last_price=mid,
                            price_change_pct=0.0, high=mid * 1.01, low=mid * 0.99,
                            volume=1000, quote_volume=1000 * mid, trade_count=10_000,
                            bid=bids[0][0], ask=asks[0][0]))
    return buf


def add_trades(buf, n, *, qty=1.0, buyer_is_maker=False, age_ms=0, price=1.0):
    for _ in range(n):
        buf.on_trade(Trade(ts=now_ms() - age_ms, price=price, qty=qty,
                           buyer_is_maker=buyer_is_maker))


# ---------------------------------------------------------------------------
# 1. Order book
# ---------------------------------------------------------------------------

def test_orderbook_heavy_bids_scores_high():
    cfg = Config()
    buf = mk_buf()
    mid = buf.last_price
    # bid 100 per level vs ask 10 per level -> rasio ~0.91 -> skor maksimal
    bids = [(mid - 0.0001 * (i + 1), 100.0) for i in range(20)]
    asks = [(mid + 0.0001 * (i + 1), 10.0) for i in range(20)]
    buf.on_book(BookSnapshot(ts=now_ms(), bids=bids, asks=asks))
    r = OrderBookDetector().score(buf, cfg)
    assert r.eligible
    assert r.score > 90, r.details
    assert r.details["bid_ask_ratio"] > 0.85


def test_orderbook_balanced_scores_low():
    cfg = Config()
    buf = mk_buf()   # qty sama kedua sisi
    r = OrderBookDetector().score(buf, cfg)
    assert r.score < 15, r.details


def test_orderbook_ask_wall_penalizes():
    cfg = Config()
    buf = mk_buf()
    mid = buf.last_price
    # seimbang, tapi ada ask wall raksasa tepat di atas harga
    bids = [(mid - 0.0001 * (i + 1), 100.0) for i in range(20)]
    asks = [(mid + 0.0001 * (i + 1), 10.0) for i in range(20)]
    asks[2] = (asks[2][0], 5000.0)
    buf.on_book(BookSnapshot(ts=now_ms(), bids=bids, asks=asks))
    r_wall = OrderBookDetector().score(buf, cfg)
    # bandingkan tanpa wall
    asks[2] = (asks[2][0], 10.0)
    buf.on_book(BookSnapshot(ts=now_ms(), bids=bids, asks=asks))
    r_clean = OrderBookDetector().score(buf, cfg)
    assert r_wall.score < r_clean.score
    assert r_wall.details["ask_wall"] > 0


# ---------------------------------------------------------------------------
# 2. Trade flow & spread gate
# ---------------------------------------------------------------------------

def test_trade_flow_spike_and_buy_pressure():
    cfg = Config()
    buf = mk_buf(trades_per_candle=10)   # baseline 10 trade/menit
    # 60 trade agresif BELI dalam 1 menit terakhir -> rasio 6x
    add_trades(buf, 60, buyer_is_maker=False, qty=2.0, price=buf.last_price)
    r = TradeFlowDetector().score(buf, cfg)
    assert r.eligible
    assert r.score > 80, r.details
    assert r.details["tpm_ratio"] >= 5.5
    assert r.details["buy_ratio"] > 0.9


def test_trade_flow_quiet_scores_low():
    cfg = Config()
    buf = mk_buf(trades_per_candle=50)   # baseline tinggi, tanpa trade baru
    add_trades(buf, 5, buyer_is_maker=True, price=buf.last_price)
    r = TradeFlowDetector().score(buf, cfg)
    assert r.score < 25, r.details


def test_trade_flow_spread_gate_blocks_entry():
    cfg = Config()
    buf = mk_buf(spread_bps=40.0)        # spread 0.4% -> jauh di atas limit 15 bps
    add_trades(buf, 100, price=buf.last_price)
    r = TradeFlowDetector().score(buf, cfg)
    assert r.eligible is False
    assert "spread" in r.details.get("reason", "")


# ---------------------------------------------------------------------------
# 3. Volume
# ---------------------------------------------------------------------------

def test_volume_spike_scores_high():
    cfg = Config()
    buf = mk_buf(n_candles=40, vol=1.0)
    # candle terakhir volume 5x rata-rata
    last = buf.candles[-1]
    last.volume = 5.0
    last.quote_volume = 5.0 * last.close
    r = VolumeDetector().score(buf, cfg)
    assert r.eligible
    assert r.score > 90, r.details
    assert r.details["volume_spike"] >= 4.9


def test_volume_flat_scores_low():
    cfg = Config()
    buf = mk_buf(n_candles=40, vol=1.0)
    r = VolumeDetector().score(buf, cfg)
    assert r.score < 5, r.details


def test_volume_insufficient_candles():
    cfg = Config()
    buf = mk_buf(n_candles=10, vol=1.0)
    r = VolumeDetector().score(buf, cfg)
    assert r.eligible is False


# ---------------------------------------------------------------------------
# 4. Whale & spoofing
# ---------------------------------------------------------------------------

def test_whale_net_buy_scores_high():
    cfg = Config()
    buf = mk_buf()
    # 100 trade kecil (notional 100) dalam window buffer (10 menit) -> rata2 100
    add_trades(buf, 100, qty=100.0, age_ms=5 * 60_000, price=1.0)
    # 3 whale beli @ notional 20.000 dalam 3 menit terakhir
    for _ in range(3):
        buf.on_trade(Trade(ts=now_ms(), price=1.0, qty=20_000.0,
                           buyer_is_maker=False))
    r = WhaleDetector().score(buf, cfg)
    assert r.eligible
    # 0.6 x whale_score(100) + 0.4 x spoof(0) = 60
    assert r.score >= 60, r.details
    assert r.details["whale_net_usd"] >= 50_000


def test_whale_spoof_ask_wall_disappears():
    cfg = Config()
    buf = mk_buf()
    add_trades(buf, 100, qty=100.0, age_ms=5 * 60_000, price=1.0)
    mid = buf.last_price
    # snapshot LAMA: ada ask wall raksasa
    old_bids = [(mid - 0.0001 * (i + 1), 100.0) for i in range(20)]
    old_asks = [(mid + 0.0001 * (i + 1), 10.0) for i in range(20)]
    old_asks[3] = (old_asks[3][0], 2000.0)
    buf.on_book(BookSnapshot(ts=now_ms() - 10_000, bids=old_bids, asks=old_asks))
    # snapshot BARU: wall hilang
    new_asks = [(mid + 0.0001 * (i + 1), 10.0) for i in range(20)]
    buf.on_book(BookSnapshot(ts=now_ms(), bids=old_bids, asks=new_asks))
    r = WhaleDetector().score(buf, cfg)
    assert r.details["spoof_bull"] >= 1, r.details


# ---------------------------------------------------------------------------
# 5. Manipulasi
# ---------------------------------------------------------------------------

def test_manipulation_pump_and_dump_veto():
    cfg = Config()
    buf = SymbolBuffer("TESTUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    # 20 candle: 5 datar -> 8 naik 1.0 -> 1.10 -> 7 turun ke ~1.04.
    # Seluruh kaki impulsif berada di dalam window 15 candle terakhir.
    prices = [1.0] * 5 + [1.0 + 0.0125 * i for i in range(1, 9)] + \
             [1.10 - 0.01 * i for i in range(1, 8)]
    for k, p in enumerate(prices):
        o = prices[k - 1] if k else 1.0
        buf.on_candle(Candle(
            open_time=base - (len(prices) - k) * 60_000,
            close_time=base - (len(prices) - k) * 60_000 + 59_999,
            open=o, high=max(o, p) * 1.001, low=min(o, p) * 0.999, close=p,
            volume=2.0, quote_volume=2.0 * p, trades=50,
            taker_buy_volume=1.0, closed=True))
    buf.on_book(BookSnapshot(ts=now_ms(),
                             bids=[(1.0399 - 0.0001 * i, 100.0) for i in range(20)],
                             asks=[(1.0401 + 0.0001 * i, 100.0) for i in range(20)]))
    buf.on_ticker(Ticker24h(ts=now_ms(), symbol="TESTUSDT", last_price=1.04,
                            price_change_pct=25.0, high=1.10, low=0.99,
                            volume=1000, quote_volume=1040, trade_count=10_000,
                            bid=1.0399, ask=1.0401))
    r = ManipulationDetector().score(buf, cfg)
    assert r.score >= cfg.signal.manipulation.veto_threshold * 100, r.details
    assert r.veto is True
    assert r.details["components"]["pump_dump"] > 0, r.details


def test_manipulation_overextension_detected():
    cfg = Config()
    buf = mk_buf(n_candles=30, drift=0.0)
    # 5 candle terakhir naik kumulatif total ~6% (di atas ambang 5%)
    candles = list(buf.candles)
    for i in range(5):
        c = candles[-5 + i]
        c.close = c.close * (1.012 ** (i + 1))
        c.high = c.close * 1.001
    buf.on_ticker(Ticker24h(ts=now_ms(), symbol="TESTUSDT",
                            last_price=candles[-1].close, price_change_pct=6.0,
                            high=candles[-1].high, low=candles[0].low,
                            volume=1000, quote_volume=1000, trade_count=1000,
                            bid=candles[-1].close * 0.9999,
                            ask=candles[-1].close * 1.0001))
    r = ManipulationDetector().score(buf, cfg)
    assert r.details["components"]["overext"] > 50, r.details


def test_manipulation_wash_trading_detected():
    cfg = Config()
    # volume 5x tapi harga nyaris datar
    buf = mk_buf(n_candles=40, drift=0.0, vol=1.0)
    for c in list(buf.candles)[-10:]:
        c.volume = 5.0
    r = ManipulationDetector().score(buf, cfg)
    assert r.details["components"]["wash"] == 100, r.details
    assert r.score > 25


def test_manipulation_calm_market_low_score():
    cfg = Config()
    buf = mk_buf(n_candles=40, drift=0.0005)   # tren sehat & tenang
    r = ManipulationDetector().score(buf, cfg)
    assert r.score < 25, r.details
    assert r.veto is False


# ---------------------------------------------------------------------------
# 6. Price action
# ---------------------------------------------------------------------------

def test_price_action_uptrend_breakout_scores_high():
    cfg = Config()
    buf = SymbolBuffer("TESTUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    # tangga naik ZIGZAG siklus 6 candle (4 naik + 2 turun sedikit) supaya
    # swing high & swing low benar-benar terbentuk (pola HH + HL)
    price = 1.0
    for k in range(1, 31):
        o = price
        step = 0.004 if k % 6 in (1, 2, 3, 4) else -0.0015
        c = price * (1 + step)
        buf.on_candle(Candle(
            open_time=base - (31 - k) * 60_000,
            close_time=base - (31 - k) * 60_000 + 59_999,
            open=o, high=max(o, c) * 1.001, low=min(o, c) * 0.999, close=c,
            volume=1.0, quote_volume=c, trades=30, taker_buy_volume=0.6,
            closed=True))
        price = c
    # candle terakhir: BREAKOUT di atas high 20 candle sebelumnya + volume 3x
    prev_high = max(c.high for c in list(buf.candles)[-21:-1])
    last = list(buf.candles)[-1]
    last.close = prev_high * 1.005
    last.high = last.close * 1.001
    last.volume = 3.0
    r = PriceActionDetector().score(buf, cfg)
    assert r.eligible
    assert r.details["breakout"] is True
    assert r.details["vol_confirm"] is True
    assert r.details["structure"] == 30.0, r.details   # HH + HL terdeteksi
    assert r.score >= 55, r.details
    # usulan stop dari swing low harus DI BAWAH harga sekarang
    assert 0 < r.details["swing_stop"] < last.close


def test_price_action_downtrend_scores_low():
    cfg = Config()
    buf = SymbolBuffer("TESTUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    price = 1.0
    for k in range(1, 31):
        o = price
        c = price * 0.996                      # stair turun
        buf.on_candle(Candle(
            open_time=base - (31 - k) * 60_000,
            close_time=base - (31 - k) * 60_000 + 59_999,
            open=o, high=max(o, c) * 1.001, low=min(o, c) * 0.999, close=c,
            volume=1.0, quote_volume=c, trades=30, taker_buy_volume=0.4,
            closed=True))
        price = c
    r = PriceActionDetector().score(buf, cfg)
    assert r.score < 25, r.details
    assert r.details["breakout"] is False


def test_price_action_fibonacci_golden_zone():
    cfg = Config()
    buf = SymbolBuffer("TESTUSDT", max_candles=500)
    base = now_ms() // 60_000 * 60_000
    # 15 candle naik 1.00 -> 1.10 (kaki impulsif), lalu 15 candle turun
    # perlahan ke 1.05 = retracement 50% (golden zone 38.2-61.8%)
    path = [1.0 + (0.10 / 14) * i for i in range(15)] + \
           [1.10 - (0.05 / 15) * i for i in range(1, 16)]
    for k, p in enumerate(path):
        o = path[k - 1] if k else 1.0
        buf.on_candle(Candle(
            open_time=base - (len(path) - k) * 60_000,
            close_time=base - (len(path) - k) * 60_000 + 59_999,
            open=o, high=max(o, p) * 1.001, low=min(o, p) * 0.999, close=p,
            volume=1.0, quote_volume=p, trades=30, taker_buy_volume=0.55,
            closed=True))
    r = PriceActionDetector().score(buf, cfg)
    # retracement (1.10 - 1.05) / (1.10 - 1.00) = 0.50 -> golden zone
    assert 0.382 <= r.details["fib_retr"] <= 0.618, r.details
