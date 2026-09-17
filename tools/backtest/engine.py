"""
Engine backtest berbasis klines 1m.

PRINSIP: memakai KODE BOT ASLI sebanyak mungkin —
  - bot.signal_engine.detectors   : VolumeDetector, PriceActionDetector,
                                    ManipulationDetector (makan SymbolBuffer
                                    berisi candle historis — sama seperti live)
  - bot.risk_management.stops     : initial_stop, take_profit_levels,
                                    breakeven, trailing
  - bot.risk_management.sizing    : size_position (pipeline lengkap)
sehingga hasil analisis mencerminkan logika yang benar-benar berjalan.

BATASAN (jujur, jangan diabaikan saat membaca hasil):
  1. Hanya detektor yang bisa dievaluasi dari klines yang aktif:
     trade_flow (PROXY dari trade_count & taker_buy candle — rumus sama,
     tanpa gate spread karena spread tidak ada di klines), volume,
     price_action, manipulation (penalti; wash-trading via candle).
     Order book & whale/spoof TIDAK bisa dari klines -> bobotnya
     dinormalisasi ulang ke 3 detektor tersedia. Skor di sini bukan skor
     gabungan-6-detektor live.
  2. Eksekusi: entry di OPEN candle berikutnya + slippage; jika SL dan TP
     tersentuh di candle yang sama -> SL dihitung DULU (asumsi terburuk).
  3. Portofolio: risk % memakai balance (tanpa unrealized PnL), saldo spot
     serta posisi lintas simbol direplay kronologis; PnL tertutup masuk balance.

Alur:
  score_series(symbol, candles, cfg)   -> skor per candle (SEKALI per simbol,
                                          mahal) lalu tiap kombinasi parameter
                                          cukup replay skor + simulasi posisi
                                          (murah).
  simulate(combo, scores_by_symbol)    -> daftar trade + metrik.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401

import statistics
from dataclasses import dataclass, field
from typing import Optional

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.models import Candle, SymbolFilters, Ticker24h
from bot.risk_management.sizing import size_position
from bot.risk_management.stops import (
    breakeven_price,
    should_trigger_breakeven,
    initial_stop,
    take_profit_levels,
    update_trailing,
)
from bot.signal_engine.detectors import (
    ManipulationDetector,
    PriceActionDetector,
    VolumeDetector,
    clamp01,
)

# detektor klines (orderbook & whale tidak bisa dari klines)
KLINE_DETECTORS = ("trade_flow", "volume", "price_action")


# ============================================================================
# Muat data
# ============================================================================

def load_candles(path: str) -> list[Candle]:
    """Baca satu file CSV.gz hasil download.py -> list Candle (closed)."""
    import csv
    import gzip
    out: list[Candle] = []
    with gzip.open(path, "rt", newline="") as f:
        r = csv.reader(f)
        header = next(r)
        idx = {name: i for i, name in enumerate(header)}
        for row in r:
            try:
                out.append(Candle(
                    open_time=int(row[idx["ts"]]),
                    close_time=int(row[idx["close_ts"]]),
                    open=float(row[idx["open"]]),
                    high=float(row[idx["high"]]),
                    low=float(row[idx["low"]]),
                    close=float(row[idx["close"]]),
                    volume=float(row[idx["volume"]]),
                    quote_volume=float(row[idx["quote_volume"]]),
                    trades=int(row[idx["trades"]]),
                    taker_buy_volume=float(row[idx["taker_buy_base"]]),
                    closed=True,
                ))
            except (ValueError, IndexError):
                continue
    return out


# ============================================================================
# Skor per candle (pakai detektor asli)
# ============================================================================

def _trade_flow_proxy(buf: SymbolBuffer, cfg: Config):
    """
    Proxy detektor trade_flow dari klines (rumus sama dengan
    TradeFlowDetector, tanpa gate spread):
      tpm        = jumlah trade candle terakhir (sudah close = 1 menit penuh)
      baseline   = rata-rata trades per candle dari N candle terakhir
      buy_ratio  = taker_buy_volume / volume candle terakhir
      skor       = 0.6 * spike + 0.4 * buy_pressure  (clamp 0..100)
    """
    c = cfg.signal.trade_flow
    candles = list(buf.candles)
    if not candles:
        return 0.0, False
    last = candles[-1]
    hist = candles[-c.baseline_minutes:]
    base = statistics.fmean([cd.trades for cd in hist]) if hist else 0.0
    base = max(base, 0.5)
    ratio = last.trades / base
    tpm_score = clamp01((ratio - 1.0) / max(c.spike_scale, 1e-9)) * 100
    tot = last.volume
    buy_ratio = (last.taker_buy_volume / tot) if tot > 0 else 0.5
    bp_score = clamp01((buy_ratio - 0.5) / 0.25) * 100
    return 0.6 * tpm_score + 0.4 * bp_score, True


def _synth_ticker(buf: SymbolBuffer, symbol: str) -> Ticker24h:
    """Ticker24h sintetis dari jendela candle (dipakai ManipulationDetector)."""
    candles = list(buf.candles)
    n = min(len(candles), 1440)          # ~24 jam untuk interval 1m
    win = candles[-n:]
    last = win[-1]
    first = win[0]
    chg = (last.close / first.open - 1.0) * 100 if first.open > 0 else 0.0
    return Ticker24h(
        ts=last.close_time, symbol=symbol, last_price=last.close,
        price_change_pct=chg,
        high=max(c.high for c in win), low=min(c.low for c in win),
        volume=sum(c.volume for c in win),
        quote_volume=sum(c.quote_volume for c in win),
        trade_count=sum(c.trades for c in win),
        bid=last.close, ask=last.close,
    )


@dataclass
class CandleScore:
    ts: int
    entry_idx: int            # indeks candle ENTRY di list candles (i+1)
    entry_price: float        # open candle entry
    close: float
    score: float              # skor gabungan (sudah termasuk penalti manip)
    eligible: bool
    veto: bool
    swing_low: float          # usulan SL struktur (low terendah sekitaran)


def score_series(symbol: str, candles: list[Candle], cfg: Config) -> list[CandleScore]:
    """
    Hitung skor sinyal tiap candle memakai detektor ASLI bot.
    Skor pada candle i memakai data sampai candle i (close) — entry
    disimulasikan di open candle i+1, jadi TIDAK ada look-ahead bias.
    """
    weights = cfg.signal.weights
    vol_det = VolumeDetector()
    pa_det = PriceActionDetector()
    manip_det = ManipulationDetector()
    min_candles = cfg.signal.min_candles

    # buffer besar supaya jendela 24j (manipulation) & baseline tersedia
    buf = SymbolBuffer(symbol, max_candles=1500)
    out: list[CandleScore] = []
    wsum = sum(weights.get(n, 0.0) for n in KLINE_DETECTORS)
    if wsum <= 0:
        wsum = 1.0

    for i, cd in enumerate(candles):
        buf.on_candle(cd)
        if len(buf.candles) < max(min_candles, 30):
            continue
        # ticker sintetis diperbarui tiap menit (murah: window slice)
        buf.ticker = _synth_ticker(buf, symbol)

        r_vol = vol_det.score(buf, cfg)
        r_pa = pa_det.score(buf, cfg)
        r_manip = manip_det.score(buf, cfg)
        tf_score, tf_ok = _trade_flow_proxy(buf, cfg)

        scores = {"trade_flow": tf_score, "volume": r_vol.score,
                  "price_action": r_pa.score}
        eligible = (tf_ok and r_vol.eligible and r_pa.eligible
                    and r_manip.eligible)
        base = sum(scores[n] * weights.get(n, 0.0)
                   for n in KLINE_DETECTORS) / wsum
        final = max(0.0, min(100.0, base * (
            1.0 - cfg.signal.manipulation.weight * r_manip.score / 100.0)))

        # usulan SL struktur: low terendah 30 candle terakhir
        lows = [c.low for c in list(buf.candles)[-30:]]
        swing = min(lows) if lows else cd.close * 0.98

        if i + 1 < len(candles):
            out.append(CandleScore(
                ts=cd.close_time,
                entry_idx=i + 1,
                entry_price=candles[i + 1].open,
                close=cd.close, score=final, eligible=eligible,
                veto=r_manip.veto, swing_low=swing))
    return out


# ============================================================================
# Simulasi posisi (per kombinasi parameter — MURAH, replay skor)
# ============================================================================

@dataclass
class Combo:
    """Satu kombinasi strategi yang sama dengan alur bot live.

    Tidak ada partial TP / TP1 / TP2. Setiap posisi memiliki satu target
    berbasis risk:reward, dengan BE pada ``be_rr`` lalu trailing.
    """

    threshold: float
    sl_pct: float
    tp_rr: float = 2.0
    trail_pct: float = 0.5
    risk_pct: float = 1.0
    be_rr: float = 1.0
    be_buffer_pct: float = 0.25   # sama dengan breakeven.buffer_pct live
    max_open: int = 1             # sama dengan default bot live
    fee_pct: float = 0.1          # per sisi, %
    slippage_pct: float = 0.05    # entry & SL, %
    max_hold_min: float = 0.0     # batas umur posisi (menit); 0 = nonaktif


@dataclass
class BtTrade:
    symbol: str
    entry_ts: int
    exit_ts: int
    entry: float
    exit: float
    qty: float
    pnl: float                   # sudah termasuk fee kedua sisi
    reason: str
    entry_cost: float = 0.0      # quote yang dikunci saat entry (untuk saldo)


def _filters_for(price: float) -> SymbolFilters:
    """Filter sintetis mirip simulator paper (step/tick mengikuti magnitude harga)."""
    import math
    mag = 10 ** (-max(1, int(math.log10(price)) + 4))
    return SymbolFilters(symbol="BT", tick_size=mag, step_size=mag * 10,
                         min_qty=mag * 10, max_qty=1e12, min_notional=5.0)


def _simulate_trade(symbol: str, candles: list[Candle], s: CandleScore,
                    combo: Combo, balance: float, available_quote: float,
                    ts1: int) -> Optional[BtTrade]:
    """Simulasikan satu entry dengan urutan exit yang sama seperti bot live.

    SL dihitung lebih dahulu bila rentang candle menyentuh SL dan TP bersamaan
    (asumsi konservatif karena data hanya resolusi satu menit). BE dipicu saat
    high mencapai +``be_rr`` R, lalu trailing baru boleh menaikkan SL.
    """
    entry = s.entry_price * (1 + combo.slippage_pct / 100.0)
    stop = initial_stop(entry=entry, swing_low=s.swing_low,
                        mode="percent", percent_pct=combo.sl_pct,
                        min_stop_pct=0.5, max_stop_pct=4.0)
    if stop >= entry:
        return None

    sizing = size_position(
        balance=balance, available_quote=available_quote, entry=entry, stop=stop,
        risk_pct=combo.risk_pct, filters=_filters_for(entry))
    if sizing.qty <= 0:
        return None

    # Satu TP penuh berbasis R, sama seperti take_profit.mode=rr di bot.
    target = take_profit_levels(entry, stop, "rr", combo.tp_rr, [])[0]["price"]
    qty = sizing.qty
    fee = combo.fee_pct / 100.0
    slip = combo.slippage_pct / 100.0
    qty_net = qty * (1.0 - fee)  # fee beli diasumsikan dipotong dari aset base
    pnl = -qty * fee * entry
    sl = stop
    be_done = False
    highest = entry
    entry_ms = (candles[s.entry_idx].open_time
                if s.entry_idx < len(candles) else s.ts)
    last_close_ts = entry_ms

    for k in range(s.entry_idx, len(candles)):
        c = candles[k]
        if c.open_time > ts1:
            break
        last_close_ts = c.close_time
        highest = max(highest, c.high)

        # 1) SL dahulu: worst-case bila high dan low menyentuh exit di candle sama.
        if c.low <= sl:
            px = min(c.open, sl) * (1 - slip)
            pnl += qty_net * (px - entry) - qty_net * fee * px
            reason = "TRAILING" if be_done and sl > breakeven_price(
                entry, buffer_pct=combo.be_buffer_pct, fee_pct=combo.fee_pct) else "SL"
            return BtTrade(symbol, s.ts, c.close_time, entry, px, qty, pnl,
                           reason, sizing.notional)

        # 2) Satu TP penuh.
        if c.high >= target:
            px = target
            pnl += qty_net * (px - entry) - qty_net * fee * px
            return BtTrade(symbol, s.ts, c.close_time, entry, px, qty, pnl,
                           "TP", sizing.notional)

        # 3) BE pada +1R (atau rasio yang sedang diuji), sebelum TP 2R.
        if not be_done and should_trigger_breakeven(
                price=c.high, entry=entry, initial_stop=stop,
                trigger_rr=combo.be_rr):
            be_done = True
            sl = max(sl, breakeven_price(entry, buffer_pct=combo.be_buffer_pct,
                                         fee_pct=combo.fee_pct))

        # 4) Trailing boleh berjalan hanya setelah BE aktif.
        if be_done:
            sl = update_trailing(
                current_sl=sl, highest=highest, entry=entry,
                mode="percent", percent_pct=combo.trail_pct,
                atr_value=None, atr_multiplier=2.5)

        # 5) Optional max-hold sesudah SL/TP/BE/trailing pada candle tersebut.
        if (combo.max_hold_min > 0
                and c.close_time - entry_ms >= combo.max_hold_min * 60_000):
            px = c.close * (1 - slip)
            pnl += qty_net * (px - entry) - qty_net * fee * px
            return BtTrade(symbol, s.ts, c.close_time, entry, px, qty, pnl,
                           "MAX_HOLD", sizing.notional)

    # Window berakhir: realisasikan sisa posisi pada close candle terakhir di window.
    exit_candle = next((c for c in reversed(candles[s.entry_idx:])
                        if c.open_time <= ts1), None)
    if exit_candle is None:
        return None
    px = exit_candle.close * (1 - slip)
    pnl += qty_net * (px - entry) - qty_net * fee * px
    return BtTrade(symbol, s.ts, last_close_ts, entry, px, qty, pnl,
                   "END", sizing.notional)


def _simulate_symbol(symbol: str, candles: list[Candle],
                     scores: list[CandleScore], combo: Combo,
                     equity: float, ts0: int, ts1: int) -> list[BtTrade]:
    """Helper satu simbol untuk unit test / analisis terisolasi.

    ``simulate_combo`` dipakai grid karena ia meniru balance dan posisi lintas
    pair. Helper ini sengaja mempertahankan API lama untuk test terfokus.
    """
    trades: list[BtTrade] = []
    cooldown_until = 0
    for sig in scores:
        if (sig.ts < ts0 or sig.ts >= ts1 or not sig.eligible or sig.veto
                or sig.score < combo.threshold or sig.ts < cooldown_until):
            continue
        trade = _simulate_trade(symbol, candles, sig, combo, equity, equity, ts1)
        if trade is None:
            continue
        trades.append(trade)
        cooldown_until = trade.exit_ts + 3 * 60_000
    return trades


def simulate_combo(scores_by_symbol: dict[str, list[CandleScore]],
                   candles_by_symbol: dict[str, list[Candle]],
                   combo: Combo, equity: float = 10_000.0,
                   ts0: int = 0, ts1: int = 2**62,
                   respect_max_open: bool = True) -> list[BtTrade]:
    """Replay semua signal secara kronologis dengan saldo spot yang nyata.

    Balance untuk risk sizing = quote bebas + modal entry posisi aktif. Karena
    modal posisi aktif dicatat pada harga entry, unrealized PnL tidak ikut
    mengubah risk %. Saat posisi tertutup, PnL direalisasikan ke quote bebas.
    Ini membuat satu posisi default dan mode multi sampai tiga posisi konsisten
    dengan logika bot live, termasuk keterbatasan saldo fisik.
    """
    candidates: list[tuple[int, int, str, CandleScore]] = []
    for symbol, scores in scores_by_symbol.items():
        candles = candles_by_symbol.get(symbol, [])
        for sig in scores:
            if (sig.ts < ts0 or sig.ts >= ts1 or not sig.eligible or sig.veto
                    or sig.score < combo.threshold):
                continue
            entry_ms = (candles[sig.entry_idx].open_time
                        if sig.entry_idx < len(candles) else sig.ts)
            candidates.append((entry_ms, sig.ts, symbol, sig))
    candidates.sort(key=lambda row: (row[0], row[2]))

    quote_free = equity
    active: list[BtTrade] = []
    active_symbols: set[str] = set()
    cooldown_until: dict[str, int] = {}
    trades: list[BtTrade] = []
    position_limit = combo.max_open if respect_max_open else 3
    position_limit = max(1, min(3, int(position_limit)))

    def settle_exits(until_ms: int) -> None:
        nonlocal quote_free, active
        settled: list[BtTrade] = []
        still_active: list[BtTrade] = []
        for trade in active:
            if trade.exit_ts <= until_ms:
                settled.append(trade)
            else:
                still_active.append(trade)
        active = still_active
        for trade in settled:
            # Entry sebelumnya mengurangi quote sebesar entry_cost. Di exit,
            # modal itu dan PnL ekonomis kembali menjadi saldo quote bebas.
            quote_free += trade.entry_cost + trade.pnl
            active_symbols.discard(trade.symbol)
            cooldown_until[trade.symbol] = trade.exit_ts + 3 * 60_000

    for entry_ms, signal_ts, symbol, sig in candidates:
        settle_exits(entry_ms)
        if symbol in active_symbols or signal_ts < cooldown_until.get(symbol, 0):
            continue
        if len(active) >= position_limit:
            continue

        balance = quote_free + sum(t.entry_cost for t in active)
        trade = _simulate_trade(
            symbol, candles_by_symbol[symbol], sig, combo,
            balance=balance, available_quote=quote_free, ts1=ts1)
        if trade is None:
            continue
        quote_free -= trade.entry_cost
        active.append(trade)
        active_symbols.add(symbol)
        trades.append(trade)

    return sorted(trades, key=lambda trade: trade.entry_ts)

# ============================================================================
# Metrik
# ============================================================================

@dataclass
class Metrics:
    trades: int = 0
    wins: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    profit_factor: Optional[float] = None
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_dd: float = 0.0            # % dari equity awal (kurva closed-trade)
    avg_hold_min: float = 0.0
    exit_counts: dict = field(default_factory=dict)    # reason -> jumlah
    exit_pnl_pct: dict = field(default_factory=dict)   # reason -> % modal


def compute_metrics(trades: list[BtTrade], equity: float = 10_000.0) -> Metrics:
    if not trades:
        return Metrics()
    pnls = [t.pnl for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    m = Metrics()
    m.trades = len(trades)
    m.wins = len(wins)
    m.win_rate = len(wins) / len(trades) * 100
    m.total_pnl = sum(pnls)
    m.total_pnl_pct = m.total_pnl / equity * 100
    gp, gl = sum(wins), abs(sum(losses))
    m.profit_factor = None if gp == 0 and gl == 0 else (
        float("inf") if gl == 0 else gp / gl)
    m.avg_win = (gp / len(wins)) if wins else 0.0
    m.avg_loss = (gl / len(losses)) if losses else 0.0
    # max drawdown pada kurva equity closed-trade
    curve, peak, dd = equity, equity, 0.0
    for p in pnls:
        curve += p
        peak = max(peak, curve)
        dd = max(dd, (peak - curve) / peak * 100 if peak > 0 else 0.0)
    m.max_dd = dd
    holds = [(t.exit_ts - t.entry_ts) / 60_000 for t in trades]
    m.avg_hold_min = statistics.fmean(holds)
    # rincian per alasan exit (TP / SL / MAX_HOLD / END)
    for t in trades:
        m.exit_counts[t.reason] = m.exit_counts.get(t.reason, 0) + 1
        m.exit_pnl_pct[t.reason] = (m.exit_pnl_pct.get(t.reason, 0.0)
                                    + t.pnl / equity * 100)
    return m

