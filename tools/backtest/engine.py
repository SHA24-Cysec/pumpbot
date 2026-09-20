"""
Engine simulasi backtest: satu trade dan satu portofolio per kombinasi parameter.

Semua keputusan exit memakai fungsi murni milik bot (bot.risk_management.stops
dan bot.risk_management.sizing) agar hasil sejajar dengan PositionManager.

ATURAN INTRABAR PESIMISTIS (candle tidak menyimpan urutan tick):
  1. Entry terisi di OPEN candle i+1 dikali (1 + slippage).
  2. Pada setiap candle, SL selalu dicek LEBIH DULU daripada TP. Bila low candle
     menyentuh stop yang berlaku, exit terjadi di min(stop, open) dikurangi
     slippage, sehingga gap turun diisi pada open yang lebih buruk.
  3. Baru setelah itu TP diperiksa: exit di max(TP, open) tanpa slippage karena
     TP adalah limit order.
  4. Pembaruan highest, breakeven, dan trailing dilakukan PALING AKHIR dan hanya
     berlaku untuk candle BERIKUTNYA. Stop yang naik pada candle ini tidak
     dipakai untuk candle yang sama (mencegah lookahead intrabar).

Urutan breakeven lalu trailing meniru PositionManager: trailing hanya aktif
setelah breakeven terpicu, trailing monoton naik, dan stop baru hanya
dipublikasikan bila kenaikannya memenuhi update_step_pct.

ASUMSI: filter LOT_SIZE dan MIN_NOTIONAL diabaikan (qty dianggap bisa pecahan
apa pun), dan tidak ada partial fill.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from bot.config import Config
from bot.models import Candle
from bot.risk_management.sizing import clamp_to_available_balance, compute_raw_qty
from bot.risk_management.stops import (
    breakeven_price,
    initial_stop,
    should_trigger_breakeven,
    should_update_exit_order,
    take_profit_levels,
    update_trailing,
)

from tools.backtest.signals import Entry


@dataclass(frozen=True)
class Params:
    """Satu kombinasi parameter exit yang diuji."""
    sl_pct: float
    tp_rr: float
    be_rr: float = 0.0            # 0 = breakeven mati
    be_buffer_pct: float = 0.25
    trail_pct: float = 0.0        # 0 = trailing mati
    trail_step_pct: float = 0.15
    fee_pct: float = 0.1
    slippage_pct: float = 0.0
    cooldown_min: Optional[float] = None   # None = pakai nilai config

    def key(self) -> tuple:
        """Kunci unik untuk deduplikasi grid."""
        return (self.sl_pct, self.tp_rr, self.be_rr, self.be_buffer_pct,
                self.trail_pct, self.trail_step_pct, self.fee_pct,
                self.slippage_pct, self.cooldown_min)


@dataclass
class TradeResult:
    """Hasil satu trade tersimulasi."""
    symbol: str
    ts_entry: int
    ts_exit: int
    entry: float
    exit: float
    stop_initial: float
    tp: float
    reason: str          # SL | TP | END
    exit_idx: int
    be_triggered: bool = False
    qty: float = 0.0
    pnl: float = 0.0
    r_multiple: float = 0.0


def simulate_trade(candles: list[Candle], entry_idx: int, params: Params,
                   cfg: Config, symbol: str = "") -> Optional[TradeResult]:
    """
    Simulasikan satu trade mulai dari candle entry_idx.

    Return None bila data tidak cukup atau harga entry tidak valid.
    """
    n = len(candles)
    if not 0 <= entry_idx < n:
        return None
    first = candles[entry_idx]
    entry = first.open * (1.0 + params.slippage_pct / 100.0)
    if entry <= 0:
        return None

    stop0 = initial_stop(
        entry=entry,
        swing_low=None,
        mode="percent",
        percent_pct=params.sl_pct,
        min_stop_pct=cfg.stops.min_stop_pct,
        max_stop_pct=cfg.stops.max_stop_pct,
    )
    if stop0 <= 0 or stop0 >= entry:
        return None

    levels = take_profit_levels(entry, stop0, "rr", params.tp_rr, [])
    if not levels:
        return None
    tp = float(levels[0]["price"])

    stop = stop0
    highest = entry
    be_done = False
    trail_on = False

    exit_price = None
    reason = "END"
    exit_idx = n - 1

    for i in range(entry_idx, n):
        c = candles[i]

        # a) stop loss diperiksa lebih dulu (pesimistis)
        if c.low <= stop:
            exit_price = min(stop, c.open) * (1.0 - params.slippage_pct / 100.0)
            reason = "SL"
            exit_idx = i
            break

        # b) take profit (limit order, tanpa slippage)
        if c.high >= tp:
            exit_price = max(tp, c.open)
            reason = "TP"
            exit_idx = i
            break

        # c) pembaruan stop untuk candle BERIKUTNYA
        highest = max(highest, c.high)

        if params.be_rr > 0 and not be_done:
            if should_trigger_breakeven(price=highest, entry=entry,
                                        initial_stop=stop0,
                                        trigger_rr=params.be_rr):
                new_sl = breakeven_price(entry, params.be_buffer_pct,
                                         params.fee_pct)
                if new_sl > stop:
                    stop = new_sl
                be_done = True

        # Trailing hanya boleh aktif setelah breakeven (sama seperti bot).
        if params.trail_pct > 0 and be_done:
            trail_on = True
        if trail_on:
            new_sl = update_trailing(
                current_sl=stop, highest=highest, entry=entry,
                mode="percent", percent_pct=params.trail_pct,
                atr_value=0.0, atr_multiplier=0.0,
            )
            if should_update_exit_order(stop, new_sl, params.trail_step_pct):
                stop = new_sl

    if exit_price is None:
        exit_price = candles[-1].close
        exit_idx = n - 1
        reason = "END"

    return TradeResult(
        symbol=symbol,
        ts_entry=first.open_time,
        ts_exit=candles[exit_idx].close_time,
        entry=entry,
        exit=max(exit_price, 0.0),
        stop_initial=stop0,
        tp=tp,
        reason=reason,
        exit_idx=exit_idx,
        be_triggered=be_done,
    )


# ---------------------------------------------------------------------------
# Portofolio
# ---------------------------------------------------------------------------

@dataclass
class PortfolioResult:
    """Hasil simulasi portofolio untuk satu kombinasi parameter."""
    params: Params
    trades: list[TradeResult] = field(default_factory=list)
    equity_curve: list[tuple[int, float]] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


def compute_metrics(trades: list[TradeResult],
                    equity_curve: list[tuple[int, float]],
                    start_equity: float) -> dict:
    """Hitung metrik ringkas dari daftar trade dan kurva equity."""
    n = len(trades)
    if start_equity <= 0:
        start_equity = 1.0
    end_equity = equity_curve[-1][1] if equity_curve else start_equity

    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl < 0]
    gross_win = sum(t.pnl for t in wins)
    gross_loss = abs(sum(t.pnl for t in losses))

    if gross_loss > 0:
        profit_factor = gross_win / gross_loss
    else:
        profit_factor = float("inf") if gross_win > 0 else 0.0

    peak = start_equity
    max_dd = 0.0
    for _, eq in equity_curve:
        peak = max(peak, eq)
        if peak > 0:
            max_dd = max(max_dd, (peak - eq) / peak * 100.0)

    r_values = [t.r_multiple for t in trades]
    avg_r = sum(r_values) / n if n else 0.0
    win_rate = (len(wins) / n * 100.0) if n else 0.0
    avg_win = (gross_win / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    expectancy = ((win_rate / 100.0) * avg_win
                  - (1.0 - win_rate / 100.0) * avg_loss)

    return {
        "trades": n,
        "win_rate": round(win_rate, 2),
        "net_return_pct": round((end_equity / start_equity - 1.0) * 100.0, 4),
        "profit_factor": (round(profit_factor, 4)
                          if profit_factor != float("inf") else float("inf")),
        "max_dd_pct": round(max_dd, 4),
        "avg_r": round(avg_r, 4),
        "expectancy": round(expectancy, 6),
        "end_equity": round(end_equity, 6),
    }


def simulate_portfolio(entries: list[Entry], data: dict[str, list[Candle]],
                       params: Params, cfg: Config,
                       start_equity: float = 1000.0,
                       risk_pct: Optional[float] = None,
                       presorted: bool = False) -> PortfolioResult:
    """
    Simulasi portofolio kronologis dengan satu posisi terbuka saja.

    Sesuai risk.allow_multiple_positions=false pada config: entry yang muncul
    saat posisi lain masih berjalan dilewati, dan cooldown per simbol
    (signal.cooldown_after_exit_min) diberlakukan setelah exit.

    presorted=True berarti pemanggil menjamin entries sudah terurut menurut
    (ts_entry, symbol) persis seperti keluaran scan_all, sehingga sort ulang
    yang mahal pada himpunan entry besar dilewati.
    """
    risk = float(cfg.risk.risk_per_trade_pct if risk_pct is None else risk_pct)
    # cooldown dapat dioverride per kombinasi (grid), None = ikut config.
    cd_min = (cfg.signal.cooldown_after_exit_min
              if params.cooldown_min is None else params.cooldown_min)
    cooldown_ms = max(0, int(cd_min)) * 60_000
    fee_rate = params.fee_pct / 100.0

    equity = float(start_equity)
    curve: list[tuple[int, float]] = []
    trades: list[TradeResult] = []
    busy_until = -1                 # waktu exit posisi berjalan (ms)
    cooldown: dict[str, int] = {}

    ordered = entries if presorted else sorted(
        entries, key=lambda x: (x.ts_entry, x.symbol))
    for e in ordered:
        if e.ts_entry <= busy_until:
            continue
        if e.ts_entry < cooldown.get(e.symbol, -1):
            continue
        candles = data.get(e.symbol)
        if not candles or not 0 <= e.entry_idx < len(candles):
            continue
        if equity <= 0:
            break

        res = simulate_trade(candles, e.entry_idx, params, cfg, e.symbol)
        if res is None:
            continue

        raw = compute_raw_qty(equity, risk, res.entry, res.stop_initial)
        qty = clamp_to_available_balance(raw, res.entry, equity)
        if qty <= 0:
            continue

        entry_notional = qty * res.entry
        exit_notional = qty * res.exit
        fees = (entry_notional + exit_notional) * fee_rate
        pnl = exit_notional - entry_notional - fees
        risk_quote = qty * (res.entry - res.stop_initial)

        res.qty = qty
        res.pnl = pnl
        res.r_multiple = (pnl / risk_quote) if risk_quote > 0 else 0.0

        equity += pnl
        trades.append(res)
        curve.append((res.ts_exit, equity))
        busy_until = res.ts_exit
        cooldown[e.symbol] = res.ts_exit + cooldown_ms

    metrics = compute_metrics(trades, curve, start_equity)
    return PortfolioResult(params=params, trades=trades,
                           equity_curve=curve, metrics=metrics)
