"""
Statistik performa - PURE FUNCTIONS (target unit test).

Dihitung dari daftar trade yang sudah close + deret equity:
  * win rate, rata-rata profit/rugi per trade
  * profit factor (gross profit / gross loss)
  * expectancy
  * maximum drawdown dari equity curve
"""

from __future__ import annotations

from typing import Optional


def compute_stats(closed_trades: list[dict]) -> dict:
    """
    closed_trades: list dict dengan minimal key "realized_pnl"
    (positif = profit, negatif = rugi, quote asset).
    """
    n = len(closed_trades)
    if n == 0:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate_pct": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "profit_factor": None,
            "expectancy": 0.0, "total_pnl": 0.0, "best_trade": 0.0,
            "worst_trade": 0.0,
        }

    pnls = [float(t.get("realized_pnl", 0.0) or 0.0) for t in closed_trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))          # positif
    total = sum(pnls)

    profit_factor: Optional[float]
    if gross_loss == 0:
        profit_factor = None if gross_profit == 0 else float("inf")
    else:
        profit_factor = gross_profit / gross_loss

    win_rate = len(wins) / n * 100.0
    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    expectancy = total / n

    return {
        "total_trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate, 2),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "profit_factor": (None if profit_factor is None
                          else (round(profit_factor, 3)
                                if profit_factor != float("inf") else "inf")),
        "expectancy": round(expectancy, 4),
        "total_pnl": round(total, 4),
        "best_trade": round(max(pnls), 4),
        "worst_trade": round(min(pnls), 4),
    }


def max_drawdown(equity_series: list[float]) -> dict:
    """
    Maximum drawdown dari deret equity (berurutan waktu).

    DD = (puncak - nilai saat ini) / puncak. MDD = DD terburuk.
    Return {"max_dd_pct", "peak", "trough"}.
    """
    if len(equity_series) < 2:
        return {"max_dd_pct": 0.0, "peak": equity_series[0] if equity_series else 0.0,
                "trough": equity_series[0] if equity_series else 0.0}

    peak = equity_series[0]
    max_dd = 0.0
    dd_peak, dd_trough = peak, peak
    for v in equity_series:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            dd_peak, dd_trough = peak, v
    return {"max_dd_pct": round(max_dd * 100.0, 3),
            "peak": round(dd_peak, 2), "trough": round(dd_trough, 2)}


def daily_pnl(equity_now: float, equity_start_of_day: float) -> dict:
    """PnL harian (nominal & persen) dari equity awal hari."""
    if equity_start_of_day <= 0:
        return {"pnl": 0.0, "pnl_pct": 0.0}
    pnl = equity_now - equity_start_of_day
    return {"pnl": round(pnl, 4), "pnl_pct": round(pnl / equity_start_of_day * 100.0, 4)}
