"""
Unit test STATISTIK PERFORMA: win rate, profit factor, expectancy,
maximum drawdown, dan PnL harian.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.risk_management.stats import compute_stats, daily_pnl, max_drawdown


def test_stats_empty():
    s = compute_stats([])
    assert s["total_trades"] == 0
    assert s["win_rate_pct"] == 0.0
    assert s["profit_factor"] is None


def test_stats_mixed():
    trades = [
        {"realized_pnl": 100},    # win
        {"realized_pnl": 50},     # win
        {"realized_pnl": -40},    # loss
        {"realized_pnl": -60},    # loss
        {"realized_pnl": 0.0},    # breakeven (dihitung loss karena <= 0)
    ]
    s = compute_stats(trades)
    assert s["total_trades"] == 5
    assert s["wins"] == 2
    assert s["losses"] == 3
    assert abs(s["win_rate_pct"] - 40.0) < 1e-9
    assert abs(s["avg_win"] - 75.0) < 1e-9
    assert abs(s["avg_loss"] - (100 / 3)) < 1e-3   # dibulatkan 4 desimal
    # PF = gross profit / gross loss = 150 / 100 = 1.5
    assert abs(s["profit_factor"] - 1.5) < 1e-9
    # expectancy = total / n = 50 / 5 = 10
    assert abs(s["expectancy"] - 10.0) < 1e-9
    assert abs(s["total_pnl"] - 50.0) < 1e-9
    assert s["best_trade"] == 100 and s["worst_trade"] == -60


def test_stats_all_wins():
    s = compute_stats([{"realized_pnl": 10}, {"realized_pnl": 20}])
    assert s["profit_factor"] == "inf"
    assert s["win_rate_pct"] == 100.0


def test_max_drawdown():
    dd = max_drawdown([100, 120, 90, 130, 125, 140])
    # puncak 120 -> lembah 90 = 25% drawdown
    assert abs(dd["max_dd_pct"] - 25.0) < 1e-9
    assert dd["peak"] == 120 and dd["trough"] == 90


def test_max_drawdown_monotonic_up():
    dd = max_drawdown([100, 110, 120, 130])
    assert dd["max_dd_pct"] == 0.0


def test_max_drawdown_short_series():
    assert max_drawdown([])["max_dd_pct"] == 0.0
    assert max_drawdown([100])["max_dd_pct"] == 0.0


def test_daily_pnl():
    d = daily_pnl(10_500, 10_000)
    assert d["pnl"] == 500 and d["pnl_pct"] == 5.0
    d2 = daily_pnl(9_800, 10_000)
    assert d2["pnl"] == -200 and d2["pnl_pct"] == -2.0
    assert daily_pnl(100, 0)["pnl_pct"] == 0.0
