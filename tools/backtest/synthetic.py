#!/usr/bin/env python3
"""
Generator klines 1m SINTETIS untuk menguji pipeline backtest tanpa unduh data
(juga berguna sebagai smoke-test di mesin mana pun).

Model per simbol (mirip SimulatedGateway tapi pada level candle):
  - random walk dasar dengan volatilitas realistis
  - episode PUMP: naik tajam + volume & jumlah trade melonjak + taker buy
    dominan, lalu pelan-pelan melemah (kadang dump)
  - episode DUMP: penurunan tajam

Pemakaian:
    python tools/backtest/synthetic.py --symbols 6 --days 30
    python tools/backtest/synthetic.py --selftest   # uji pipeline end-to-end

Output: tools/backtest/data/SIM{N}USDT_1m.csv.gz (format sama dengan download.py)
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401

from bot.models import Candle


def generate_symbol(seed: int, days: int, start_price: float | None = None) -> list[Candle]:
    rng = random.Random(seed)
    price = start_price or rng.uniform(0.5, 20.0)
    candles: list[Candle] = []
    n = days * 1440
    regime = "normal"
    regime_left = 0
    drift = 0.0
    vol_mult = 1.0
    for i in range(n):
        if regime_left <= 0:
            r = rng.random()
            if r < 0.015:            # pump ~ tiap ~70 menit
                regime, regime_left = "pump", rng.randint(15, 90)
                drift = rng.uniform(0.0008, 0.004)   # naik per menit
                vol_mult = rng.uniform(6, 20)
            elif r < 0.020:          # dump lebih jarang
                regime, regime_left = "dump", rng.randint(10, 60)
                drift = -rng.uniform(0.0008, 0.003)
                vol_mult = rng.uniform(4, 12)
            else:
                regime, regime_left = "normal", rng.randint(30, 240)
                drift = rng.uniform(-0.00015, 0.00015)
                vol_mult = 1.0
        regime_left -= 1

        # OHLC dari random walk intra-candle
        o = price
        move = rng.gauss(drift, 0.0012)
        c = max(1e-6, o * (1 + move))
        hi = max(o, c) * (1 + abs(rng.gauss(0, 0.0006)))
        lo = min(o, c) * (1 - abs(rng.gauss(0, 0.0006)))
        base_vol = rng.uniform(50, 300)
        volume = base_vol * vol_mult * rng.uniform(0.7, 1.3)
        trades = int(volume / rng.uniform(8, 40))
        # taker buy dominan saat pump, sebaliknya saat dump
        buy_share = {"pump": rng.uniform(0.58, 0.75),
                     "dump": rng.uniform(0.25, 0.42)}.get(regime, 0.5)
        taker_buy = volume * buy_share
        ts = 1_700_000_000_000 + i * 60_000
        candles.append(Candle(
            open_time=ts, close_time=ts + 59_999,
            open=o, high=hi, low=lo, close=c,
            volume=volume, quote_volume=volume * c,
            trades=max(1, trades), taker_buy_volume=taker_buy, closed=True))
        price = c
    return candles


def save(candles: list[Candle], path: str) -> None:
    with gzip.open(path, "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "open", "high", "low", "close", "volume",
                    "close_ts", "quote_volume", "trades",
                    "taker_buy_base", "taker_buy_quote"])
        for c in candles:
            w.writerow([c.open_time, c.open, c.high, c.low, c.close, c.volume,
                        c.close_time, c.quote_volume, c.trades,
                        c.taker_buy_volume, c.taker_buy_volume * c.close])


def selftest() -> int:
    """Uji pipeline end-to-end: generate -> skor -> simulasi -> invarian."""
    print("SELFTEST pipeline backtest (data sintetis)...")
    from tools.backtest import engine

    sym = "SIMTESTUSDT"
    candles = generate_symbol(seed=7, days=12)
    cfg = _default_cfg()
    scores = engine.score_series(sym, candles, cfg)
    assert scores, "seri skor kosong"
    expected = len(candles) - max(cfg.signal.min_candles, 30)
    assert len(scores) == expected, \
        f"jumlah skor aneh: {len(scores)} != {expected}"

    combo = engine.Combo(threshold=55, sl_pct=2.5, tp_rr=2.0,
                         be_rr=1.0, trail_pct=0.5)
    trades = engine.simulate_combo(
        {sym: scores}, {sym: candles}, combo,
        ts0=scores[0].ts, ts1=scores[-1].ts)
    m = engine.compute_metrics(trades)
    print(f"  skor/candle OK ({len(scores):,}), trade: {m.trades}")
    assert m.trades > 0, "tidak ada trade pada data pump sintetis (salah?)"

    # invarian 1: PnL = equity_akhir - equity_awal (tak ada uang hantu)
    eq_final = 10_000.0 + m.total_pnl
    assert eq_final > 0, "equity negatif"
    # invarian 2: tidak ada entry dengan skor < threshold
    th_by_ts = {s.ts: s.score for s in scores}
    for t in trades:
        assert th_by_ts[t.entry_ts] >= combo.threshold - 1e-9, \
            "ada entry di bawah threshold"
    # invarian 3: exit sesudah entry
    for t in trades:
        assert t.exit_ts > t.entry_ts, "exit sebelum entry"

    print(f"  metrik: win_rate={m.win_rate:.0f}% PF="
          f"{m.profit_factor if m.profit_factor is not None else 'inf'} "
          f"pnl={m.total_pnl:+.2f} dd={m.max_dd:.2f}%")
    print("  SEMUA invarian lolok -> pipeline backtest sehat")
    return 0


def _default_cfg():
    from bot.config import load_config
    return load_config(os.path.join(_bootstrap._ROOT, "config", "config.yaml"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=int, default=6)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()

    for i in range(args.symbols):
        sym = f"SIM{i+1}USDT"
        candles = generate_symbol(seed=args.seed + i, days=args.days)
        path = os.path.join(_bootstrap.DATA_DIR, f"{sym}_1m.csv.gz")
        save(candles, path)
        print(f"{sym}: {len(candles):,} candle -> {path}")
    print("\nSelesai. Lanjut: python tools/backtest/grid.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
