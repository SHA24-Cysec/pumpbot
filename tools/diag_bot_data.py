#!/usr/bin/env python3
"""
Diagnosis RUNTIME jalur data bot (bukan order — aman, data pasar publik).

Mereplikasi persis alur DataCollector: gateway live/testnet -> watchlist
(top max_symbols by volume, filter min_quote_volume_24h) -> seed klines
historis -> subscribe 4 stream per simbol -> buffer. Setelah N detik,
laporkan kondisi TIAP buffer dan penyebab pasti flag GATE:

    ready() = len(candles) >= signal.min_candles  DAN  book sudah terisi

Pemakaian (di mesin Anda, venv bot aktif):
    python tools/diag_bot_data.py
    python tools/diag_bot_data.py --duration 180
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import load_config  # noqa: E402
from bot.data_collector.buffers import SymbolBuffer  # noqa: E402

COUNTERS = {"candle": 0, "trade": 0, "book": 0, "ticker": 0}


def _wrap(buf: SymbolBuffer, name: str):
    """Bungkus metode buffer supaya tiap event terhitung."""
    orig = getattr(buf, f"on_{name}")

    def wrapped(*a, **kw):
        COUNTERS[name] += 1
        return orig(*a, **kw)
    setattr(buf, f"on_{name}", wrapped)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=150,
                    help="lama pemantauan aliran stream (detik)")
    args = ap.parse_args()

    from bot.config import ConfigError
    try:
        cfg = load_config("config/config.yaml")  # API key hanya bila mode testnet/live
    except ConfigError as exc:
        print(f"Konfigurasi tidak valid: {exc}", file=sys.stderr)
        return 2
    print("config : config/config.yaml")
    print(f"mode   : {cfg.mode}")
    if cfg.mode == "paper":
        print("Tool diagnosa stream memerlukan mode testnet atau live. "
              "Ubah field mode di config/config.yaml terlebih dahulu.", file=sys.stderr)
        return 2
    print(f"universe: max={cfg.universe.max_symbols} "
          f"min_vol={cfg.universe.min_quote_volume_24h:,.0f} "
          f"| min_candles={cfg.signal.min_candles} "
          f"| history_candles={cfg.data.history_candles}")

    from bot.exchange.binance_gateway import BinanceGateway
    gw = BinanceGateway(cfg.mode,
                        os.getenv("BINANCE_API_KEY", ""),
                        os.getenv("BINANCE_API_SECRET", ""),
                        quote_asset=cfg.quote_asset,
                        depth_levels=cfg.data.depth_levels)

    # ---- 1. watchlist (mirror DataCollector.build_watchlist) ----
    t0 = time.time()
    tickers = await gw.get_universe()
    eligible = [t for t in tickers
                if t.quote_volume >= cfg.universe.min_quote_volume_24h
                and t.symbol not in (cfg.universe.exclude_symbols or [])]
    eligible.sort(key=lambda t: t.quote_volume, reverse=True)
    watch = [t.symbol for t in eligible[: cfg.universe.max_symbols]]
    print(f"\nwatchlist: {len(watch)} simbol "
          f"(dari {len(tickers)} ticker, {len(eligible)} lolos filter) "
          f"dalam {time.time()-t0:.1f}s")

    # ---- 2. buffer + seed historis (mirror DataCollector.start) ----
    d = cfg.data
    buffers: dict[str, SymbolBuffer] = {}
    for sym in watch:
        buf = SymbolBuffer(sym, max_candles=max(720, d.history_candles),
                           max_trades=d.trade_buffer_max,
                           trade_window_sec=d.trade_window_sec,
                           book_history_sec=d.book_history_sec)
        for n in ("candle", "trade", "book", "ticker"):
            _wrap(buf, n)
        buffers[sym] = buf

    print("seed klines historis (paralel 3)...")
    sem = asyncio.Semaphore(3)
    seed_fail: list[str] = []

    async def seed(sym: str):
        async with sem:
            try:
                candles = await gw.get_klines(sym, d.kline_interval,
                                              d.history_candles)
                for c in candles:
                    buffers[sym].on_candle(c)
            except Exception as exc:
                seed_fail.append(f"{sym}: {type(exc).__name__}: {exc}")

    t0 = time.time()
    await asyncio.gather(*(seed(s) for s in watch))
    seeded = sum(1 for b in buffers.values()
                 if len(b.candles) >= cfg.signal.min_candles)
    print(f"seed selesai {time.time()-t0:.0f}s: {seeded}/{len(watch)} simbol "
          f">= {cfg.signal.min_candles} candle"
          + (f" | GAGAL: {seed_fail[:5]}" if seed_fail else ""))

    # ---- 3. subscribe stream real-time ----
    t0 = time.time()
    await gw.subscribe(
        symbols=watch,
        on_candle=lambda s, c: buffers[s].on_candle(c),
        on_trade=lambda s, t: buffers[s].on_trade(t),
        on_book=lambda s, b: buffers[s].on_book(b),
        on_ticker=lambda s, t: buffers[s].on_ticker(t),
    )
    print(f"subscribe {len(watch)} simbol x 4 stream selesai "
          f"{time.time()-t0:.0f}s\n")

    # ---- 4. pantau ----
    for step in range(max(1, args.duration // 15)):
        await asyncio.sleep(15)
        ready_now = sum(1 for b in buffers.values()
                        if b.ready(cfg.signal.min_candles))
        print(f"  t+{(step+1)*15:4d}s: siap {ready_now}/{len(watch)} | "
              f"event diterima: {COUNTERS}", flush=True)

    # ---- 5. laporan akhir ----
    print("\n" + "=" * 66)
    print("LAPORAN PER BUFFER (10 teratas + ringkasan)")
    print("=" * 66)
    no_candle = no_book = ready = 0
    for sym, b in buffers.items():
        ok = b.ready(cfg.signal.min_candles)
        ready += ok
        no_candle += len(b.candles) < cfg.signal.min_candles
        no_book += b.book is None
    for sym, b in list(buffers.items())[:10]:
        print(f"  {sym:16s} candle={len(b.candles):4d} "
              f"book={'ADA' if b.book is not None else 'KOSONG':7s} "
              f"ticker={'ADA' if b.ticker is not None else 'KOSONG':7s} "
              f"trade={len(b.trades):4d} "
              f"{'READY' if b.ready(cfg.signal.min_candles) else 'GATE'}")
    print(f"\nTotal: {ready}/{len(watch)} READY | "
          f"candle kurang: {no_candle} | book kosong: {no_book}")
    print(f"Event masuk sejak start: {COUNTERS}")

    print("""
CARA MEMBACA:
- book KOSONG dominan  -> stream orderbook (depth20) tidak masuk padahal
  subscribe sukses -> kemungkinan versi SDK (pip show binance-sdk-spot)
  atau callback gagal; kirim hasil ini.
- candle kurang dominan -> seed gagal (lihat baris GAGAL di atas) DAN
  stream kline tidak menutup candle -> cek jaringan/API klines.
- Semua READY tapi dashboard bot tetap GATE -> jalur engine/collector
  bot yang bermasalah; kirim logs/pumpbot.log.
""")
    await gw.stop()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
