#!/usr/bin/env python3
"""
Unduh klines 1m historis dari Binance (endpoint PUBLIK, tanpa API key).

Pemakaian:
    python tools/backtest/download.py --symbols BTCUSDT,ETHUSDT --months 6
    python tools/backtest/download.py --top 20 --months 6        # 20 pair
                                                                 # terlikuid
Output: tools/backtest/data/{SYMBOL}_1m.csv.gz
  kolom: ts,open,high,low,close,volume,close_ts,quote_volume,trades,
         taker_buy_base,taker_buy_quote

Catatan:
- DEFAULT: https://data-api.binance.vision — mirror MARKET-DATA PUBLIK
  resmi Binance (server Binance sungguhan, tanpa auth, tanpa geo-block).
  api.binance.com menolak request pasar-data dari sebagian jaringan
  dengan HTTP 400 (WAF/regional), padahal format request sudah benar —
  terbukti identik diterima mirror ini. Override dengan --base bila perlu.
- Rate limit klines weight 2/request; pacing 0.15 s/request aman jauh
  di bawah limit IP. Error 429/418 ditunggu sesuai header retry-after.
- Error 400/401/403/404 bersifat PERMANEN (retry tidak akan membantu)
  -> langsung gagal dengan pesan asli dari server.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import sys
import time

import _bootstrap  # noqa: F401  (path setup)

# Mirror market-data publik resmi Binance — kandidat default yang paling
# stabil utk data historis (dokumentasi: "market data only" endpoint).
DEFAULT_BASE = "https://data-api.binance.vision"

# Nama exception SDK yang menandakan error PERMANEN (jangan di-retry).
# Dicek lewat nama kelas supaya tidak bergantung pada import internal SDK.
_PERMANENT_ERRORS = {
    "BadRequestError",     # 400 — request ditolak
    "UnauthorizedError",   # 401
    "ForbiddenError",      # 403
    "NotFoundError",       # 404
    # error client-side (salah nama param dll.) — retry tidak akan membantu
    "TypeError",
    "ValueError",
    "AttributeError",
    "KeyError",
}


def _err_msg(exc: Exception) -> str:
    """Pesan error sesingkat-mungkinnya tapi informatif (code+msg Binance)."""
    msg = getattr(exc, "error_message", None) or str(exc)
    code = getattr(exc, "status_code", None)
    return f"{type(exc).__name__}: {msg}" + (f" (code={code})" if code else "")


def _retry(desc: str, fn, attempts: int = 5):
    """
    Jalankan fn() dengan backoff eksponensial. Error permanen
    (400/401/403/404) langsung di-raise dengan pesan asli server.
    """
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if type(exc).__name__ in _PERMANENT_ERRORS:
                raise RuntimeError(
                    f"{desc}: ditolak server -> {_err_msg(exc)}") from exc
            wait = min(60, 2 ** attempt * 2)
            print(f"  retry {attempt+1} ({_err_msg(exc)[:140]}): "
                  f"tunggu {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"{desc}: gagal setelah {attempts} retry")


def _client(base: str | None = None):
    from binance_sdk_spot import Spot
    from binance_common.configuration import ConfigurationRestAPI
    # klines = endpoint publik -> tanpa api key pun jalan
    return Spot(config_rest_api=ConfigurationRestAPI(
        base_path=base or DEFAULT_BASE, timeout=30_000))


def _top_symbols(n: int, quote: str, base: str | None = None) -> list[str]:
    """
    Ambil n pair dengan quote-volume 24 jam tertinggi.

    Endpoint /api/v3/ticker/24hr MEWAJIBKAN symbol/symbols (maks 100 per
    permintaan) sejak perubahan API Binance — tanpa parameter, production
    mengembalikan format mentah yang membuat parsing gagal. Maka: daftar
    simbol dari exchangeInfo dulu, lalu ticker per kelompok 100 simbol.
    """
    from bot.exchange.binance_gateway import (
        _is_pump_candidate_symbol,
        _is_requestable_symbol,
        _to_plain,
    )
    client = _client(base)

    info = _to_plain(_retry("exchangeInfo",
                            lambda: client.rest_api.exchange_info()))
    symbols = [
        s.get("symbol", "")
        for s in (info.get("symbols") or [])
        if s.get("status") == "TRADING"
        and _is_pump_candidate_symbol(s.get("symbol", ""), quote)
        and _is_requestable_symbol(s.get("symbol", ""))
        # CATATAN: listing non-ASCII (mis. "币安人生USDT") akan membuat
        # SELURUH request ticker24hr(symbols=...) ditolak 400 (-1100),
        # maka wajib disaring sebelum dikirim.
    ]

    tickers: list[tuple[str, float]] = []
    for i in range(0, len(symbols), 100):
        chunk = symbols[i:i + 100]
        resp = _retry(f"ticker24hr ({len(chunk)} simbol)",
                      lambda c=chunk: client.rest_api.ticker24hr(symbols=c))
        rows = _to_plain(resp) or []
        for t in rows:
            if not isinstance(t, dict):
                continue
            qv = float(t.get("quoteVolume", 0) or 0)
            if qv > 0:
                tickers.append((t.get("symbol", ""), qv))
        time.sleep(0.2)                   # pacing rate limit antar chunk
    tickers.sort(key=lambda x: -x[1])
    return [s for s, _ in tickers[:n]]


def download_symbol(symbol: str, months: int, out_dir: str,
                    base: str | None = None) -> str:
    import os

    from binance_sdk_spot.rest_api.models import KlinesIntervalEnum

    client = _client(base)
    end = int(time.time() * 1000)
    start = end - months * 30 * 24 * 3600 * 1000
    path = os.path.join(out_dir, f"{symbol}_1m.csv.gz")
    n = 0
    t0 = time.time()

    with gzip.open(path, "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "open", "high", "low", "close", "volume",
                    "close_ts", "quote_volume", "trades",
                    "taker_buy_base", "taker_buy_quote"])
        cursor = start
        while cursor < end:
            resp = _retry(f"{symbol} klines",
                          lambda: client.rest_api.klines(
                              symbol=symbol,
                              interval=KlinesIntervalEnum.INTERVAL_1m,
                              start_time=cursor, limit=1000))
            rows = resp.data() if hasattr(resp, "data") else resp
            if not rows:
                break
            for k in rows:
                # [openTime, open, high, low, close, volume, closeTime,
                #  quoteVolume, trades, takerBuyBase, takerBuyQuote, ...]
                w.writerow([k[0], k[1], k[2], k[3], k[4], k[5],
                            k[6], k[7], k[8], k[9], k[10]])
                n += 1
            last_open = int(rows[-1][0])
            cursor = last_open + 60_000     # candle 1m berikutnya
            if len(rows) < 1000:
                break
            time.sleep(0.15)                # pacing rate limit

    dur = time.time() - t0
    print(f"{symbol}: {n:,} candle -> {path} ({dur:.0f}s)", flush=True)
    return path


def main(argv=None) -> int:
    import os
    ap = argparse.ArgumentParser(description="Unduh klines 1m untuk backtest")
    ap.add_argument("--symbols", default=None,
                    help="daftar simbol dipisah koma, mis. BTCUSDT,ETHUSDT")
    ap.add_argument("--top", type=int, default=20,
                    help="ambil N pair terlikuid (kalau --symbols kosong)")
    ap.add_argument("--months", type=int, default=6)
    ap.add_argument("--quote", default="USDT")
    ap.add_argument("--out", default=None)
    ap.add_argument("--base", default=DEFAULT_BASE,
                    help=f"base URL REST (default: {DEFAULT_BASE})")
    args = ap.parse_args(argv)

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        print(f"Mengambil {args.top} pair {args.quote} terlikuid "
              f"dari {args.base} ...")
        symbols = _top_symbols(args.top, args.quote, base=args.base)
        print("Pair:", ", ".join(symbols))

    from _bootstrap import DATA_DIR
    out = args.out or DATA_DIR
    os.makedirs(out, exist_ok=True)

    failed = []
    for i, s in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] {s}...", flush=True)
        try:
            download_symbol(s, args.months, out, base=args.base)
        except Exception as exc:
            print(f"  GAGAL: {exc}", flush=True)
            failed.append(s)
    if failed:
        print(f"\nGagal: {', '.join(failed)}")
        return 1
    print("\nSelesai. Lanjut: python tools/backtest/grid.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
