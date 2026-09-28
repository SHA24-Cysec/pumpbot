"""
Pengunduh data klines publik Binance Spot untuk keperluan backtest.

Prinsip:
  * Hanya endpoint market data publik, tanpa API key dan tanpa BinanceGateway.
  * Hanya stdlib (urllib) supaya tidak menambah dependensi.
  * Host utama data-api.binance.vision (khusus market data), fallback
    api.binance.com bila host utama bermasalah.

Endpoint yang dipakai (sesuai dokumentasi resmi Spot REST API):
  * GET /api/v3/ticker/24hr?type=MINI  -> bobot 80 bila tanpa parameter simbol
  * GET /api/v3/klines                 -> bobot 2, limit maksimum 1000

Batas bobot REQUEST_WEIGHT saat ini 6000 per menit per IP (dibaca dari
exchangeInfo). Modul ini memantau header x-mbx-used-weight-1m dan tidur bila
pemakaian mendekati batas, serta menghormati Retry-After pada HTTP 429 dan 418.

CLI:
    python -m tools.backtest.download --top 30 --days 30 --interval 1m
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from bot.config import Config

from tools.backtest.progress import ProgressUI
from tools.backtest.util import load_backtest_config

# Host publik. Yang pertama khusus market data (tanpa endpoint berkunci).
HOSTS = ("https://data-api.binance.vision", "https://api.binance.com")

# Direktori cache CSV
DATA_DIR = os.path.join("data", "backtest")

# Interval yang didukung tools ini
# Interval candle yang didukung. Seluruhnya adalah interval resmi endpoint
# /api/v3/klines Binance. 1s dilewati karena berbagai konversi "per menit"
# di tools ini membagi durasi dengan 60_000; 3d/1w/1M dilewati karena candle
# berdurasi hari-bulan tidak relevan untuk deteksi pump jangka menit-jam.
# Menambah entri di sini otomatis meluaskan pilihan di dashboard, CLI
# download, CLI optimizer, dan signals_probe (semuanya membaca dict ini).
INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}

# Batas bobot per menit (REQUEST_WEIGHT / MINUTE) dan ambang jaga-jaga.
WEIGHT_LIMIT = 6000
WEIGHT_SAFE_RATIO = 0.85

# Aturan filter simbol, disamakan dengan bot/exchange/binance_gateway.py
_STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EUR", "GBP", "AUD",
    "TRY", "BRL", "ARS", "JPY", "RUB", "UAH", "PLN", "RON", "ZAR",
}
_LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

# Hanya simbol ASCII huruf besar dan angka yang diterima.
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}USDT$")

# Header CSV cache
CSV_HEADER = [
    "open_time", "close_time", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "taker_buy_volume",
]

USER_AGENT = "pumpbot-backtest/1.0 (+public market data only)"


class DownloadError(RuntimeError):
    """Kegagalan permanen saat mengunduh data (tidak layak diulang)."""


# ---------------------------------------------------------------------------
# Lapisan HTTP
# ---------------------------------------------------------------------------

@dataclass
class HttpResult:
    """Hasil satu permintaan HTTP yang sudah di-parse."""
    payload: object
    used_weight: int = 0


def _parse_retry_after(value: Optional[str], default: float) -> float:
    """Ubah header Retry-After (detik) menjadi float, dengan batas wajar."""
    if not value:
        return default
    try:
        return min(max(float(value), 0.0), 300.0)
    except (TypeError, ValueError):
        return default


def _sleep_if_weight_high(used_weight: int, sleeper=time.sleep) -> None:
    """Tidur sebentar bila bobot terpakai sudah mendekati batas per menit."""
    if used_weight >= WEIGHT_LIMIT * WEIGHT_SAFE_RATIO:
        sleeper(5.0)


def http_get_json(path: str, params: dict, timeout: float = 15.0,
                  max_retry: int = 5, sleeper=time.sleep) -> HttpResult:
    """
    GET JSON ke endpoint publik dengan fallback host, backoff, dan
    penanganan rate limit.

    Aturan:
      * HTTP 400 dianggap kesalahan permanen (fail fast), tidak diulang.
      * HTTP 429 dan 418 dihormati lewat Retry-After lalu diulang.
      * Error jaringan dan HTTP 5xx diulang dengan exponential backoff.
    """
    query = urllib.parse.urlencode(params) if params else ""
    last_error: Optional[Exception] = None

    for attempt in range(max_retry):
        host = HOSTS[min(attempt, len(HOSTS) - 1)] if attempt else HOSTS[0]
        url = f"{host}{path}" + (f"?{query}" if query else "")
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                headers = resp.headers
                used = 0
                try:
                    used = int(headers.get("x-mbx-used-weight-1m") or 0)
                except (TypeError, ValueError):
                    used = 0
                payload = json.loads(raw.decode("utf-8"))
                _sleep_if_weight_high(used, sleeper)
                return HttpResult(payload=payload, used_weight=used)
        except urllib.error.HTTPError as exc:
            last_error = exc
            code = exc.code
            if code == 400:
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", "replace")[:200]
                except Exception:
                    detail = ""
                raise DownloadError(
                    f"HTTP 400 dari {path} params={params}: {detail}") from exc
            if code in (429, 418):
                retry_after = _parse_retry_after(
                    exc.headers.get("Retry-After") if exc.headers else None,
                    default=30.0)
                print(f"  rate limit (HTTP {code}), tunggu {retry_after:.0f} detik")
                sleeper(retry_after)
                continue
            # 403, 451, 5xx, dan lainnya: coba host berikutnya dengan backoff
            sleeper(min(2.0 ** attempt, 20.0) + random.random())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError,
                ConnectionError, OSError) as exc:
            last_error = exc
            sleeper(min(2.0 ** attempt, 20.0) + random.random())

    raise DownloadError(f"Gagal GET {path} setelah {max_retry} percobaan: {last_error}")


# ---------------------------------------------------------------------------
# Pemilihan simbol
# ---------------------------------------------------------------------------

def is_tradeable_symbol(symbol: str, quote: str = "USDT") -> bool:
    """
    True bila simbol layak dipakai backtest.

    Aturan disamakan dengan bot: harus berakhiran quote, base bukan stablecoin,
    dan bukan leveraged token (UP, DOWN, BULL, BEAR).
    """
    if not symbol or not symbol.endswith(quote):
        return False
    if not _SYMBOL_RE.match(symbol):
        return False
    base = symbol[: -len(quote)]
    if not base:
        return False
    if base in _STABLE_BASES:
        return False
    return not any(base.endswith(sfx) and len(base) > len(sfx)
                   for sfx in _LEVERAGED_SUFFIXES)


def top_symbols(top_n: int, cfg: Config, quote: str = "USDT",
                fetcher=http_get_json) -> list[str]:
    """
    Ambil daftar simbol dengan volume quote 24 jam tertinggi.

    Satu panggilan GET /api/v3/ticker/24hr?type=MINI (tanpa parameter simbol),
    lalu disaring memakai aturan bot dan config universe.
    """
    res = fetcher("/api/v3/ticker/24hr", {"type": "MINI"})
    rows = res.payload
    if not isinstance(rows, list):
        raise DownloadError("Respons ticker 24 jam bukan list")

    min_vol = float(getattr(cfg.universe, "min_quote_volume_24h", 0.0) or 0.0)
    excluded = {str(s).upper() for s in (cfg.universe.exclude_symbols or [])}

    pairs: list[tuple[str, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        if not is_tradeable_symbol(symbol, quote):
            continue
        if symbol in excluded:
            continue
        try:
            qv = float(row.get("quoteVolume") or 0.0)
        except (TypeError, ValueError):
            continue
        if qv < min_vol:
            continue
        pairs.append((symbol, qv))

    pairs.sort(key=lambda item: (-item[1], item[0]))
    return [sym for sym, _ in pairs[: max(0, top_n)]]


# ---------------------------------------------------------------------------
# Klines
# ---------------------------------------------------------------------------

def normalize_ts(value: float) -> int:
    """
    Normalisasi timestamp ke milidetik.

    Data spot di data.binance.vision sejak 2025-01-01 memakai mikrodetik.
    Nilai di atas 1e14 karena itu dianggap mikrodetik lalu dibagi 1000.
    """
    ts = int(value)
    if ts > 100_000_000_000_000:   # 1e14
        ts //= 1000
    return ts


def parse_kline_row(row: Iterable) -> Optional[dict]:
    """Ubah satu baris klines (array 12 elemen) menjadi dict siap tulis CSV."""
    row = list(row)
    if len(row) < 11:
        return None
    try:
        return {
            "open_time": normalize_ts(row[0]),
            "close_time": normalize_ts(row[6]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "quote_volume": float(row[7]),
            "trades": int(float(row[8])),
            "taker_buy_volume": float(row[9]),
        }
    except (TypeError, ValueError):
        return None


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int,
                 fetcher=http_get_json, now_ms: Optional[int] = None,
                 max_pages: int = 100_000,
                 progress_cb: Optional[Callable[[int], None]] = None
                 ) -> list[dict]:
    """
    Unduh klines dengan paging sampai end_ms.

    Paging memakai startTime = open_time candle terakhir + 1 supaya tidak ada
    duplikat dan tidak ada candle terlewat. Candle yang belum close dibuang.
    progress_cb bila ada dipanggil tiap halaman dengan jumlah candle baru.
    """
    if interval not in INTERVAL_MS:
        raise DownloadError(f"Interval tidak didukung: {interval}")
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    # Candle dianggap close hanya bila close_time sudah terlewat. Patokan ini
    # tidak bergantung pada perataan epoch sehingga aman untuk semua interval.
    end_ms = min(int(end_ms), now)

    out: list[dict] = []
    cursor = int(start_ms)
    pages = 0
    while cursor <= end_ms and pages < max_pages:
        pages += 1
        res = fetcher("/api/v3/klines", {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 1000,
        })
        rows = res.payload
        if not isinstance(rows, list) or not rows:
            break
        parsed = [p for p in (parse_kline_row(r) for r in rows) if p]
        if not parsed:
            break
        last_open = parsed[-1]["open_time"]
        sebelum = len(out)
        for item in parsed:
            # Buang candle terakhir yang belum close.
            if item["close_time"] >= now:
                continue
            if out and item["open_time"] <= out[-1]["open_time"]:
                continue
            out.append(item)
        if progress_cb and len(out) > sebelum:
            progress_cb(len(out) - sebelum)
        next_cursor = last_open + 1
        if next_cursor <= cursor:      # jaga-jaga agar loop tidak macet
            break
        cursor = next_cursor
        if len(parsed) < 1000:
            break
    return out


# ---------------------------------------------------------------------------
# Cache CSV
# ---------------------------------------------------------------------------

def csv_path(symbol: str, interval: str, data_dir: str = DATA_DIR) -> str:
    """Lokasi file cache untuk satu simbol dan interval."""
    return os.path.join(data_dir, f"{symbol}_{interval}.csv")


def read_csv(path: str) -> list[dict]:
    """Baca cache CSV. File rusak atau tidak ada menghasilkan list kosong."""
    if not os.path.exists(path):
        return []
    out: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                try:
                    out.append({
                        "open_time": int(row["open_time"]),
                        "close_time": int(row["close_time"]),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row["volume"]),
                        "quote_volume": float(row["quote_volume"]),
                        "trades": int(float(row["trades"])),
                        "taker_buy_volume": float(row["taker_buy_volume"]),
                    })
                except (KeyError, TypeError, ValueError):
                    continue
    except OSError:
        return []
    out.sort(key=lambda r: r["open_time"])
    # Buang duplikat open_time (ambil yang pertama).
    dedup: list[dict] = []
    for row in out:
        if dedup and row["open_time"] == dedup[-1]["open_time"]:
            continue
        dedup.append(row)
    return dedup


def write_csv(path: str, rows: list[dict]) -> None:
    """Tulis cache CSV secara atomik (tulis ke file sementara lalu rename)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row[k] for k in CSV_HEADER})
    os.replace(tmp, path)


def merge_rows(existing: list[dict], fresh: list[dict]) -> list[dict]:
    """Gabungkan dua deret candle: terurut naik dan tanpa duplikat open_time."""
    by_ts: dict[int, dict] = {r["open_time"]: r for r in existing}
    for row in fresh:
        by_ts[row["open_time"]] = row
    return [by_ts[k] for k in sorted(by_ts)]


def count_gaps(rows: list[dict], interval: str) -> int:
    """Hitung jumlah candle yang hilang di tengah deret (data bolong)."""
    step = INTERVAL_MS[interval]
    if len(rows) < 2 or step <= 0:
        return 0
    missing = 0
    for prev, cur in zip(rows, rows[1:]):
        delta = cur["open_time"] - prev["open_time"]
        if delta > step:
            missing += int(delta // step) - 1
    return missing


def download_symbol(symbol: str, interval: str, start_ms: int, end_ms: int,
                    data_dir: str = DATA_DIR, fetcher=http_get_json,
                    now_ms: Optional[int] = None,
                    progress_cb: Optional[Callable[[int], None]] = None
                    ) -> dict:
    """
    Unduh satu simbol dengan resume dari cache.

    Return ringkasan: jumlah baris, jumlah baris baru, dan jumlah candle bolong.
    progress_cb bila ada dipanggil tiap halaman berisi jumlah candle baru.
    """
    path = csv_path(symbol, interval, data_dir)
    existing = read_csv(path)
    step = INTERVAL_MS[interval]

    fresh: list[dict] = []
    if existing and existing[0]["open_time"] > start_ms:
        # Riwayat diminta lebih panjang dari isi cache: isi bagian depan dulu.
        fresh += fetch_klines(symbol, interval, int(start_ms),
                              existing[0]["open_time"] - 1,
                              fetcher=fetcher, now_ms=now_ms,
                              progress_cb=progress_cb)

    cursor = int(start_ms)
    if existing and existing[-1]["open_time"] >= start_ms:
        cursor = existing[-1]["open_time"] + step

    if cursor <= end_ms:
        fresh += fetch_klines(symbol, interval, cursor, end_ms,
                              fetcher=fetcher, now_ms=now_ms,
                              progress_cb=progress_cb)

    merged = merge_rows(existing, fresh)
    window = [r for r in merged if start_ms <= r["open_time"] <= end_ms]
    if fresh:
        write_csv(path, merged)
    elif not os.path.exists(path) and merged:
        write_csv(path, merged)

    return {
        "symbol": symbol,
        "path": path,
        "rows": len(window),
        "new_rows": len(fresh),
        "gaps": count_gaps(window, interval),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _hitung_baris_cache(path: str) -> int:
    """Hitung cepat jumlah candle pada CSV cache (baris dikurangi header)."""
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            return max(0, sum(1 for _ in fh) - 1)
    except OSError:
        return 0


def build_parser() -> argparse.ArgumentParser:
    """Definisi argumen CLI downloader."""
    p = argparse.ArgumentParser(
        prog="python -m tools.backtest.download",
        description="Unduh klines publik Binance Spot untuk backtest.")
    p.add_argument("--top", type=int, default=30,
                   help="jumlah simbol volume tertinggi (default 30)")
    p.add_argument("--days", type=int, default=30,
                   help="panjang riwayat dalam hari (default 30)")
    p.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MS),
                   help="interval candle (default 1m)")
    p.add_argument("--symbols", default="",
                   help="daftar simbol manual dipisah koma (melewati filter volume)")
    p.add_argument("--config", default=os.path.join("config", "config.yaml"),
                   help="path config.yaml")
    p.add_argument("--data-dir", default=DATA_DIR,
                   help=f"direktori cache CSV (default {DATA_DIR})")
    p.add_argument("--no-progress", action="store_true",
                   help="matikan progress bar interaktif (log baris biasa)")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    """Titik masuk CLI downloader."""
    args = build_parser().parse_args(argv)
    cfg = load_backtest_config(args.config)

    if args.days <= 0:
        print("Jumlah hari harus lebih dari 0")
        return 2

    now = int(time.time() * 1000)
    end_ms = now
    start_ms = now - args.days * 86_400_000

    if args.symbols.strip():
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        print(f"Mengambil daftar simbol teratas (top {args.top}) ...")
        try:
            symbols = top_symbols(args.top, cfg, cfg.quote_asset)
        except DownloadError as exc:
            print(f"Gagal mengambil daftar simbol: {exc}")
            return 1

    if not symbols:
        print("Tidak ada simbol yang lolos filter.")
        return 1

    print(f"{len(symbols)} simbol, interval {args.interval}, {args.days} hari")
    step = INTERVAL_MS[args.interval]
    est_total = max(1, (end_ms - start_ms) // step)

    ok, failed, gappy = 0, [], []
    with ProgressUI(enabled=not args.no_progress) as ui:
        total_bar = ui.bar(f"Mengunduh {len(symbols)} simbol", total=len(symbols))
        for i, symbol in enumerate(symbols, 1):
            cached = _hitung_baris_cache(
                csv_path(symbol, args.interval, args.data_dir))
            per = ui.bar(f"{symbol} [{i}/{len(symbols)}]",
                         total=est_total, quiet=True)
            if cached:
                per.advance(cached)
            try:
                info = download_symbol(symbol, args.interval, start_ms, end_ms,
                                       data_dir=args.data_dir,
                                       progress_cb=per.advance)
            except DownloadError as exc:
                per.close()
                ui.log(f"[{i}/{len(symbols)}] {symbol}: GAGAL {exc}")
                failed.append(symbol)
                total_bar.advance()
                continue
            per.close()
            flag = ""
            if info["gaps"] > 0:
                flag = f"  [BOLONG {info['gaps']} candle]"
                gappy.append(symbol)
            ui.log(f"[{i}/{len(symbols)}] {symbol}: {info['rows']} candle "
                   f"(+{info['new_rows']} baru){flag}")
            ok += 1
            total_bar.advance()
        total_bar.close()

    print("\nRingkasan")
    print(f"  berhasil : {ok}")
    fail_txt = f" -> {', '.join(failed)}" if failed else ""
    print(f"  gagal    : {len(failed)}{fail_txt}")
    print(f"  bolong   : {len(gappy)}" + (f" -> {', '.join(gappy)}" if gappy else ""))
    print(f"  cache    : {args.data_dir}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
