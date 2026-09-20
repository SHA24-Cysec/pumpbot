"""Diagnostik edge sinyal: return maju murni setelah entry, TANPA exit.

Pertanyaan yang dijawab: "setelah sinyal bunyi, apakah harga rata rata naik
melebihi fee?" Semua aturan exit (SL, TP, breakeven, trailing) dibuang; beli
di open candle entry, jual persis H menit kemudian berapapun harganya.

Setiap himpunan sinyal dibandingkan dengan baseline sinyal acak pada jumlah
yang sama, supaya terlihat apakah momen yang dipilih detector lebih baik dari
memilih waktu secara acak.

CATATAN STATISTIK: sinyal yang berdekatan waktunya saling berkorelasi
(misalnya 50 sinyal pada pump yang sama), sehingga standard error biasa
sedikit OPTIMIS. Nilai p dibaca sebagai petunjuk, bukan bukti formal.

CLI:
    python -m tools.backtest.signals_probe --days 365 --thr 40,50,60 \
        --horizons 15,30,60,240 --workers 4
"""

from __future__ import annotations

import argparse
import glob
import math
import os
import random
import statistics
from typing import Optional

from bot.config import Config
from bot.models import Candle

from tools.backtest.download import DATA_DIR, INTERVAL_MS
from tools.backtest.optimize import (SignalParams, load_data, parse_floats,
                                     variant_cfg)
from tools.backtest.progress import ProgressUI
from tools.backtest.signals import Entry, scan_all
from tools.backtest.util import load_backtest_config

# Horizon default: 15m, 30m, 60m, dan 240m setelah entry.
DEFAULT_HORIZONS = [15, 30, 60, 240]

# Z untuk interval kepercayaan 95 persen.
Z95 = 1.96


# ---------------------------------------------------------------------------
# Perhitungan return maju
# ---------------------------------------------------------------------------

def forward_returns(entries: list[Entry], data: dict[str, list[Candle]],
                    horizon_min: int, interval_min: int
                    ) -> tuple[list[float], int]:
    """Return persen setelah horizon min untuk tiap entry.

    Harga masuk = entry_open (open candle entry). Harga keluar = close candle
    pada entry_idx + jumlah candle horizon. Entry yang tidak punya cukup
    candle ke depan (ujung data) dilewati dan dihitung sebagai skipped.
    """
    step = max(1, int(horizon_min) // max(int(interval_min), 1))
    out: list[float] = []
    skipped = 0
    for e in entries:
        candles = data.get(e.symbol)
        if not candles or not 0 <= e.entry_idx < len(candles):
            skipped += 1
            continue
        j = e.entry_idx + step
        if j >= len(candles):
            skipped += 1
            continue
        px0 = e.entry_open
        if px0 <= 0:
            skipped += 1
            continue
        out.append((candles[j].close - px0) / px0 * 100.0)
    return out, skipped


def random_entries(data: dict[str, list[Candle]], count: int, seed: int = 42,
                   min_idx: int = 30, tail_step: int = 1) -> list[Entry]:
    """Bangkitkan entry palsu pada (simbol, indeks) acak merata.

    Indeks dibatasi ke [min_idx, len-1-tail_step] supaya pembagian tail
    konsisten dengan sinyal asli. Deterministik untuk seed yang sama.
    """
    rng = random.Random(seed)
    segmen: list[tuple[str, int, int]] = []   # (simbol, lo, hi_inklusif)
    total = 0
    for sym in sorted(data):
        candles = data[sym]
        hi = len(candles) - 1 - max(0, tail_step)
        lo = min(min_idx, hi)
        if hi <= lo:
            continue
        segmen.append((sym, lo, hi))
        total += hi - lo
    out: list[Entry] = []
    if total <= 0 or count <= 0:
        return out
    for _ in range(count):
        pos = rng.randrange(total)
        for sym, lo, hi in segmen:
            lebar = hi - lo
            if pos < lebar:
                idx = lo + pos
                c = data[sym][idx]
                out.append(Entry(symbol=sym, entry_idx=idx,
                                 ts_entry=c.open_time,
                                 entry_open=c.open, score=0.0))
                break
            pos -= lebar
    return out


# ---------------------------------------------------------------------------
# Statistik ringkas
# ---------------------------------------------------------------------------

def _norm_cdf(x: float) -> float:
    """CDF normal baku via fungsi galat."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def summarize(vals: list[float]) -> dict:
    """Ringkasan statistik satu himpunan return (persen)."""
    n = len(vals)
    if n == 0:
        return {"n": 0, "mean": 0.0, "median": 0.0, "pct_pos": 0.0,
                "std": 0.0, "se": 0.0, "lo": 0.0, "hi": 0.0}
    mean = statistics.fmean(vals)
    med = statistics.median(vals)
    pct = 100.0 * sum(1 for v in vals if v > 0) / n
    std = statistics.stdev(vals) if n > 1 else 0.0
    se = std / math.sqrt(n)
    return {"n": n, "mean": mean, "median": med, "pct_pos": pct,
            "std": std, "se": se, "lo": mean - Z95 * se, "hi": mean + Z95 * se}


def compare(a: dict, b: dict) -> dict:
    """Bandingkan rata rata dua himpunan independent (uji z dua sisi)."""
    diff = a["mean"] - b["mean"]
    se = math.hypot(a["se"], b["se"])
    if se <= 0:
        z, p = 0.0, 1.0
    else:
        z = diff / se
        p = min(1.0, 2.0 * (1.0 - _norm_cdf(abs(z))))
    return {"diff": diff, "se": se, "z": z, "p": p}


def verdict(sig: dict, cmp_: dict, fee_rt: float) -> str:
    """Kesimpulan satu sel: adakah edge yang mengalahkan fee?

    Syarat "signifikan" bukan hanya p < 0.05, tetapi selisihnya juga harus
    POSITIF (sinyal mengalahkan acak). Uji p bersifat dua sisi, sehingga
    sinyal yang signifikan LEBIH BURUK dari acak tanpa syarat ini bisa
    salah dibaca sebagai edge.
    """
    if sig["n"] < 30:
        return "DATA KURANG (n<30)"
    mengalahkan_acak = cmp_["p"] < 0.05 and cmp_["diff"] > 0.0
    if sig["lo"] > fee_rt and mengalahkan_acak:
        return "EDGE MELEBIHI FEE"
    if sig["lo"] > 0.0 and mengalahkan_acak:
        return "POSITIF TAPI DI BAWAH FEE"
    if cmp_["p"] < 0.05 and cmp_["diff"] < 0.0:
        return "LEBIH BURUK DARI ACAK"
    return "TIDAK SIGNIFIKAN / NOL"


# ---------------------------------------------------------------------------
# Output tabel
# ---------------------------------------------------------------------------

def _fmt(value: float, lebar: int = 7, des: int = 3) -> str:
    """Format persen bertanda dengan lebar tetap."""
    return f"{value:+{lebar}.{des}f}"


def print_probe_table(label: str, n_signal: int, per_horizon: list[dict],
                      fee_rt: float) -> None:
    """Cetak tabel satu kombinasi sinyal: satu baris per horizon."""
    print(f"\n== {label} ==")
    print(f"   sinyal: {n_signal}, baseline acak: jumlah yang sama, fee {fee_rt:g}%")
    header = (f"   {'HOR':>5} {'n_sinyal':>9} {'rata2%':>8} "
              f"{'CI95 bawah':>10} {'CI95 atas':>10} {'median%':>8} "
              f"{'%pos':>6} {'acak%':>8} {'selisih':>8} {'p':>7} kesimpulan")
    print(header)
    print("   " + "-" * (len(header) - 3))
    for row in per_horizon:
        s, cmp_, v = row["sig"], row["cmp"], row["verdict"]
        print(f"   {row['h']:>4}m {s['n']:>9} {_fmt(s['mean'], 8)} "
              f"{_fmt(s['lo'], 10)} {_fmt(s['hi'], 10)} "
              f"{_fmt(s['median'], 8)} {s['pct_pos']:>5.1f} "
              f"{_fmt(row['rnd']['mean'], 8)} {_fmt(cmp_['diff'], 8)} "
              f"{cmp_['p']:>7.3f} {v}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Definisi argumen CLI diagnostik."""
    p = argparse.ArgumentParser(
        prog="python -m tools.backtest.signals_probe",
        description="Ukur edge murni sinyal entry: return maju tanpa exit.")
    p.add_argument("--days", type=int, default=365, help="panjang data (hari)")
    p.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MS))
    p.add_argument("--symbols", default="", help="batasi simbol, dipisah koma")
    p.add_argument("--thr", default="40,50,60",
                   help="daftar threshold sinyal, dipisah koma")
    p.add_argument("--wpa", default="",
                   help="daftar rasio bobot price action (default ikut config)")
    p.add_argument("--horizons", default="15,30,60,240",
                   help="daftar horizon menit, dipisah koma")
    p.add_argument("--fee", type=float, default=None,
                   help="fee round trip persen (default 2x fee_pct config)")
    p.add_argument("--random-mult", type=float, default=1.0,
                   help="kelipatan jumlah baseline acak (0 = nonaktif)")
    p.add_argument("--seed", type=int, default=42, help="seed baseline acak")
    p.add_argument("--workers", type=int, default=1, help="proses paralel scan")
    p.add_argument("--config", default=os.path.join("config", "config.yaml"))
    p.add_argument("--data-dir", default=DATA_DIR)
    p.add_argument("--no-progress", action="store_true",
                   help="matikan progress bar interaktif")
    return p


def _rasio_default(cfg: Config) -> float:
    """Rasio bobot price action dalam (pa + volume) menurut config."""
    w = cfg.signal.weights or {}
    pa, vol = float(w.get("price_action", 0) or 0), float(w.get("volume", 0) or 0)
    tot = pa + vol
    return pa / tot if tot > 0 else 0.5


def main(argv: Optional[list[str]] = None) -> int:
    """Titik masuk CLI diagnostik edge sinyal."""
    args = build_parser().parse_args(argv)
    cfg = load_backtest_config(args.config)

    thr_list = parse_floats(args.thr, [cfg.signal.score_threshold])
    wpa_list = parse_floats(args.wpa, [_rasio_default(cfg)])
    hor_list = sorted({max(1, int(round(h))) for h in
                       parse_floats(args.horizons, DEFAULT_HORIZONS)})
    fee_rt = (float(cfg.risk.fee_pct) * 2.0 if args.fee is None
              else float(args.fee))
    interval_min = INTERVAL_MS[args.interval] // 60_000
    step_max = max(hor_list) // interval_min

    symbol_list = [s.strip().upper() for s in args.symbols.split(",")
                   if s.strip()]
    if symbol_list:
        n_paths = len(symbol_list)
    else:
        n_paths = len(glob.glob(os.path.join(
            args.data_dir, f"*_{args.interval}.csv")))

    with ProgressUI(enabled=not args.no_progress) as ui:
        bar = ui.bar("Memuat cache candle", total=max(1, n_paths))
        data = load_data(symbol_list, args.interval, args.days,
                         args.data_dir, progress_cb=bar.advance)
        bar.close()
        if not data:
            ui.log(f"Tidak ada data di {args.data_dir}. Jalankan dulu:")
            ui.log("  python -m tools.backtest.download "
                   "--top 30 --days 30 --interval 1m")
            return 1
        ui.log(f"Data: {len(data)} simbol, "
               f"{sum(len(c) for c in data.values())} candle {args.interval}")
        ui.log("CATATAN: hasil di sini TANPA exit apa pun. Sinyal berdekatan "
               "berkorelasi, jadi p-value bersifat optimis.")

        kombo = [(t, w) for t in thr_list for w in wpa_list]
        bar = ui.bar("Memindai sinyal",
                     total=max(1, len(kombo) * len(data)))
        hasil: list[tuple[str, int, list[dict]]] = []
        for i, (thr, wpa) in enumerate(kombo, 1):
            sp = SignalParams(threshold=thr, w_pa=wpa,
                              ma_period=cfg.signal.volume.ma_period,
                              spike_scale=cfg.signal.volume.spike_scale,
                              structure_candles=(
                                  cfg.signal.price_action.structure_candles),
                              breakout_lookback=(
                                  cfg.signal.price_action.breakout_lookback),
                              swing_neighbors=(
                                  cfg.signal.price_action.swing_neighbors),
                              min_candles=cfg.signal.min_candles)
            vcfg = variant_cfg(cfg, sp)
            entries = scan_all(data, vcfg, thr, interval_min, args.workers,
                               progress_cb=bar.advance)
            label = f"thr={thr:g}/wpa={wpa:g}"
            ui.log(f"[{i}/{len(kombo)}] {label} -> {len(entries)} sinyal")

            base: list[Entry] = []
            if args.random_mult > 0 and entries:
                base = random_entries(data, int(len(entries)
                                                * args.random_mult),
                                      seed=args.seed, tail_step=step_max)

            per_h: list[dict] = []
            for h in hor_list:
                vs, _skip_s = forward_returns(entries, data, h, interval_min)
                vb, _skip_b = forward_returns(base, data, h, interval_min)
                s_stat, b_stat = summarize(vs), summarize(vb)
                cmp_ = compare(s_stat, b_stat)
                per_h.append({"h": h, "sig": s_stat, "rnd": b_stat,
                              "cmp": cmp_,
                              "verdict": verdict(s_stat, cmp_, fee_rt)})
            hasil.append((label, len(entries), per_h))
        bar.close()

    for label, n, per_h in hasil:
        print_probe_table(label, n, per_h, fee_rt)

    print("\nCara membaca: tiap blok adalah satu kombinasi sinyal (thr/wpa). "
          "Sel 'EDGE MELEBIHI FEE' berarti batas bawah CI 95% rata rata")
    print("return melewati fee DAN sinyal mengalahkan baseline acak secara "
          "signifikan (p < 0.05). Hanya sel itulah yang layak dioptimasi.")
    print("Baseline acak: entry pada waktu acak dengan jumlah sama "
          "(seed tetap, jadi bisa direproduksi).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
