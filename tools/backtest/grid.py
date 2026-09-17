#!/usr/bin/env python3
"""
Grid search + walk-forward + analisis sensitivitas parameter entry
(berbasis klines 1m; lihat engine.py untuk batasan & asumsi).

Pemakaian:
    python tools/backtest/grid.py                     # pakai semua file di data/
    python tools/backtest/grid.py --train-days 42 --test-days 14
    python tools/backtest/grid.py --dd-limit 5 --min-trades 30

Output di tools/backtest/out/:
    results.csv      : seluruh kombinasi x window (train & test)
    wf_summary.csv   : hasil walk-forward per window (parameter TERPILIH diuji
                       out-of-sample)
    report.md        : laporan ringkas + tabel plateau + rekomendasi
    heat_*.png       : heatmap sensitivitas (kalau matplotlib terpasang)

Metrik dilaporkan dari window TEST (out-of-sample). Kriteria pemilih
parameter per window train: profit factor maksimum DENGAN syarat
drawdown <= dd_limit dan jumlah trade >= min_trades.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import os
import pickle
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _bootstrap  # noqa: F401

from bot.config import load_config
from tools.backtest import engine
from tools.backtest.engine import Combo, compute_metrics, score_series

# Cache skor per simbol: scoring adalah bagian termahal (~2 ms/candle).
# Dengan cache, crash di tengah jalan / run ulang / ubah grid TIDAK perlu
# menghitung ulang skor (cukup replay). Kunci cache mencakup identitas
# data (jumlah + batas ts) dan sidik-jari config, jadi otomatis
# ter-invalidasi kalau data atau config berubah.
CACHE_DIR = os.path.join(os.path.dirname(_bootstrap.DATA_DIR), "cache")


def _cfg_fingerprint(cfg) -> str:
    """Sidik jari parameter yang memengaruhi scoring (dataclass Config)."""
    return hashlib.md5(repr(cfg).encode()).hexdigest()[:12]


def _cache_path(sym: str, candles, cfg) -> str:
    fp = "|".join((sym, str(len(candles)), str(candles[0].open_time),
                   str(candles[-1].close_time), _cfg_fingerprint(cfg)))
    h = hashlib.md5(fp.encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{sym}_{h}.pkl")


def load_cached_scores(sym: str, candles, cfg) -> list | None:
    """Skor tersimpan utk (data, config) yang sama persis, kalau ada."""
    try:
        with open(_cache_path(sym, candles, cfg), "rb") as f:
            scores = pickle.load(f)
        if isinstance(scores, list) and len(scores) > 0:
            return scores
    except Exception:
        pass
    return None


def save_cached_scores(sym: str, candles, cfg, scores) -> None:
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = _cache_path(sym, candles, cfg) + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump(scores, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, _cache_path(sym, candles, cfg))
    except Exception:
        pass                      # cache gagal -> tidak fatal

# grid default (kecil & disiplin — anti overfitting)
DEFAULT_GRID = {
    "threshold": [55, 60, 65, 70, 75],
    "sl_pct": [1.0, 1.5, 2.5],
    # Satu TP penuh berbasis risk:reward, bukan TP1/TP2 parsial.
    "tp_rr": [1.5, 2.0, 2.5],
    # Trailing berjalan setelah BE +1R.
    "trail_pct": [0.3, 0.5, 0.8],
}

# Grid fokus tetap memakai alur live: satu TP RR, BE +1R lalu trailing.
FOCUS_GRID = {
    "threshold": [70, 75, 80, 85],
    "sl_pct": [1.0, 1.5],
    "tp_rr": [2.0, 2.5, 3.0],
    "trail_pct": [0.4, 0.5, 0.6],
}



def load_all(data_dir: str, pattern: str = "*_1m.csv.gz"):
    import glob
    out = {}
    for path in sorted(glob.glob(os.path.join(data_dir, pattern))):
        sym = os.path.basename(path).split("_")[0]
        out[sym] = engine.load_candles(path)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-days", type=int, default=42)
    ap.add_argument("--test-days", type=int, default=14)
    ap.add_argument("--dd-limit", type=float, default=5.0,
                    help="batas drawdown maks (%%) agar kombinasi dianggap layak")
    ap.add_argument("--min-trades", type=int, default=30,
                    help="jumlah trade minimum di window train")
    ap.add_argument("--equity", type=float, default=10_000.0,
                    help="balance awal quote untuk simulasi spot")
    ap.add_argument("--risk-per-trade", type=float, default=1.0,
                    help="risk per transaksi (%% balance, default 1)")
    ap.add_argument("--max-open", type=int, choices=(1, 2, 3), default=1,
                    help="posisi simultan; 1 adalah default bot live")
    ap.add_argument("--data", default=_bootstrap.DATA_DIR)
    ap.add_argument("--out", default=_bootstrap.OUT_DIR)
    ap.add_argument("--no-cache", action="store_true",
                    help="abaikan cache skor (hitung ulang dari nol)")
    ap.add_argument("--max-hold", default="0",
                    help="batas umur posisi (menit), dipisah koma untuk "
                         "perbandingan; 0 = nonaktif. cth: 0,60,120,240")
    ap.add_argument("--focus", action="store_true",
                    help="grid fokus 72 kombinasi dengan TP RR dan trailing "
                         "setelah BE")
    args = ap.parse_args(argv)

    data = load_all(args.data)
    if not data:
        print(f"Tidak ada data di {args.data}. Jalankan dulu download.py "
              "atau synthetic.py")
        return 2
    # Backtest membaca detector dari satu konfigurasi project. Mode ``paper``
    # adalah default aman di config/config.yaml dan mode tidak dapat dioverride.
    cfg = load_config(os.path.join(_bootstrap._ROOT, "config", "config.yaml"))

    # ---------- 1. skor per candle (SEKALI per simbol; bagian mahal) ----------
    t0 = time.time()
    print(f"Menghitung seri skor untuk {len(data)} simbol "
          f"({sum(len(c) for c in data.values()):,} candle)...")
    scores = {}
    skipped = []
    for sym, candles in data.items():
        # cache: data + config identik -> pakai skor yang sudah dihitung
        cached = None if args.no_cache else load_cached_scores(sym, candles,
                                                                cfg)
        if cached is not None:
            scores[sym] = cached
            print(f"  {sym}: {len(cached):,} skor (cache)", flush=True)
            continue
        try:
            scores[sym] = score_series(sym, candles, cfg)
        except Exception as exc:
            # satu simbol bermasalah TIDAK boleh mematikan run berjam-jam;
            # lewati dengan peringatan keras agar kelihatan di laporan.
            print(f"  !! {sym}: SKIPPED — {type(exc).__name__}: {exc}",
                  flush=True)
            skipped.append(sym)
            continue
        save_cached_scores(sym, candles, cfg, scores[sym])
        print(f"  {sym}: {len(scores[sym]):,} skor", flush=True)
    print(f"Selesai dalam {time.time()-t0:.0f}s")

    if not scores:
        print("TIDAK ADA simbol yang berhasil di-skor. Periksa data.")
        return 2
    if skipped:
        print(f"\nPERINGATAN: {len(skipped)} simbol dilewati: "
              f"{', '.join(skipped)}\n")


    # rentang waktu global
    all_ts = [s.ts for ss in scores.values() for s in ss]
    t_min, t_max = min(all_ts), max(all_ts)
    day_ms = 86_400_000

    # ---------- 2. grid x walk-forward ----------
    grid_def = FOCUS_GRID if args.focus else DEFAULT_GRID
    max_holds = sorted({float(x) for x in args.max_hold.split(",")
                        if x.strip() != ""})
    grid_keys = list(grid_def)
    combos = [Combo(**dict(zip(grid_keys, vals)), max_hold_min=mh,
                    risk_pct=args.risk_per_trade, max_open=args.max_open)
              for vals in itertools.product(*grid_def.values())
              for mh in max_holds]
    print(f"\nGrid: {len(combos)} kombinasi x "
          f"walk-forward (train {args.train_days}d / test {args.test_days}d)")

    rows = []          # semua hasil (train & test) -> results.csv
    wf_rows = []       # parameter terpilih per window -> wf_summary.csv

    w_start = t_min
    w_idx = 0
    while True:
        tr0, tr1 = w_start, w_start + args.train_days * day_ms
        te1 = tr1 + args.test_days * day_ms
        if te1 > t_max + day_ms // 2:
            break
        w_idx += 1

        # --- evaluasi seluruh grid di TRAIN ---
        best, best_key = None, None
        for combo in combos:
            m = compute_metrics(engine.simulate_combo(
                scores, data, combo, equity=args.equity, ts0=tr0, ts1=tr1))
            rows.append(_row(combo, m, f"train_{w_idx}"))
            ok = (m.trades >= args.min_trades and m.max_dd <= args.dd_limit
                  and m.profit_factor is not None)
            key = (m.profit_factor if ok else -1e9, -m.max_dd)
            if best_key is None or key > best_key:
                best, best_key = combo, key

        if best is None:
            w_start = tr1
            continue

        # --- kombinasi terpilih diuji di TEST (out-of-sample) ---
        m_test = compute_metrics(engine.simulate_combo(
            scores, data, best, equity=args.equity, ts0=tr1, ts1=te1))
        rows.append(_row(best, m_test, f"test_{w_idx}"))
        wf_rows.append({
            "window": w_idx,
            "train_start": _d(tr0), "test_start": _d(tr1),
            **_params(best),
            "test_trades": m_test.trades,
            "test_pnl_pct": round(m_test.total_pnl_pct, 3),
            "test_pf": _pf(m_test.profit_factor),
            "test_dd_pct": round(m_test.max_dd, 3),
            "test_win_rate": round(m_test.win_rate, 1),
        })
        print(f"  window {w_idx}: train {_d(tr0)}..{_d(tr1)} -> test "
              f"{m_test.trades} trade, pnl {m_test.total_pnl_pct:+.2f}%, "
              f"dd {m_test.max_dd:.2f}%", flush=True)
        w_start = tr1                    # window bergulir tiap test-period

    if not wf_rows:
        print("\nData tidak cukup untuk satu window walk-forward pun "
              "(perlu > train+test hari).")
        return 2

    # ---------- 3. tulis hasil ----------
    os.makedirs(args.out, exist_ok=True)
    _write_csv(os.path.join(args.out, "results.csv"), rows)
    _write_csv(os.path.join(args.out, "wf_summary.csv"), wf_rows)
    report = _report(rows, wf_rows, args, len(data))
    with open(os.path.join(args.out, "report.md"), "w") as f:
        f.write(report)
    print(f"\nHasil ditulis ke {args.out}/ (results.csv, wf_summary.csv, "
          f"report.md)")

    _heatmaps(rows, args.out)
    return 0


# ---------------------------------------------------------------- helpers

def _params(c: Combo) -> dict:
    return {"threshold": c.threshold, "sl_pct": c.sl_pct,
            "tp_rr": c.tp_rr, "be_rr": c.be_rr,
            "trail_pct": c.trail_pct, "risk_pct": c.risk_pct,
            "max_open": c.max_open, "max_hold_min": c.max_hold_min}


def _row(c: Combo, m, window: str) -> dict:
    row = {"window": window, **_params(c),
           "trades": m.trades, "pnl_pct": round(m.total_pnl_pct, 3),
           "pf": _pf(m.profit_factor), "dd_pct": round(m.max_dd, 3),
           "win_rate": round(m.win_rate, 1),
           "avg_hold_min": round(m.avg_hold_min, 1)}
    # rincian alasan exit (utk studi max_hold)
    for reason in ("TP", "SL", "TRAILING", "MAX_HOLD", "END"):
        row[f"n_{reason.lower()}"] = m.exit_counts.get(reason, 0)
        row[f"pnl_{reason.lower()}_pct"] = round(
            m.exit_pnl_pct.get(reason, 0.0), 3)
    return row


def _pf(x):
    return "inf" if x == float("inf") else (round(x, 3) if x is not None else "")


def _d(ts_ms: int) -> str:
    import datetime
    return datetime.datetime.fromtimestamp(
        ts_ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%d")


def _write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    keys = list(rows[0])
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def _report(rows, wf_rows, args, n_symbols) -> str:
    """Laporan ringkas + analisis plateau sederhana."""
    tests = [r for r in rows if r["window"].startswith("test_")]
    trains = [r for r in rows if r["window"].startswith("train_")]

    # agregasi OOS gabungan dari wf_summary
    tot_tr = sum(r["test_trades"] for r in wf_rows)
    tot_pnl = sum(r["test_pnl_pct"] for r in wf_rows)
    mean_dd = sum(r["test_dd_pct"] for r in wf_rows) / len(wf_rows)
    wins_w = sum(1 for r in wf_rows if r["test_pnl_pct"] > 0)

    # plateau: sensitivitas tiap parameter = variasi metrik test saat
    # parameter digeser (rata-rata atas parameter lain), pakai data train
    lines = [
        "# Laporan Analisis Sensitivitas Parameter (klines-only)",
        "",
        f"- Simbol: {n_symbols} | walk-forward: {len(wf_rows)} window "
        f"(train {args.train_days}d, test {args.test_days}d)",
        f"- Kriteria pemilih: PF maksimum, syarat DD <= {args.dd_limit}% "
        f"dan trade >= {args.min_trades}/window",
        f"- Simulasi: risk {args.risk_per_trade:g}% balance | maks. "
        f"{args.max_open} posisi | BE +1R | TP satu target RR | trailing setelah BE",
        "",
        "## Hasil out-of-sample (gabungan seluruh window test)",
        "",
        f"- Total trade: {tot_tr}",
        f"- Total PnL (akumulatif): {tot_pnl:+.2f}% dari modal awal",
        f"- Rata-rata drawdown per window: {mean_dd:.2f}%",
        f"- Window profit: {wins_w}/{len(wf_rows)}",
        "",
        "## Parameter terpilih per window",
        "",
        "| window | test mulai | threshold | SL% | TP RR | BE R | trail% | "
        "maxHold | trade | PnL% | PF | DD% | WR% |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in wf_rows:
        lines.append(
            f"| {r['window']} | {r['test_start']} | {r['threshold']} | "
            f"{r['sl_pct']} | {r['tp_rr']} | {r['be_rr']} | "
            f"{r['trail_pct']} | {r['max_hold_min']:g} | "
            f"{r['test_trades']} | {r['test_pnl_pct']:+.2f} "
            f"| {r['test_pf']} | {r['test_dd_pct']} | {r['test_win_rate']} |")

    # plateau per parameter (dari seluruh hasil train)
    lines += ["", "## Sensitivitas parameter (rata-rata PF & DD di train)",
              "", "PF yang stabil terhadap pergeseran nilai = area robust.",
              ""]
    params_plateau = ["threshold", "sl_pct", "tp_rr", "trail_pct"]
    if len({r.get("max_hold_min", 0) for r in trains}) > 1:
        params_plateau.append("max_hold_min")
    for p in params_plateau:
        groups = {}
        for r in trains:
            groups.setdefault(r[p], []).append(r)
        lines.append(f"### {p}")
        lines.append("")
        lines.append("| nilai | rata PF | rata DD% | rata trade | rata PnL% |")
        lines.append("|---|---|---|---|---|")
        for val in sorted(groups):
            g = groups[val]
            pfs = [float(r["pf"]) for r in g if r["pf"] not in ("", "inf")]
            dds = [r["dd_pct"] for r in g]
            trs = [r["trades"] for r in g]
            pns = [r["pnl_pct"] for r in g]
            mean = lambda xs: sum(xs) / len(xs) if xs else 0.0
            lines.append(
                f"| {val} | {mean(pfs):.2f} | {mean(dds):.2f} | "
                f"{mean(trs):.0f} | {mean(pns):+.2f} |")
        lines.append("")

    # ---------- analisis max_hold ----------
    mhs = sorted({r.get("max_hold_min", 0) for r in rows})
    if len(mhs) > 1:
        lines += ["", "## Analisis max_hold (semua window, train+test)", ""]
        lines.append("Batas umur posisi: 0 = nonaktif (perilaku lama). "
                     "MAX_HOLD = exit paksa di harga close ketika umur "
                     "posisi melewati batas (SL/TP bursa selalu diutamakan).")
        lines.append("")
        lines.append("| maxHold(menit) | trade | rata PnL% | rata DD% | "
                     "%exit MAX_HOLD | PnL MAX_HOLD% | rata hold(menit) |")
        lines.append("|---|---|---|---|---|---|---|")
        for mh in mhs:
            g = [r for r in rows if r.get("max_hold_min", 0) == mh]
            trs = sum(r["trades"] for r in g)
            n_mh = sum(r.get("n_max_hold", 0) for r in g)
            p_mh = sum(r.get("pnl_max_hold_pct", 0.0) for r in g)
            mean = lambda xs, k: (sum(r[k] for r in xs) / len(xs)
                                  if xs else 0.0)
            share = (n_mh / trs * 100) if trs else 0.0
            lines.append(
                f"| {mh:g} | {trs} | {mean(g,'pnl_pct'):+.2f} | "
                f"{mean(g,'dd_pct'):.2f} | {share:.1f}% | {p_mh:+.2f} | "
                f"{mean(g,'avg_hold_min'):.0f} |")
        lines += [
            "",
            "Cara baca: bila kolom 'PnL MAX_HOLD%' tidak jauh lebih buruk",
            "dari total dan rata PnL%/DD% membaik pada suatu nilai maxHold,",
            "berarti time-stop membantu (membebaskan modal dari posisi mati).",
            "Bila rata PnL% menurun tajam saat maxHold mengecil, artinya",
            "winner bot ini lambat — biarkan max_hold nonaktif.",
            "",
        ]

    lines += [
        "## Catatan wajib baca",
        "",
        "- Skor di sini dari 3 detektor klines (trade_flow proxy, volume,",
        "  price_action) + penalti manipulasi — BUKAN skor gabungan-6-detektor",
        "  yang dipakai live (orderbook & whale tidak bisa diuji dari klines).",
        "- Threshold hasil optimasi ini TIDAK langsung berlaku untuk live;",
        "  gunakan untuk memahami karakter & area robust, lalu validasi testnet.",
        "- Entry disimulasikan di open candle berikutnya; SL diprioritaskan",
        "  bila TP & SL tersentuh di candle yang sama (asumsi konservatif).",
    ]
    return "\n".join(lines) + "\n"


def _heatmaps(rows, out_dir: str) -> None:
    """Heatmap pasangan parameter (opsional, butuh matplotlib)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib tidak terpasang -> heatmap dilewati "
              "(pip install matplotlib untuk mengaktifkan)")
        return

    trains_all = [r for r in rows if r["window"].startswith("train_")]
    mhs = sorted({r.get("max_hold_min", 0) for r in trains_all})
    pairs = [("threshold", "sl_pct"), ("threshold", "tp_rr"),
             ("sl_pct", "trail_pct"), ("tp_rr", "trail_pct")]
    for pa, pb in pairs:
      for mh in mhs:
        trains = [r for r in trains_all if r.get("max_hold_min", 0) == mh]
        xs = sorted({r[pa] for r in trains})
        ys = sorted({r[pb] for r in trains})
        grid = [[None] * len(xs) for _ in ys]
        for r in trains:
            i, j = ys.index(r[pb]), xs.index(r[pa])
            v = r["dd_pct"]
            grid[i][j] = v if grid[i][j] is None else (grid[i][j] + v) / 2
        fig, ax = plt.subplots(figsize=(5, 4))
        im = ax.imshow(grid, cmap="RdYlGn_r", aspect="auto")
        ax.set_xticks(range(len(xs)), [str(x) for x in xs])
        ax.set_yticks(range(len(ys)), [str(y) for y in ys])
        ax.set_xlabel(pa)
        ax.set_ylabel(pb)
        judul = f"Max drawdown % (rata-rata train): {pa} x {pb}"
        if len(mhs) > 1:
            judul += f" | maxHold {int(mh)}m"
        ax.set_title(judul)
        fig.colorbar(im)
        for i in range(len(ys)):
            for j in range(len(xs)):
                if grid[i][j] is not None:
                    ax.text(j, i, f"{grid[i][j]:.1f}", ha="center",
                            va="center", fontsize=8)
        suffix = f"_mh{int(mh)}" if len(mhs) > 1 else ""
        path = os.path.join(out_dir, f"heat_{pa}_x_{pb}{suffix}.png")
        fig.tight_layout()
        fig.savefig(path, dpi=120)
        plt.close(fig)
        print(f"  heatmap: {path}")


if __name__ == "__main__":
    raise SystemExit(main())
