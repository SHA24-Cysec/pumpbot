"""
Grid search parameter exit + parameter sinyal, plus validasi out of sample.

Alur:
  1. Muat cache CSV hasil tools.backtest.download.
  2. Bangun grid SINYAL (threshold, rasio bobot, lookback detector, dst.).
     Tiap kombinasi sinyal memicu satu scan entry penuh dengan varian config.
  3. Di atas tiap himpunan entry, jalankan grid EXIT seperti biasa
     (SL, TP, BE, trailing, cooldown). Tetap satu grid kartesius penuh,
     tetapi tanpa scan berulang untuk kombinasi yang berbagi sinyal.
  4. Beri skor gabungan pada seluruh baris, evaluasi ulang top K pada
     data out of sample memakai entry dari kombinasi sinyalnya sendiri.
  5. Cetak tabel, tulis CSV lengkap, dan cuplikan YAML siap tempel.
     YAML hanya memuat parameter exit (keputusan desain: parameter sinyal
     cukup dilaporkan di tabel dan CSV).

CLI:
    python -m tools.backtest.optimize --days 30 --top 10 --oos 0.3 --workers 4
    python -m tools.backtest.optimize --thr 30,40,50 --wpa 0.5,0.6,0.7 \
        --ma-period 10,20,30 --spike-scale 2.5,3.0,4.0
"""

from __future__ import annotations

import argparse
import copy
import csv
import glob
import math
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Callable, Optional

from bot.config import Config
from bot.models import Candle

from tools.backtest.download import DATA_DIR, INTERVAL_MS, csv_path, read_csv
from tools.backtest.engine import Params, simulate_portfolio
from tools.backtest.progress import ProgressUI
from tools.backtest.signals import Entry, scan_all
from tools.backtest.util import load_backtest_config, rows_to_candles

# Grid default
DEFAULT_SL = [0.5, 0.75, 1.0, 1.5, 2.0, 3.0]
DEFAULT_TP_RR = [1.0, 1.5, 2.0, 3.0, 4.0]
DEFAULT_BE_RR = [0.0, 0.5, 0.75, 1.0]
DEFAULT_BE_BUFFER = [0.2, 0.3, 0.5]
DEFAULT_TRAIL = [0.0, 0.3, 0.5, 0.75, 1.0]

# Profit factor tak hingga dipotong di angka ini agar normalisasi tetap sehat.
PF_CAP = 5.0

RESULTS_CSV = os.path.join(DATA_DIR, "results.csv")


# ---------------------------------------------------------------------------
# Grid
# ---------------------------------------------------------------------------

def parse_floats(text: str, fallback: list[float]) -> list[float]:
    """Parse daftar angka dipisah koma dari CLI, kosong berarti default."""
    if not text or not text.strip():
        return list(fallback)
    out: list[float] = []
    for chunk in text.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            out.append(float(chunk))
        except ValueError:
            print(f"Peringatan: nilai '{chunk}' bukan angka, dilewati")
    return out or list(fallback)


def parse_ints(text: str, fallback: list[int]) -> list[int]:
    """Seperti parse_floats tetapi untuk bilangan bulat."""
    vals = parse_floats(text, [float(v) for v in fallback])
    return [int(round(v)) for v in vals]


# ---------------------------------------------------------------------------
# Grid SINYAL (tiap kombinasi memicu satu scan entry penuh)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SignalParams:
    """Satu kombinasi parameter sinyal yang diuji."""
    threshold: float
    w_pa: float               # rasio bobot price_action dalam (pa + volume)
    ma_period: int            # volume.ma_period
    spike_scale: float        # volume.spike_scale
    structure_candles: int    # price_action.structure_candles
    breakout_lookback: int    # price_action.breakout_lookback
    swing_neighbors: int      # price_action.swing_neighbors
    min_candles: int          # signal.min_candles

    def label(self) -> str:
        """Label ringkas satu baris untuk log."""
        return (f"thr={self.threshold:g}/wpa={self.w_pa:g}/ma={self.ma_period}"
                f"/spk={self.spike_scale:g}/str={self.structure_candles}"
                f"/brk={self.breakout_lookback}/sw={self.swing_neighbors}"
                f"/mc={self.min_candles}")


def variant_cfg(cfg: Config, sp: SignalParams) -> Config:
    """Salin cfg dan terapkan parameter sinyal dari satu kombinasi.

    Config asli tidak disentuh: scan memerlukan deepcopy karena objek ini
    akan dipickle ke proses pekerja.
    """
    c = copy.deepcopy(cfg)
    c.signal.score_threshold = sp.threshold
    weights = dict(c.signal.weights)
    weights["price_action"] = sp.w_pa
    weights["volume"] = 1.0 - sp.w_pa
    c.signal.weights = weights
    c.signal.volume.ma_period = sp.ma_period
    c.signal.volume.spike_scale = sp.spike_scale
    c.signal.price_action.structure_candles = sp.structure_candles
    c.signal.price_action.breakout_lookback = sp.breakout_lookback
    c.signal.price_action.swing_neighbors = sp.swing_neighbors
    c.signal.min_candles = sp.min_candles
    return c


def build_signal_grid(cfg: Config, thr_list: list[float],
                      wpa_list: list[float], ma_list: list[int],
                      spike_list: list[float], struct_list: list[int],
                      brk_list: list[int], swing_list: list[int],
                      minc_list: list[int]) -> list[SignalParams]:
    """Bangun grid kartesius parameter sinyal, tanpa duplikat.

    w_pa di luar (0, 1) dibuang karena akan menonaktifkan salah satu detector.
    """
    out: list[SignalParams] = []
    for wpa in wpa_list:
        if not (0.0 < wpa < 1.0):
            print(f"Peringatan: w_pa {wpa} di luar (0, 1) -> dibuang")
            continue
        for thr in thr_list:
            for ma in ma_list:
                for spk in spike_list:
                    for st in struct_list:
                        for brk in brk_list:
                            for sw in swing_list:
                                for mc in minc_list:
                                    out.append(SignalParams(
                                        threshold=float(thr),
                                        w_pa=float(wpa),
                                        ma_period=int(ma),
                                        spike_scale=float(spk),
                                        structure_candles=int(st),
                                        breakout_lookback=int(brk),
                                        swing_neighbors=int(sw),
                                        min_candles=int(mc)))
    seen: set[SignalParams] = set()
    unik: list[SignalParams] = []
    for sp in out:
        if sp in seen:
            continue
        seen.add(sp)
        unik.append(sp)
    return unik


def canonical(sl: float, tp_rr: float, be_rr: float, be_buffer: float,
              trail: float) -> tuple[float, float, float, float, float]:
    """
    Kanonikalisasi satu kombinasi agar duplikat fungsional hilang.

    Aturan:
      * be_rr <= 0 atau be_rr >= tp_rr  -> breakeven mati (BE tidak pernah
        tercapai sebelum TP), buffer tidak relevan, dan trailing dipaksa mati
        karena di bot trailing hanya aktif setelah breakeven.
      * trail <= 0 -> trailing mati.
    """
    be_off = be_rr <= 0 or be_rr >= tp_rr
    if be_off:
        return (sl, tp_rr, 0.0, 0.0, 0.0)
    if trail <= 0:
        return (sl, tp_rr, be_rr, be_buffer, 0.0)
    return (sl, tp_rr, be_rr, be_buffer, trail)


def build_grid(cfg: Config, sl_list: list[float], tp_list: list[float],
               be_list: list[float], buf_list: list[float],
               trail_list: list[float],
               cooldown_list: Optional[list[float]] = None) -> list[Params]:
    """Bangun daftar kombinasi unik, membuang sl_pct di luar batas config.

    cooldown_list None atau [None] berarti pakai nilai config; daftar angka
    menambah dimensi cooldown (menit) pada grid.
    """
    lo, hi = cfg.stops.min_stop_pct, cfg.stops.max_stop_pct
    valid_sl = []
    for sl in sl_list:
        if sl < lo or sl > hi:
            print(f"Peringatan: sl_pct {sl} di luar [{lo}, {hi}] -> dibuang")
            continue
        valid_sl.append(sl)

    cds: list[Optional[float]] = (
        [None] if not cooldown_list
        else [None if v is None else float(v) for v in cooldown_list])

    seen: set[tuple] = set()
    grid: list[Params] = []
    for cd in cds:
        for sl in valid_sl:
            for tp in tp_list:
                if tp <= 0:
                    continue
                for be in be_list:
                    for buf in buf_list:
                        for tr in trail_list:
                            key = canonical(sl, tp, be, buf, tr) + (cd,)
                            if key in seen:
                                continue
                            seen.add(key)
                            grid.append(Params(
                                sl_pct=key[0], tp_rr=key[1], be_rr=key[2],
                                be_buffer_pct=key[3], trail_pct=key[4],
                                trail_step_pct=cfg.trailing.update_step_pct,
                                fee_pct=cfg.risk.fee_pct, slippage_pct=0.0,
                                cooldown_min=cd,
                            ))
    return grid


# ---------------------------------------------------------------------------
# Skor gabungan
# ---------------------------------------------------------------------------

def _norm(values: list[float]) -> list[float]:
    """Min-max normalisasi; semua nilai sama menghasilkan 0.5 (netral)."""
    if not values:
        return []
    lo, hi = min(values), max(values)
    if not math.isfinite(lo) or not math.isfinite(hi) or hi - lo < 1e-12:
        return [0.5] * len(values)
    return [(v - lo) / (hi - lo) for v in values]


def score_results(rows: list[dict], w_pnl: float, w_pf: float, w_dd: float,
                  min_trades: int) -> list[dict]:
    """
    Beri skor gabungan pada hasil grid.

    Kombinasi dengan jumlah trade di bawah min_trades didiskualifikasi
    (skor None) dan tidak ikut normalisasi.
    """
    wsum = w_pnl + w_pf + w_dd
    if wsum <= 0:
        w_pnl, w_pf, w_dd, wsum = 0.4, 0.3, 0.3, 1.0

    qualified = [r for r in rows if r["metrics"]["trades"] >= min_trades]
    qualified_ids = {id(r) for r in qualified}
    for r in rows:
        r["score"] = None
        r["disqualified"] = id(r) not in qualified_ids
    if not qualified:
        return rows

    pnl = [r["metrics"]["net_return_pct"] for r in qualified]
    pf = [min(r["metrics"]["profit_factor"], PF_CAP) for r in qualified]
    dd = [r["metrics"]["max_dd_pct"] for r in qualified]

    n_pnl, n_pf, n_dd = _norm(pnl), _norm(pf), _norm(dd)
    for i, r in enumerate(qualified):
        r["score"] = round(
            (w_pnl * n_pnl[i] + w_pf * n_pf[i] + w_dd * (1.0 - n_dd[i])) / wsum,
            6)
    return rows


# ---------------------------------------------------------------------------
# Eksekusi paralel
# ---------------------------------------------------------------------------

_CTX: dict = {}


def _init_worker(entries: list[Entry], data: dict, cfg: Config,
                 start_equity: float, risk_pct: Optional[float],
                 presorted: bool) -> None:
    """Initializer proses pekerja: data candle dikirim sekali per proses."""
    _CTX.update(entries=entries, data=data, cfg=cfg,
                start_equity=start_equity, risk_pct=risk_pct,
                presorted=presorted)


def _run_combo(params: Params) -> dict:
    """Jalankan satu kombinasi di proses pekerja."""
    res = simulate_portfolio(_CTX["entries"], _CTX["data"], params, _CTX["cfg"],
                             start_equity=_CTX["start_equity"],
                             risk_pct=_CTX["risk_pct"],
                             presorted=_CTX["presorted"])
    return {"params": params, "metrics": res.metrics}


def run_grid(grid: list[Params], entries: list[Entry],
             data: dict[str, list[Candle]], cfg: Config,
             start_equity: float = 1000.0, risk_pct: Optional[float] = None,
             workers: int = 1,
             progress_cb: Optional[Callable[[int], None]] = None,
             presorted: bool = False) -> list[dict]:
    """Jalankan seluruh kombinasi grid, serial atau paralel.

    progress_cb bila ada dipanggil tiap satu kombinasi selesai dihitung.
    presorted=True aman bila entries keluaran scan_all (sudah terurut waktu),
    dan menghemat sort ulang per kombinasi pada himpunan entry besar.
    """
    if workers <= 1 or len(grid) <= 1:
        _init_worker(entries, data, cfg, start_equity, risk_pct, presorted)
        out: list[dict] = []
        for p in grid:
            out.append(_run_combo(p))
            if progress_cb:
                progress_cb(1)
        return out

    max_workers = min(workers, len(grid), (os.cpu_count() or 1) * 2)
    out = []
    with ProcessPoolExecutor(
            max_workers=max_workers, initializer=_init_worker,
            initargs=(entries, data, cfg, start_equity, risk_pct,
                      presorted)) as pool:
        for res in pool.map(_run_combo, grid, chunksize=4):
            out.append(res)
            if progress_cb:
                progress_cb(1)
    return out


# ---------------------------------------------------------------------------
# Pemuatan data dan pemisahan in sample / out of sample
# ---------------------------------------------------------------------------

def load_data(symbols: list[str], interval: str, days: int,
              data_dir: str = DATA_DIR,
              progress_cb: Optional[Callable[[int], None]] = None
              ) -> dict[str, list[Candle]]:
    """Muat cache CSV menjadi dict simbol -> daftar Candle.

    progress_cb bila ada dipanggil tiap satu file cache selesai dibaca.
    """
    out: dict[str, list[Candle]] = {}
    if symbols:
        paths = [csv_path(s, interval, data_dir) for s in symbols]
    else:
        paths = sorted(glob.glob(os.path.join(data_dir, f"*_{interval}.csv")))

    for path in paths:
        rows = read_csv(path)
        if progress_cb:
            progress_cb(1)
        if not rows:
            continue
        if days > 0:
            cutoff = rows[-1]["open_time"] - days * 86_400_000
            rows = [r for r in rows if r["open_time"] >= cutoff]
        if len(rows) < 2:
            continue
        symbol = os.path.basename(path).rsplit(f"_{interval}.csv", 1)[0]
        out[symbol] = rows_to_candles(rows)
    return out


def split_chronological(data: dict[str, list[Candle]], oos: float
                        ) -> tuple[dict[str, list[Candle]], dict[str, list[Candle]]]:
    """
    Pisah data secara kronologis memakai satu batas waktu global.

    oos adalah porsi akhir data untuk out of sample (mis. 0.3 = 30% terakhir).
    Batas waktu global menjaga urutan lintas simbol tetap konsisten.
    """
    if oos <= 0 or oos >= 1 or not data:
        return data, {}
    starts = [c[0].open_time for c in data.values() if c]
    ends = [c[-1].open_time for c in data.values() if c]
    if not starts:
        return data, {}
    t0, t1 = min(starts), max(ends)
    cut = t0 + int((t1 - t0) * (1.0 - oos))

    is_data: dict[str, list[Candle]] = {}
    oos_data: dict[str, list[Candle]] = {}
    for sym, candles in data.items():
        head = [c for c in candles if c.open_time <= cut]
        tail = [c for c in candles if c.open_time > cut]
        if len(head) >= 2:
            is_data[sym] = head
        if len(tail) >= 2:
            oos_data[sym] = tail
    return is_data, oos_data


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _fmt_pf(value: float) -> str:
    """Format profit factor, termasuk kasus tak hingga."""
    return "inf" if value == float("inf") else f"{value:.2f}"


def _fmt_cd(value: Optional[float]) -> str:
    """Cooldown: tampilkan '-' bila mengikuti config."""
    return "-" if value is None else f"{value:g}"


def print_table(rows: list[dict], oos_map: Optional[dict] = None,
                top: int = 10, vwap_enabled: Optional[bool] = None) -> None:
    """Cetak tabel top K kombinasi beserta metrik in sample dan out of sample.

    Kolom sinyal: THR = threshold, WPA = rasio bobot price action,
    MA = volume.ma_period, SPK = volume.spike_scale, STR/BRK = lookback
    price action, CD = cooldown menit. Rincian lengkap ada di CSV.
    """
    if vwap_enabled is not None:
        print(f"Filter Anchored VWAP: {'AKTIF' if vwap_enabled else 'MATI'}")
    header = (f"{'#':>2} {'THR':>4} {'WPA':>4} {'MA':>3} {'SPK':>4} "
              f"{'STR':>3} {'BRK':>3} {'CD':>4} "
              f"{'SL%':>5} {'TP':>4} {'BE':>4} {'BUF':>4} {'TR%':>4} "
              f"{'N':>4} {'WIN%':>6} {'RET%':>9} {'PF':>6} "
              f"{'MDD%':>7} {'SKOR':>6}")
    if oos_map:
        header += f" | {'N':>4} {'RET%':>9} {'PF':>6} {'MDD%':>7}"
    print(header)
    print("-" * len(header))
    for i, r in enumerate(rows[:top], 1):
        p, m, sp = r["params"], r["metrics"], r["signal"]
        line = (f"{i:>2} {sp.threshold:>4g} {sp.w_pa:>4g} {sp.ma_period:>3d} "
                f"{sp.spike_scale:>4g} {sp.structure_candles:>3d} "
                f"{sp.breakout_lookback:>3d} {_fmt_cd(p.cooldown_min):>4} "
                f"{p.sl_pct:>5.2f} {p.tp_rr:>4.2f} {p.be_rr:>4.2f} "
                f"{p.be_buffer_pct:>4.2f} {p.trail_pct:>4.2f} "
                f"{m['trades']:>4} {m['win_rate']:>6.2f} "
                f"{m['net_return_pct']:>9.2f} {_fmt_pf(m['profit_factor']):>6} "
                f"{m['max_dd_pct']:>7.2f} "
                f"{(r['score'] if r['score'] is not None else 0.0):>6.3f}")
        if oos_map is not None:
            om = oos_map.get((sp, p.key()))
            if om:
                line += (f" | {om['trades']:>4} {om['net_return_pct']:>9.2f} "
                         f"{_fmt_pf(om['profit_factor']):>6} "
                         f"{om['max_dd_pct']:>7.2f}")
                if om["net_return_pct"] < 0:
                    line += "  <-- OOS NEGATIF"
            else:
                line += f" | {'-':>4} {'-':>9} {'-':>6} {'-':>7}"
        print(line)


def write_results_csv(rows: list[dict], path: str = RESULTS_CSV,
                      oos_map: Optional[dict] = None) -> str:
    """Tulis seluruh hasil grid ke CSV.

    Kolom sinyal (threshold sampai min_candles) mengidentifikasi kombinasi
    sinyal yang menghasilkan baris tersebut; kolom exit lainnya sama seperti
    sebelumnya.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fields = ["threshold", "w_pa", "ma_period", "spike_scale",
              "structure_candles", "breakout_lookback", "swing_neighbors",
              "min_candles",
              "sl_pct", "tp_rr", "be_rr", "be_buffer_pct", "trail_pct",
              "trail_step_pct", "fee_pct", "cooldown_min", "trades", "win_rate",
              "net_return_pct", "profit_factor", "max_dd_pct", "avg_r",
              "expectancy", "score", "disqualified",
              "oos_trades", "oos_net_return_pct", "oos_profit_factor",
              "oos_max_dd_pct"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            p, m, sp = r["params"], r["metrics"], r["signal"]
            om = (oos_map or {}).get((sp, p.key()), {})
            writer.writerow({
                "threshold": sp.threshold, "w_pa": sp.w_pa,
                "ma_period": sp.ma_period, "spike_scale": sp.spike_scale,
                "structure_candles": sp.structure_candles,
                "breakout_lookback": sp.breakout_lookback,
                "swing_neighbors": sp.swing_neighbors,
                "min_candles": sp.min_candles,
                "sl_pct": p.sl_pct, "tp_rr": p.tp_rr, "be_rr": p.be_rr,
                "be_buffer_pct": p.be_buffer_pct, "trail_pct": p.trail_pct,
                "trail_step_pct": p.trail_step_pct, "fee_pct": p.fee_pct,
                "cooldown_min": ("" if p.cooldown_min is None
                                 else p.cooldown_min),
                "trades": m["trades"], "win_rate": m["win_rate"],
                "net_return_pct": m["net_return_pct"],
                "profit_factor": _fmt_pf(m["profit_factor"]),
                "max_dd_pct": m["max_dd_pct"], "avg_r": m["avg_r"],
                "expectancy": m["expectancy"],
                "score": "" if r.get("score") is None else r["score"],
                "disqualified": int(bool(r.get("disqualified"))),
                "oos_trades": om.get("trades", ""),
                "oos_net_return_pct": om.get("net_return_pct", ""),
                "oos_profit_factor": (_fmt_pf(om["profit_factor"])
                                      if om else ""),
                "oos_max_dd_pct": om.get("max_dd_pct", ""),
            })
    return path


def yaml_snippet(p: Params, cfg: Config) -> str:
    """Cuplikan YAML siap tempel untuk config.yaml."""
    be_enabled = "true" if p.be_rr > 0 else "false"
    tr_enabled = "true" if p.trail_pct > 0 else "false"
    be_trigger = p.be_rr if p.be_rr > 0 else cfg.breakeven.trigger_rr
    be_buffer = p.be_buffer_pct if p.be_rr > 0 else cfg.breakeven.buffer_pct
    tr_pct = p.trail_pct if p.trail_pct > 0 else cfg.trailing.percent_pct
    return (
        "stops:\n"
        "  mode: percent\n"
        f"  percent_pct: {p.sl_pct}\n"
        f"  min_stop_pct: {cfg.stops.min_stop_pct}\n"
        f"  max_stop_pct: {cfg.stops.max_stop_pct}\n"
        "\n"
        "take_profit:\n"
        "  mode: rr\n"
        f"  rr: {p.tp_rr}\n"
        "\n"
        "breakeven:\n"
        f"  enabled: {be_enabled}\n"
        f"  trigger_rr: {be_trigger}\n"
        f"  buffer_pct: {be_buffer}\n"
        "\n"
        "trailing:\n"
        f"  enabled: {tr_enabled}\n"
        "  mode: percent\n"
        f"  percent_pct: {tr_pct}\n"
        f"  update_step_pct: {p.trail_step_pct}\n"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Definisi argumen CLI optimizer."""
    p = argparse.ArgumentParser(
        prog="python -m tools.backtest.optimize",
        description="Grid search parameter SL, TP, breakeven, dan trailing.")
    p.add_argument("--days", type=int, default=30, help="panjang data (hari)")
    p.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MS))
    p.add_argument("--symbols", default="", help="batasi simbol, dipisah koma")
    p.add_argument("--top", type=int, default=10, help="jumlah baris tabel teratas")
    p.add_argument("--oos", type=float, default=0.3,
                   help="porsi data akhir untuk out of sample (0 = mati)")
    p.add_argument("--workers", type=int, default=1, help="jumlah proses paralel")
    p.add_argument("--min-trades", type=int, default=30,
                   help="jumlah trade minimum agar kombinasi dinilai")
    p.add_argument("--threshold", type=float, default=None,
                   help="satu nilai threshold (dipakai bila --thr kosong)")
    p.add_argument("--thr", default="",
                   help="daftar threshold dipisah koma (default 40,50,60)")
    p.add_argument("--wpa", default="",
                   help="daftar rasio bobot price action dalam (pa+volume), "
                        "dipisah koma (default 0.5,0.6,0.7)")
    p.add_argument("--ma-period", default="",
                   help="daftar volume.ma_period (default ikut config)")
    p.add_argument("--spike-scale", default="",
                   help="daftar volume.spike_scale (default ikut config)")
    p.add_argument("--structure", default="",
                   help="daftar price_action.structure_candles "
                        "(default ikut config)")
    p.add_argument("--breakout", default="",
                   help="daftar price_action.breakout_lookback "
                        "(default ikut config)")
    p.add_argument("--swing", default="",
                   help="daftar price_action.swing_neighbors "
                        "(default ikut config)")
    p.add_argument("--min-candles", default="",
                   help="daftar signal.min_candles (default ikut config)")
    p.add_argument("--cooldown", default="",
                   help="daftar cooldown sesudah exit dalam menit "
                        "(default ikut config)")
    p.add_argument("--risk-pct", type=float, default=None,
                   help="override risk_per_trade_pct")
    p.add_argument("--equity", type=float, default=1000.0, help="modal awal")
    p.add_argument("--w-pnl", type=float, default=0.4)
    p.add_argument("--w-pf", type=float, default=0.3)
    p.add_argument("--w-dd", type=float, default=0.3)
    p.add_argument("--sl", default="", help="daftar sl_pct dipisah koma")
    p.add_argument("--tp", default="", help="daftar tp_rr dipisah koma")
    p.add_argument("--be", default="", help="daftar be_rr dipisah koma")
    p.add_argument("--be-buffer", default="", help="daftar be_buffer_pct")
    p.add_argument("--trail", default="", help="daftar trail_pct")
    p.add_argument("--vwap", choices=("config", "on", "off"), default="config",
                   help="filter Anchored VWAP: ikut config (default), paksa on, "
                        "atau paksa off")
    p.add_argument("--config", default=os.path.join("config", "config.yaml"))
    p.add_argument("--data-dir", default=DATA_DIR)
    p.add_argument("--results", default=RESULTS_CSV)
    p.add_argument("--no-progress", action="store_true",
                   help="matikan progress bar interaktif (log baris biasa)")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    """Titik masuk CLI optimizer."""
    args = build_parser().parse_args(argv)
    cfg = load_backtest_config(args.config)

    # Override filter Anchored VWAP sebelum variant_cfg menyalin config.
    if args.vwap == "on":
        cfg.signal.vwap.enabled = True
    elif args.vwap == "off":
        cfg.signal.vwap.enabled = False
    vwap_status = ("AKTIF" if cfg.signal.vwap.enabled else "MATI")
    print(f"Filter Anchored VWAP: {vwap_status} (--vwap {args.vwap})")

    # Bangun kedua grid sebelum area progres supaya peringatannya tampil biasa.
    if args.thr.strip():
        thr_list = parse_floats(args.thr, [cfg.signal.score_threshold])
    elif args.threshold is not None:
        thr_list = [float(args.threshold)]
    else:
        thr_list = [40.0, 50.0, 60.0]
    signal_grid = build_signal_grid(
        cfg, thr_list,
        parse_floats(args.wpa, [0.5, 0.6, 0.7]),
        parse_ints(args.ma_period, [cfg.signal.volume.ma_period]),
        parse_floats(args.spike_scale, [cfg.signal.volume.spike_scale]),
        parse_ints(args.structure, [cfg.signal.price_action.structure_candles]),
        parse_ints(args.breakout, [cfg.signal.price_action.breakout_lookback]),
        parse_ints(args.swing, [cfg.signal.price_action.swing_neighbors]),
        parse_ints(args.min_candles, [cfg.signal.min_candles]),
    )
    if not signal_grid:
        print("Grid sinyal kosong setelah validasi.")
        return 1

    cds = parse_floats(args.cooldown, [])
    exit_grid = build_grid(
        cfg,
        parse_floats(args.sl, DEFAULT_SL),
        parse_floats(args.tp, DEFAULT_TP_RR),
        parse_floats(args.be, DEFAULT_BE_RR),
        parse_floats(args.be_buffer, DEFAULT_BE_BUFFER),
        parse_floats(args.trail, DEFAULT_TRAIL),
        cooldown_list=cds or None,
    )
    if not exit_grid:
        print("Grid exit kosong setelah validasi.")
        return 1
    print(f"Kombinasi sinyal: {len(signal_grid)} "
          f"(tiap kombinasi = 1 scan penuh)")
    print(f"Kombinasi exit  : {len(exit_grid)}")
    print(f"Total baris     : {len(signal_grid) * len(exit_grid)}")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if symbols:
        n_paths = len(symbols)
    else:
        n_paths = len(glob.glob(
            os.path.join(args.data_dir, f"*_{args.interval}.csv")))

    interval_min = INTERVAL_MS[args.interval] // 60_000
    # Dengan beberapa kombo sinyal, pool baru per kombo lebih mahal daripada
    # membayar grid serial: grid exit hanya hitungan detik per kombo.
    grid_workers = args.workers if len(signal_grid) == 1 else 1

    with ProgressUI(enabled=not args.no_progress) as ui:
        bar = ui.bar("Memuat cache candle", total=max(1, n_paths))
        data = load_data(symbols, args.interval, args.days, args.data_dir,
                         progress_cb=bar.advance)
        bar.close()
        if not data:
            ui.log(f"Tidak ada data di {args.data_dir}. Jalankan dulu:")
            ui.log("  python -m tools.backtest.download "
                   "--top 30 --days 30 --interval 1m")
            return 1

        total_candles = sum(len(c) for c in data.values())
        ui.log(f"Data: {len(data)} simbol, {total_candles} candle "
               f"{args.interval}")
        ui.log("CATATAN: skor entry di sini adalah PROKSI CANDLE ONLY "
               "(price action + volume saja).")
        ui.log("Skalanya berbeda dari skor live, "
               "jadi threshold perlu dikalibrasi.")

        is_data, oos_data = split_chronological(data, args.oos)
        ui.log(f"Split kronologis: in sample {len(is_data)} simbol, "
               f"out of sample {len(oos_data)} simbol")

        # ----- Fase A: scan entry untuk tiap kombinasi sinyal -----
        scan_total = len(signal_grid) * (len(is_data) + len(oos_data))
        bar = ui.bar("Memindai sinyal (semua kombo)",
                     total=max(1, scan_total))
        entries_is: dict[SignalParams, list[Entry]] = {}
        entries_oos: dict[SignalParams, list[Entry]] = {}
        vc_cache: dict[SignalParams, Config] = {}
        for i, sp in enumerate(signal_grid, 1):
            vcfg = variant_cfg(cfg, sp)
            vc_cache[sp] = vcfg
            e_is = scan_all(is_data, vcfg, sp.threshold, interval_min,
                            args.workers, progress_cb=bar.advance)
            entries_is[sp] = e_is
            e_oos: list[Entry] = []
            if oos_data:
                e_oos = scan_all(oos_data, vcfg, sp.threshold, interval_min,
                                 args.workers, progress_cb=bar.advance)
            entries_oos[sp] = e_oos
            ui.log(f"[sinyal {i}/{len(signal_grid)}] {sp.label()}"
                   f" -> IS {len(e_is)} entry, OOS {len(e_oos)} entry")
        bar.close()

        # ----- Fase B: grid exit di atas tiap himpunan entry -----
        bar = ui.bar("Menjalankan grid exit",
                     total=max(1, len(signal_grid) * len(exit_grid)))
        rows: list[dict] = []
        for sp in signal_grid:
            e_is = entries_is[sp]
            if not e_is:
                ui.log(f"[lewati] {sp.label()} -> 0 entry")
                bar.advance(len(exit_grid))
                continue
            vcfg = vc_cache[sp]
            # scan_all mengembalikan entry yang sudah terurut waktu, jadi
            # sort ulang di tiap simulasi boleh dilewati.
            hasil = run_grid(exit_grid, e_is, is_data, vcfg, args.equity,
                             args.risk_pct, grid_workers,
                             progress_cb=bar.advance, presorted=True)
            for r in hasil:
                r["signal"] = sp
            rows.extend(hasil)
        bar.close()

        if not rows:
            ui.log("Tidak ada kombinasi dengan entry sama sekali. "
                   "Turunkan threshold atau tambah data.")
            return 1

        rows = score_results(rows, args.w_pnl, args.w_pf, args.w_dd,
                             args.min_trades)
        rows.sort(key=lambda r: (r["score"] is None,
                                 -(r["score"] or 0.0),
                                 -r["metrics"]["net_return_pct"]))

        ranked = [r for r in rows if r["score"] is not None]
        if not ranked:
            ui.log(f"Semua kombinasi punya trade < {args.min_trades} "
                   "(didiskualifikasi). Turunkan --min-trades "
                   "atau tambah data.")

        # ----- Fase C: evaluasi out of sample untuk top K -----
        oos_map: dict = {}
        if oos_data and ranked:
            top_rows = ranked[: max(args.top, 1)]
            bar = ui.bar("Validasi OOS top K", total=len(top_rows))
            for r in top_rows:
                sp, p = r["signal"], r["params"]
                e_oos = entries_oos.get(sp, [])
                if e_oos:
                    res = simulate_portfolio(e_oos, oos_data, p, cfg,
                                             start_equity=args.equity,
                                             risk_pct=args.risk_pct,
                                             presorted=True)
                    oos_map[(sp, p.key())] = res.metrics
                bar.advance()
            bar.close()

    print()
    print_table(rows, oos_map if oos_map else None, args.top,
                vwap_enabled=cfg.signal.vwap.enabled)

    path = write_results_csv(rows, args.results, oos_map)
    print(f"\nCSV lengkap: {path}")

    if ranked:
        best = ranked[0]["params"]
        best_sp = ranked[0]["signal"]
        print(f"\nParameter sinyal baris teratas (laporan saja, "
              f"tidak ikut ke YAML):\n  {best_sp.label()}")
        print("\nCuplikan YAML siap tempel ke config/config.yaml "
              "(parameter exit saja):\n")
        print(yaml_snippet(best, cfg))

    print("Parameter yang TIDAK dioptimasi di sini:")
    print(f"  stops.mode            : {cfg.stops.mode} (hanya percent yang diuji)")
    print(f"  trailing mode ATR     : atr_period={cfg.trailing.atr_period}, "
          f"atr_multiplier={cfg.trailing.atr_multiplier}")
    print("  take_profit multi target dan porsi jual parsial")
    print(f"  trailing.update_step_pct tetap {cfg.trailing.update_step_pct} "
          f"dari config")
    print("  LOT_SIZE dan MIN_NOTIONAL diabaikan (asumsi qty pecahan bebas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
