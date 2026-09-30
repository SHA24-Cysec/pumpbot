"""
Layanan backtest terprogram untuk dashboard.

Modul ini membungkus alur CLI ``tools.backtest.optimize`` menjadi satu fungsi
yang bisa dipanggil program lain, lengkap dengan laporan kemajuan terstruktur.
Dashboard menjalankannya sebagai PROSES TERPISAH supaya beban grid search tidak
pernah menyentuh event loop bot:

    python -m tools.backtest.service --job job.json

Setiap baris stdout adalah satu objek JSON (NDJSON). Jenis event:

    {"ev":"plan",     "n_signal":..,"n_exit":..,"total":..,"symbols":[..]}
    {"ev":"phase",    "key":"unduh","label":"Mengunduh data","total":..}
    {"ev":"progress", "key":"unduh","current":..,"total":..,"detail":".."}
    {"ev":"log",      "text":".."}
    {"ev":"done",     "rows":[..],"csv":"..","meta":{..}}
    {"ev":"error",    "text":".."}

Catatan penting soal stdout:
    Fungsi-fungsi lama (build_grid, parse_floats) memakai print() biasa. Kalau
    dibiarkan, tulisannya akan merusak aliran NDJSON. Karena itu sys.stdout
    dialihkan ke penampung yang mengubah tiap baris print menjadi event "log",
    sementara NDJSON tetap ditulis ke stdout asli yang disimpan di awal.

Modul ini tidak memakai API key dan tidak menyentuh endpoint berkunci apa pun.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from tools.backtest.download import (
    DATA_DIR,
    INTERVAL_MS,
    DownloadError,
    download_symbol,
    top_symbols,
)
from tools.backtest.engine import Params, simulate_portfolio
from tools.backtest.optimize import (
    DEFAULT_BE_BUFFER,
    DEFAULT_BE_RR,
    DEFAULT_SL,
    DEFAULT_TP_RR,
    DEFAULT_TRAIL,
    DataSpec,
    SignalParams,
    _expand_range_token,
    build_grid,
    build_signal_grid,
    hitung_cut_ms,
    load_data,
    run_grid,
    score_results,
    split_chronological,
    variant_cfg,
    write_results_csv,
    yaml_snippet,
)
from tools.backtest.signals import Entry, scan_all
from tools.backtest.util import load_backtest_config

# ---------------------------------------------------------------------------
# Kebijakan validasi
#
# Sesuai permintaan pemilik bot, TIDAK ADA lagi batas strategi seperti batas
# jumlah simbol, panjang data, kombinasi sinyal, atau total kombinasi.
# Angka berapa pun diterima selama bermakna secara teknis. Yang tetap
# ditolak (dengan pesan error yang jelas) hanyalah nilai yang mustahil
# dipakai: jumlah simbol 0, modal <= 0, porsi out of sample di luar 0..1,
# jumlah bobot skor <= 0, interval yang bukan interval Binance, langkah
# rentang nol, dan rentang yang meledak menjadi jutaan nilai (salah ketik).
#
# Konsekuensi wajar atas kebebasan ini: job besar memakan CPU dan waktu
# sesuai ukurannya. Estimasi jumlah kombinasi tetap ditampilkan di
# dashboard sebelum tombol jalankan ditekan.
# ---------------------------------------------------------------------------

# Jeda minimum antar event progress sejenis (detik) agar pipa stdout tidak
# dibanjiri puluhan ribu baris saat mengunduh.
PROGRESS_MIN_INTERVAL_S = 0.20

DEFAULT_RESULTS = os.path.join(DATA_DIR, "dashboard-results.csv")

Emitter = Callable[..., None]


# ---------------------------------------------------------------------------
# Serialisasi aman
# ---------------------------------------------------------------------------

def _finite(value: Any) -> Any:
    """Ubah nilai float tak hingga atau NaN menjadi None.

    json.dumps bawaan Python menulis Infinity dan NaN yang BUKAN JSON sah,
    sehingga JSON.parse di browser akan gagal. Semua payload dilewatkan ke
    sini lebih dulu.
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(v) for v in value]
    return value


def fmt_pf(value: float) -> str:
    """Teks profit factor yang aman ditampilkan, termasuk kasus tak hingga."""
    if value == float("inf"):
        return "inf"
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return "-"
    return f"{value:.2f}"


# ---------------------------------------------------------------------------
# Pengalih stdout -> event log
# ---------------------------------------------------------------------------

class _StdoutToEvents(io.TextIOBase):
    """File-like yang mengubah tiap baris print() menjadi satu event log."""

    def __init__(self, emit: Emitter):
        self._emit = emit
        self._buf = ""

    def write(self, text: str) -> int:  # noqa: D102
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.rstrip()
            if line:
                self._emit("log", text=line)
        return len(text)

    def flush(self) -> None:  # noqa: D102
        if self._buf.strip():
            self._emit("log", text=self._buf.strip())
        self._buf = ""

    def writable(self) -> bool:  # noqa: D102
        return True


def make_emitter(stream) -> Emitter:
    """Bangun fungsi emit NDJSON ke stream tertentu (biasanya stdout asli)."""

    def emit(ev: str, **kwargs: Any) -> None:
        payload = _finite({"ev": ev, **kwargs})
        stream.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        stream.flush()

    return emit


class _Throttle:
    """Pembatas laju event progress agar stdout tidak kebanjiran."""

    def __init__(self, interval: float = PROGRESS_MIN_INTERVAL_S):
        self.interval = interval
        self._last: dict[str, Optional[float]] = {}

    def ready(self, key: str, force: bool = False) -> bool:
        """True bila event dengan kunci ini boleh dikirim sekarang.

        Kunci yang belum pernah dipakai SELALU lolos. Memakai 0.0 sebagai
        nilai awal akan salah: time.monotonic() dihitung dari waktu boot
        mesin, jadi pada mesin yang baru menyala nilainya bisa lebih kecil
        dari interval dan event pertama tiap fase justru hilang.
        """
        now = time.monotonic()
        sebelumnya = self._last.get(key)
        if force or sebelumnya is None or now - sebelumnya >= self.interval:
            self._last[key] = now
            return True
        return False


# ---------------------------------------------------------------------------
# Permintaan job
# ---------------------------------------------------------------------------

def _as_float_list(value: Any, fallback: list[float]) -> list[float]:
    """Normalisasi daftar angka dari JSON atau teks dipisah koma.

    Tiap suku boleh berupa angka tunggal atau rentang otomatis
    'awal..akhir' / 'awal..akhir:langkah' (contoh '0.5..2.0:0.25').
    Rentang yang salah format melempar ValueError supaya kesalahan ketik
    terdengar keras, bukan tenggelam diam-diam.
    """
    if value is None or value == "" or value == []:
        return list(fallback)
    if isinstance(value, str):
        chunks: list[Any] = [c.strip() for c in value.split(",")]
    elif isinstance(value, (list, tuple)):
        chunks = list(value)
    else:
        chunks = [value]
    out: list[float] = []
    for c in chunks:
        if c is None or c == "":
            continue
        teks = str(c).strip()
        if not teks:
            continue
        if ".." in teks:
            # Token rentang: salah format harus terdengar keras,
            # bukan tenggelam diam-diam seperti nilai non-angka biasa.
            out.extend(_expand_range_token(teks))
            continue
        try:
            out.append(float(teks))
        except (TypeError, ValueError):
            continue
    return out or list(fallback)


def _as_int_list(value: Any, fallback: list[int]) -> list[int]:
    """Seperti _as_float_list tetapi menghasilkan bilangan bulat."""
    vals = _as_float_list(value, [float(v) for v in fallback])
    return [int(round(v)) for v in vals]


def _as_symbols(value: Any) -> list[str]:
    """Normalisasi daftar simbol dari JSON atau teks dipisah koma."""
    if not value:
        return []
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    else:
        raw = list(value)
    out: list[str] = []
    for s in raw:
        s = str(s).strip().upper()
        if s and s not in out:
            out.append(s)
    return out


def _int_wajib(raw: dict, nama: str, default: int, minimal: int) -> int:
    """Ambil bilangan bulat dari permintaan; tolak nilai mustahil dengan jelas.

    Tidak ada batas atas: angka berapa pun di atas `minimal` diterima.
    """
    v = raw.get(nama)
    if v is None or v == "":
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise ValueError(f"'{nama}' harus berupa angka, dapat: {v!r}")
    if not math.isfinite(f):
        raise ValueError(f"'{nama}' harus angka hingga, dapat: {v!r}")
    n = int(round(f))
    if abs(f - n) > 1e-9:
        raise ValueError(f"'{nama}' harus bilangan bulat, dapat: {v!r}")
    if n < minimal:
        raise ValueError(f"'{nama}' minimal {minimal}, dapat: {n}")
    return n


def _float_wajib(raw: dict, nama: str, default: float) -> float:
    """Ambil bilangan desimal dari permintaan; wajib angka hingga."""
    v = raw.get(nama)
    if v is None or v == "":
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise ValueError(f"'{nama}' harus berupa angka, dapat: {v!r}")
    if not math.isfinite(f):
        raise ValueError(f"'{nama}' harus angka hingga, dapat: {v!r}")
    return f


@dataclass
class JobRequest:
    """Seluruh parameter satu job optimasi, hasil normalisasi dari JSON."""

    # sumber data
    symbols: list[str] = field(default_factory=list)
    top: int = 15
    days: int = 30
    interval: str = "1m"
    download: bool = True
    data_dir: str = DATA_DIR

    # evaluasi
    oos: float = 0.3
    workers: int = 1
    min_trades: int = 30
    equity: float = 1000.0
    risk_pct: Optional[float] = None
    w_pnl: float = 0.4
    w_pf: float = 0.3
    w_dd: float = 0.3
    top_rows: int = 15

    # grid exit
    sl: list[float] = field(default_factory=lambda: list(DEFAULT_SL))
    tp: list[float] = field(default_factory=lambda: list(DEFAULT_TP_RR))
    be: list[float] = field(default_factory=lambda: list(DEFAULT_BE_RR))
    be_buffer: list[float] = field(default_factory=lambda: list(DEFAULT_BE_BUFFER))
    trail: list[float] = field(default_factory=lambda: list(DEFAULT_TRAIL))
    cooldown: list[float] = field(default_factory=list)

    # grid sinyal
    thr: list[float] = field(default_factory=list)
    wpa: list[float] = field(default_factory=list)
    ma_period: list[int] = field(default_factory=list)
    spike_scale: list[float] = field(default_factory=list)
    structure: list[int] = field(default_factory=list)
    breakout: list[int] = field(default_factory=list)
    swing: list[int] = field(default_factory=list)
    min_candles: list[int] = field(default_factory=list)

    vwap: str = "config"
    config: str = os.path.join("config", "config.yaml")
    results: str = DEFAULT_RESULTS

    @classmethod
    def from_dict(cls, raw: dict) -> "JobRequest":
        """Bangun permintaan dari dict JSON.

        Tidak ada batas atas pada angka; yang ditolak hanyalah nilai yang
        mustahil dipakai (lihat kebijakan validasi di atas modul ini), dan
        penolakannya selalu berupa ValueError berpesan jelas.
        """
        raw = dict(raw or {})
        req = cls()

        req.symbols = _as_symbols(raw.get("symbols"))
        req.top = _int_wajib(raw, "top", 15, 1)
        req.days = _int_wajib(raw, "days", 30, 1)

        interval = str(raw.get("interval") or "1m")
        if interval not in INTERVAL_MS:
            raise ValueError(
                f"interval '{interval}' tidak didukung "
                f"(pilihan: {', '.join(sorted(INTERVAL_MS))})")
        req.interval = interval

        req.download = bool(raw.get("download", True))
        req.data_dir = str(raw.get("data_dir") or DATA_DIR)

        req.oos = _float_wajib(raw, "oos", 0.3)
        if not (0.0 <= req.oos < 1.0):
            raise ValueError(
                f"'oos' (porsi out of sample) harus di antara 0 (inklusif) "
                f"dan 1 (eksklusif), dapat: {req.oos}")

        req.workers = _int_wajib(raw, "workers", 1, 1)
        req.min_trades = _int_wajib(raw, "min_trades", 30, 0)

        req.equity = _float_wajib(raw, "equity", 1000.0)
        if req.equity <= 0:
            raise ValueError(
                f"'equity' (modal simulasi) harus lebih besar dari 0, "
                f"dapat: {req.equity}")

        rp = raw.get("risk_pct")
        req.risk_pct = float(rp) if rp not in (None, "", 0) else None

        req.w_pnl = _float_wajib(raw, "w_pnl", 0.4)
        req.w_pf = _float_wajib(raw, "w_pf", 0.3)
        req.w_dd = _float_wajib(raw, "w_dd", 0.3)
        if req.w_pnl + req.w_pf + req.w_dd <= 0:
            raise ValueError(
                "jumlah bobot skor (w_pnl + w_pf + w_dd) harus lebih besar "
                f"dari 0, dapat: {req.w_pnl} + {req.w_pf} + {req.w_dd}")

        req.top_rows = _int_wajib(raw, "top_rows", 15, 1)

        req.sl = _as_float_list(raw.get("sl"), DEFAULT_SL)
        req.tp = _as_float_list(raw.get("tp"), DEFAULT_TP_RR)
        req.be = _as_float_list(raw.get("be"), DEFAULT_BE_RR)
        req.be_buffer = _as_float_list(raw.get("be_buffer"), DEFAULT_BE_BUFFER)
        req.trail = _as_float_list(raw.get("trail"), DEFAULT_TRAIL)
        req.cooldown = _as_float_list(raw.get("cooldown"), [])

        req.thr = _as_float_list(raw.get("thr"), [])
        req.wpa = _as_float_list(raw.get("wpa"), [])
        req.ma_period = _as_int_list(raw.get("ma_period"), [])
        req.spike_scale = _as_float_list(raw.get("spike_scale"), [])
        req.structure = _as_int_list(raw.get("structure"), [])
        req.breakout = _as_int_list(raw.get("breakout"), [])
        req.swing = _as_int_list(raw.get("swing"), [])
        req.min_candles = _as_int_list(raw.get("min_candles"), [])

        vwap = str(raw.get("vwap") or "config")
        if vwap not in ("config", "on", "off"):
            raise ValueError("vwap harus salah satu dari: config, on, off")
        req.vwap = vwap

        req.config = str(raw.get("config") or req.config)
        req.results = str(raw.get("results") or DEFAULT_RESULTS)
        return req


# ---------------------------------------------------------------------------
# Penyusunan hasil untuk dashboard
# ---------------------------------------------------------------------------

def _metrics_payload(m: dict) -> dict:
    """Ubah metrik mentah jadi bentuk yang aman untuk JSON dan UI."""
    pf = m.get("profit_factor", 0.0)
    return {
        "trades": int(m.get("trades", 0)),
        "win_rate": m.get("win_rate", 0.0),
        "net_return_pct": m.get("net_return_pct", 0.0),
        "profit_factor": (pf if isinstance(pf, (int, float))
                          and math.isfinite(pf) else None),
        "profit_factor_text": fmt_pf(pf),
        "max_dd_pct": m.get("max_dd_pct", 0.0),
        "avg_r": m.get("avg_r", 0.0),
        "expectancy": m.get("expectancy", 0.0),
        "end_equity": m.get("end_equity", 0.0),
    }


def apply_payload(p: Params, sp: SignalParams, cfg) -> dict:
    """
    Susun nilai config yang bisa ditulis ulang dari satu baris hasil.

    Dipisah tiga kelompok karena tingkat kepercayaannya berbeda:

      exit    : aman. Ini persis isi cuplikan YAML milik CLI optimizer.
      lookback: cukup aman. Jumlah candle yang dipakai detector, tidak
                bergantung pada skala skor.
      scoring : HATI-HATI. Skor entry di backtest hanya proksi candle
                (price action + volume), skalanya berbeda dari skor live yang
                memakai lima detector, jadi threshold dan bobot hasil backtest
                tidak bisa dipindahkan mentah-mentah.
    """
    be_on = p.be_rr > 0
    tr_on = p.trail_pct > 0
    return {
        "exit": {
            "stops.mode": "percent",
            "stops.percent_pct": p.sl_pct,
            "take_profit.mode": "rr",
            "take_profit.rr": p.tp_rr,
            "breakeven.enabled": be_on,
            "breakeven.trigger_rr": (p.be_rr if be_on
                                     else cfg.breakeven.trigger_rr),
            "breakeven.buffer_pct": (p.be_buffer_pct if be_on
                                     else cfg.breakeven.buffer_pct),
            "trailing.enabled": tr_on,
            "trailing.percent_pct": (p.trail_pct if tr_on
                                     else cfg.trailing.percent_pct),
            "trailing.update_step_pct": p.trail_step_pct,
        },
        "lookback": {
            "signal.volume.ma_period": sp.ma_period,
            "signal.volume.spike_scale": sp.spike_scale,
            "signal.price_action.structure_candles": sp.structure_candles,
            "signal.price_action.breakout_lookback": sp.breakout_lookback,
            "signal.price_action.swing_neighbors": sp.swing_neighbors,
            "signal.min_candles": sp.min_candles,
        },
        "scoring": {
            "signal.score_threshold": sp.threshold,
            "signal.weights.price_action": round(sp.w_pa, 6),
            "signal.weights.volume": round(1.0 - sp.w_pa, 6),
        },
        "cooldown": ({"signal.cooldown_after_exit_min": p.cooldown_min}
                     if p.cooldown_min is not None else {}),
    }


def row_payload(rank: int, r: dict, oos_map: dict, cfg) -> dict:
    """Ubah satu baris hasil grid menjadi objek siap kirim ke dashboard."""
    p, sp = r["params"], r["signal"]
    om = oos_map.get((sp, p.key()))
    return {
        "rank": rank,
        "score": r.get("score"),
        "disqualified": bool(r.get("disqualified")),
        "signal": {
            "threshold": sp.threshold,
            "w_pa": sp.w_pa,
            "ma_period": sp.ma_period,
            "spike_scale": sp.spike_scale,
            "structure_candles": sp.structure_candles,
            "breakout_lookback": sp.breakout_lookback,
            "swing_neighbors": sp.swing_neighbors,
            "min_candles": sp.min_candles,
            "label": sp.label(),
        },
        "exit": {
            "sl_pct": p.sl_pct,
            "tp_rr": p.tp_rr,
            "be_rr": p.be_rr,
            "be_buffer_pct": p.be_buffer_pct,
            "trail_pct": p.trail_pct,
            "trail_step_pct": p.trail_step_pct,
            "fee_pct": p.fee_pct,
            "cooldown_min": p.cooldown_min,
        },
        "is": _metrics_payload(r["metrics"]),
        "oos": _metrics_payload(om) if om else None,
        "yaml": yaml_snippet(p, cfg),
        "apply": apply_payload(p, sp, cfg),
    }


# ---------------------------------------------------------------------------
# Inti job
# ---------------------------------------------------------------------------

def _resolve_symbols(req: JobRequest, cfg, emit: Emitter) -> list[str]:
    """Tentukan daftar simbol: manual dari user atau otomatis top volume."""
    if req.symbols:
        return list(req.symbols)
    emit("log", text=f"Mengambil {req.top} simbol volume tertinggi "
                     f"dari Binance publik ...")
    syms = top_symbols(req.top, cfg, cfg.quote_asset)
    if not syms:
        raise DownloadError("Tidak ada simbol yang lolos filter universe.")
    return syms


def _do_download(req: JobRequest, symbols: list[str], emit: Emitter,
                 throttle: _Throttle) -> None:
    """Unduh atau lanjutkan cache candle untuk seluruh simbol."""
    now = int(time.time() * 1000)
    start_ms = now - req.days * 86_400_000
    emit("phase", key="unduh", label="Mengunduh data candle",
         total=len(symbols))

    gagal: list[str] = []
    for i, symbol in enumerate(symbols, 1):
        if throttle.ready("unduh", force=True):
            emit("progress", key="unduh", current=i - 1, total=len(symbols),
                 detail=f"{symbol} ({i}/{len(symbols)})")
        try:
            info = download_symbol(symbol, req.interval, start_ms, now,
                                   data_dir=req.data_dir)
        except DownloadError as exc:
            gagal.append(symbol)
            emit("log", text=f"{symbol}: GAGAL {exc}")
            continue
        except Exception as exc:  # jaringan putus, dll
            gagal.append(symbol)
            emit("log", text=f"{symbol}: GAGAL {type(exc).__name__}: {exc}")
            continue
        bolong = f", {info['gaps']} candle bolong" if info["gaps"] else ""
        emit("log", text=f"{symbol}: {info['rows']} candle "
                         f"(+{info['new_rows']} baru){bolong}")
    emit("progress", key="unduh", current=len(symbols), total=len(symbols),
         detail="selesai")
    if gagal:
        emit("log", text=f"Simbol gagal diunduh: {', '.join(gagal)}")
    if len(gagal) == len(symbols):
        raise DownloadError(
            "Semua simbol gagal diunduh. Periksa koneksi internet.")


def run_job(req: JobRequest, emit: Emitter) -> int:
    """
    Jalankan satu job optimasi penuh dan laporkan kemajuannya lewat emit.

    Return 0 bila sukses. Kegagalan yang bisa dijelaskan dilempar sebagai
    ValueError atau DownloadError dan ditangkap pemanggil.
    """
    throttle = _Throttle()
    t_mulai = time.time()

    emit("phase", key="persiapan", label="Menyiapkan grid", total=1)
    cfg = load_backtest_config(req.config)

    if req.vwap == "on":
        cfg.signal.vwap.enabled = True
    elif req.vwap == "off":
        cfg.signal.vwap.enabled = False

    signal_grid = build_signal_grid(
        cfg,
        req.thr or [cfg.signal.score_threshold],
        req.wpa or [0.5, 0.6, 0.7],
        req.ma_period or [cfg.signal.volume.ma_period],
        req.spike_scale or [cfg.signal.volume.spike_scale],
        req.structure or [cfg.signal.price_action.structure_candles],
        req.breakout or [cfg.signal.price_action.breakout_lookback],
        req.swing or [cfg.signal.price_action.swing_neighbors],
        req.min_candles or [cfg.signal.min_candles],
    )
    if not signal_grid:
        raise ValueError("Grid sinyal kosong setelah validasi. "
                         "Pastikan rasio bobot price action ada di antara "
                         "0 dan 1.")

    exit_grid = build_grid(
        cfg, req.sl, req.tp, req.be, req.be_buffer, req.trail,
        cooldown_list=req.cooldown or None)
    if not exit_grid:
        raise ValueError(
            "Grid exit kosong setelah validasi. Semua nilai SL mungkin di "
            f"luar batas config [{cfg.stops.min_stop_pct}, "
            f"{cfg.stops.max_stop_pct}].")

    symbols = _resolve_symbols(req, cfg, emit)
    total_rows = len(signal_grid) * len(exit_grid)
    emit("plan", n_signal=len(signal_grid), n_exit=len(exit_grid),
         total=total_rows, symbols=symbols,
         vwap=bool(cfg.signal.vwap.enabled))

    # ----- Unduh data -----
    if req.download:
        _do_download(req, symbols, emit, throttle)
    else:
        emit("log", text="Mode cache: tidak ada pengunduhan, "
                         "memakai CSV yang sudah ada.")

    # ----- Muat cache -----
    emit("phase", key="muat", label="Memuat cache candle", total=len(symbols))
    dimuat = {"n": 0}

    def _muat_cb(step: int) -> None:
        dimuat["n"] += step
        if throttle.ready("muat"):
            emit("progress", key="muat", current=dimuat["n"],
                 total=len(symbols), detail="")

    data = load_data(symbols, req.interval, req.days, req.data_dir,
                     progress_cb=_muat_cb)
    emit("progress", key="muat", current=len(symbols), total=len(symbols),
         detail="selesai")
    if not data:
        hint = ("" if req.download else
                " Centang 'Unduh data otomatis' lalu jalankan lagi.")
        raise ValueError(
            f"Tidak ada data candle di {req.data_dir} untuk interval "
            f"{req.interval}.{hint}")

    total_candles = sum(len(c) for c in data.values())
    emit("log", text=f"Data siap: {len(data)} simbol, "
                     f"{total_candles:,} candle {req.interval}")
    emit("log", text="CATATAN: skor entry di backtest adalah PROKSI CANDLE "
                     "(price action + volume saja), skalanya berbeda dari "
                     "skor live yang memakai lima detector.")

    cut_ms = hitung_cut_ms(data, req.oos)
    is_data, oos_data = split_chronological(data, req.oos)
    # Resep pemuatan untuk pekerja grid: mereka memuat candle IS sendiri
    # dari cache CSV, jadi dataset tidak perlu dikirim lewat pickle.
    # Tanpa ini, tiap pekerja pada metode start `spawn` (Windows) harus
    # meng-unpickle salinan penuh dan bisa mati dengan MemoryError.
    is_spec = DataSpec(symbols=tuple(sorted(is_data.keys())),
                       interval=req.interval, days=req.days,
                       data_dir=req.data_dir, cut_ms=cut_ms,
                       bagian="is" if cut_ms is not None else "all")
    # Dict candle asli tidak dipakai lagi setelah displit. Melepasnya lebih
    # awal mengurangi jejak RAM proses induk sebelum pool dinyalakan.
    # PENTING: saat oos <= 0, split_chronological mengembalikan OBJEK YANG
    # SAMA sebagai is_data. Mengosongkannya di situ akan menghapus seluruh
    # data kerja, jadi pengosongan hanya dilakukan bila memang ada salinan
    # terpisah.
    simbol_dipakai = sorted(data.keys())
    n_simbol = len(data)
    if data is not is_data and data is not oos_data:
        data.clear()
    data = {}
    emit("log", text=f"Split kronologis: in sample {len(is_data)} simbol, "
                     f"out of sample {len(oos_data)} simbol")

    interval_min = INTERVAL_MS[req.interval] // 60_000

    # ----- Fase A: pindai entry per kombinasi sinyal -----
    scan_total = len(signal_grid) * (len(is_data) + len(oos_data))
    emit("phase", key="scan", label="Memindai sinyal", total=max(1, scan_total))
    dipindai = {"n": 0}

    def _scan_cb(step: int) -> None:
        dipindai["n"] += step
        if throttle.ready("scan"):
            emit("progress", key="scan", current=dipindai["n"],
                 total=max(1, scan_total), detail="")

    entries_is: dict[SignalParams, list[Entry]] = {}
    entries_oos: dict[SignalParams, list[Entry]] = {}
    vc_cache: dict[SignalParams, Any] = {}
    for i, sp in enumerate(signal_grid, 1):
        vcfg = variant_cfg(cfg, sp)
        vc_cache[sp] = vcfg
        e_is = scan_all(is_data, vcfg, sp.threshold, interval_min,
                        req.workers, progress_cb=_scan_cb)
        entries_is[sp] = e_is
        e_oos: list[Entry] = []
        if oos_data:
            e_oos = scan_all(oos_data, vcfg, sp.threshold, interval_min,
                             req.workers, progress_cb=_scan_cb)
        entries_oos[sp] = e_oos
        emit("log", text=f"[sinyal {i}/{len(signal_grid)}] {sp.label()} "
                         f"-> IS {len(e_is)} entry, OOS {len(e_oos)} entry")
    emit("progress", key="scan", current=max(1, scan_total),
         total=max(1, scan_total), detail="selesai")

    # ----- Fase B: grid exit -----
    emit("phase", key="grid", label="Menjalankan grid exit", total=total_rows)
    dihitung = {"n": 0}

    def _grid_cb(step: int) -> None:
        dihitung["n"] += step
        if throttle.ready("grid"):
            emit("progress", key="grid", current=dihitung["n"],
                 total=total_rows, detail="")

    # Dengan banyak kombinasi sinyal, membuat pool baru tiap kombinasi lebih
    # mahal daripada menjalankan grid exit secara serial.
    grid_workers = req.workers if len(signal_grid) == 1 else 1

    rows: list[dict] = []
    for sp in signal_grid:
        e_is = entries_is[sp]
        if not e_is:
            emit("log", text=f"[lewati] {sp.label()} -> 0 entry")
            _grid_cb(len(exit_grid))
            continue
        hasil = run_grid(exit_grid, e_is, is_data, vc_cache[sp], req.equity,
                         req.risk_pct, grid_workers, progress_cb=_grid_cb,
                         presorted=True, data_spec=is_spec,
                         log_cb=lambda t: emit("log", text=t))
        for r in hasil:
            r["signal"] = sp
        rows.extend(hasil)
    emit("progress", key="grid", current=total_rows, total=total_rows,
         detail="selesai")

    if not rows:
        raise ValueError(
            "Tidak ada kombinasi yang menghasilkan entry sama sekali. "
            "Turunkan threshold, perpanjang periode, atau tambah simbol.")

    rows = score_results(rows, req.w_pnl, req.w_pf, req.w_dd, req.min_trades)
    rows.sort(key=lambda r: (r["score"] is None,
                             -(r["score"] or 0.0),
                             -r["metrics"]["net_return_pct"]))
    ranked = [r for r in rows if r["score"] is not None]
    if not ranked:
        emit("log", text=f"Semua kombinasi punya trade < {req.min_trades} "
                         f"sehingga didiskualifikasi. Turunkan 'minimal "
                         f"trade' atau tambah data.")

    # ----- Fase C: validasi out of sample untuk top K -----
    oos_map: dict = {}
    top_rows = ranked[: req.top_rows]
    if oos_data and top_rows:
        emit("phase", key="oos", label="Validasi out of sample",
             total=len(top_rows))
        for i, r in enumerate(top_rows, 1):
            sp, p = r["signal"], r["params"]
            e_oos = entries_oos.get(sp, [])
            if e_oos:
                res = simulate_portfolio(e_oos, oos_data, p, cfg,
                                         start_equity=req.equity,
                                         risk_pct=req.risk_pct,
                                         presorted=True)
                oos_map[(sp, p.key())] = res.metrics
            if throttle.ready("oos") or i == len(top_rows):
                emit("progress", key="oos", current=i, total=len(top_rows),
                     detail="")

    # ----- Simpan dan kirim hasil -----
    csv_out = write_results_csv(rows, req.results, oos_map)
    payload_rows = [row_payload(i, r, oos_map, cfg)
                    for i, r in enumerate(top_rows or rows[: req.top_rows], 1)]

    emit("done",
         rows=payload_rows,
         csv=csv_out,
         meta={
             "symbols": simbol_dipakai,
             "n_symbols": n_simbol,
             "n_candles": total_candles,
             "interval": req.interval,
             "days": req.days,
             "n_signal": len(signal_grid),
             "n_exit": len(exit_grid),
             "total_rows": len(rows),
             "ranked_rows": len(ranked),
             "min_trades": req.min_trades,
             "oos": req.oos,
             "equity": req.equity,
             "vwap_enabled": bool(cfg.signal.vwap.enabled),
             "elapsed_sec": round(time.time() - t_mulai, 1),
             "not_optimized": [
                 f"stops.mode tetap {cfg.stops.mode} (hanya percent diuji)",
                 "take_profit multi target dan porsi jual parsial",
                 f"trailing.update_step_pct tetap "
                 f"{cfg.trailing.update_step_pct} dari config",
                 "LOT_SIZE dan MIN_NOTIONAL diabaikan "
                 "(asumsi qty pecahan bebas)",
             ],
         })
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Definisi argumen CLI layanan backtest."""
    p = argparse.ArgumentParser(
        prog="python -m tools.backtest.service",
        description="Jalankan satu job optimasi dan laporkan progres NDJSON.")
    p.add_argument("--job", default="-",
                   help="path file JSON berisi parameter job, "
                        "'-' berarti baca dari stdin")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    """Titik masuk: baca job JSON, jalankan, tulis NDJSON ke stdout."""
    args = build_parser().parse_args(argv)

    real_stdout = sys.stdout
    emit = make_emitter(real_stdout)

    try:
        if args.job == "-":
            raw = json.load(sys.stdin)
        else:
            with open(args.job, encoding="utf-8") as fh:
                raw = json.load(fh)
    except Exception as exc:
        emit("error", text=f"Gagal membaca parameter job: {exc}")
        return 2

    # Semua print() dari modul lama dialihkan menjadi event log supaya aliran
    # NDJSON di stdout asli tetap bersih.
    sys.stdout = _StdoutToEvents(emit)
    try:
        req = JobRequest.from_dict(raw)
        return run_job(req, emit)
    except KeyboardInterrupt:
        emit("error", text="Dibatalkan.")
        return 130
    except (ValueError, DownloadError) as exc:
        emit("error", text=str(exc))
        return 1
    except Exception as exc:  # pengaman terakhir, tetap laporkan ke UI
        import traceback
        emit("error", text=f"{type(exc).__name__}: {exc}")
        emit("log", text=traceback.format_exc())
        return 1
    finally:
        try:
            sys.stdout.flush()
        except Exception:
            pass
        sys.stdout = real_stdout


if __name__ == "__main__":
    raise SystemExit(main())
