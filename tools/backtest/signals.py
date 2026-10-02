"""
Pencari entry dari data klines (PROKSI CANDLE ONLY).

PENTING: klines tidak memuat order book, aliran trade, maupun aktivitas whale.
Karena itu skor di sini hanya memakai dua detector asli bot yang bisa jalan
dengan data candle saja:

    PriceActionDetector dan VolumeDetector

Bobot keduanya diambil dari config lalu dinormalisasi ulang HANYA di antara
detector yang tersedia. Penalti dan veto ManipulationDetector diterapkan persis
seperti SignalEngine.evaluate. Skala skor proksi ini berbeda dari skor live
(yang memakai lima detector), jadi threshold wajib dikalibrasi ulang.

Sinyal dihitung pada candle i yang sudah close, dan entry terjadi pada OPEN
candle i+1. Tidak ada data masa depan yang dipakai saat menilai candle i.
"""

from __future__ import annotations

import os
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Callable, Optional

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.models import Candle, Ticker24h
from bot.signal_engine.detectors import (
    ManipulationDetector,
    PriceActionDetector,
    VolumeDetector,
)
from bot.signal_engine.change24h import Change24hFilter
from bot.signal_engine.vwap import VWAPFilter
from bot.utils import clamp

# Detector yang tersedia dengan data candle saja.
AVAILABLE = ("price_action", "volume")

# Jumlah candle 1 menit dalam 24 jam (untuk ticker sintetis).
_DAY_MIN = 1440


@dataclass(frozen=True)
class Entry:
    """Satu kandidat entry hasil pemindaian klines."""
    symbol: str
    entry_idx: int        # indeks candle tempat entry terjadi (i+1)
    ts_entry: int         # open_time candle entry
    entry_open: float     # harga open candle entry
    score: float          # skor proksi 0..100


def synthetic_ticker(symbol: str, candles: list[Candle], i: int,
                     interval_min: int = 1) -> Ticker24h:
    """
    Buat Ticker24h sintetis untuk ManipulationDetector.

    price_change_pct dihitung dari candle 24 jam sebelumnya, atau dari candle
    paling awal yang tersedia bila data belum genap 24 jam. Hanya data sampai
    indeks i yang dipakai (tanpa lookahead).
    """
    last = candles[i]
    span = max(1, _DAY_MIN // max(interval_min, 1))
    ref_idx = max(0, i - span)
    ref = candles[ref_idx].open
    change = ((last.close - ref) / ref * 100.0) if ref > 0 else 0.0
    window = candles[ref_idx:i + 1]
    return Ticker24h(
        ts=last.close_time,
        symbol=symbol,
        last_price=last.close,
        price_change_pct=change,
        high=max(c.high for c in window),
        low=min(c.low for c in window),
        volume=sum(c.volume for c in window),
        quote_volume=sum(c.quote_volume for c in window),
        trade_count=sum(c.trades for c in window),
        bid=last.close,
        ask=last.close,
    )


class RollingTicker:
    """Pembangkit Ticker24h sintetis dengan jendela bergulir O(1).

    Secara fungsi identik dengan :func:`synthetic_ticker`, tetapi maksimum,
    minimum, dan total volume pada jendela 24 jam tidak dihitung ulang dari
    nol pada setiap candle. at() wajib dipanggil dengan indeks yang naik
    monoton (seperti loop di scan_symbol).

    Detail: deque monoton menyimpan indeks kandidat ekstrem, jadi nilai
    max/min yang dikembalikan persis sama dengan hasil scan linear. Jumlah
    volume dijaga lewat tambah/kurang sehingga bisa bergeser sekitar 1e-12
    relatif dibanding sum() naif; tidak berpengaruh pada skor detector.
    """

    def __init__(self, symbol: str, candles: list[Candle],
                 interval_min: int = 1):
        self._symbol = symbol
        self._c = candles
        self._span = max(1, _DAY_MIN // max(interval_min, 1))
        self._hi: deque[int] = deque()   # indeks, nilai high menurun
        self._lo: deque[int] = deque()   # indeks, nilai low menaik
        self._vol = 0.0
        self._qv = 0.0
        self._tr = 0
        self._i: Optional[int] = None    # indeks terakhir yang sudah dimuat

    def _push(self, k: int) -> None:
        """Masukkan candle k ke jendela (sisi kanan)."""
        c = self._c[k]
        while self._hi and self._c[self._hi[-1]].high <= c.high:
            self._hi.pop()
        self._hi.append(k)
        while self._lo and self._c[self._lo[-1]].low >= c.low:
            self._lo.pop()
        self._lo.append(k)
        self._vol += c.volume
        self._qv += c.quote_volume
        self._tr += c.trades

    def _drop(self, k: int) -> None:
        """Keluarkan candle k dari jendela (sisi kiri)."""
        c = self._c[k]
        if self._hi and self._hi[0] == k:
            self._hi.popleft()
        if self._lo and self._lo[0] == k:
            self._lo.popleft()
        self._vol -= c.volume
        self._qv -= c.quote_volume
        self._tr -= c.trades

    def at(self, i: int) -> Ticker24h:
        """Ticker pada indeks i; i harus >= panggilan sebelumnya."""
        if self._i is None:
            for k in range(max(0, i - self._span), i + 1):
                self._push(k)
        else:
            for k in range(self._i + 1, i + 1):
                self._push(k)
                keluar = k - self._span - 1
                if keluar >= 0:
                    self._drop(keluar)
        self._i = i

        last = self._c[i]
        ref = self._c[max(0, i - self._span)].open
        change = ((last.close - ref) / ref * 100.0) if ref > 0 else 0.0
        return Ticker24h(
            ts=last.close_time,
            symbol=self._symbol,
            last_price=last.close,
            price_change_pct=change,
            high=self._c[self._hi[0]].high,
            low=self._c[self._lo[0]].low,
            volume=self._vol,
            quote_volume=self._qv,
            trade_count=self._tr,
            bid=last.close,
            ask=last.close,
        )


def _weights(cfg: Config) -> tuple[float, float, float]:
    """Bobot price_action dan volume plus jumlahnya (dinormalisasi ulang)."""
    w = cfg.signal.weights or {}
    w_pa = float(w.get("price_action", 0.0) or 0.0)
    w_vol = float(w.get("volume", 0.0) or 0.0)
    wsum = w_pa + w_vol
    if wsum <= 0:
        # Semua bobot nol: pakai rata-rata sederhana agar tidak bagi nol.
        return 0.5, 0.5, 1.0
    return w_pa, w_vol, wsum


def _buffer_cap(cfg: Config) -> int:
    """Batas riwayat candle yang benar-benar dibaca detector per panggilan.

    Ketiga detector candle hanya membaca irisan dari UJUNG
    ``buf.candles`` (indexing negatif), sehingga isi yang lebih tua tidak
    pernah menyentuh hasil:
      * VolumeDetector      : ``ma_period + 1``
      * PriceActionDetector : ``max(structure_candles, breakout_lookback+2, 15)``
      * ManipulationDetector: ``max(pump_dump_window_min, 12)``

    Menahan buffer sebesar jawara kebutuhan di atas mengubah biaya
    ``list(buf.candles)`` dari O(seluruh riwayat) menjadi O(1) per candle;
    tanpa ini scan bersifat kuadratik pada data panjang. Margin 2x + 16
    bersifat defensif: hasil identik untuk cap berapa pun >= kebutuhan.
    """
    pa = cfg.signal.price_action
    need = max(
        int(getattr(cfg.signal.volume, "ma_period", 20) or 20) + 1,
        int(getattr(pa, "structure_candles", 30) or 30),
        int(getattr(pa, "breakout_lookback", 20) or 20) + 2,
        int(getattr(cfg.signal.manipulation, "pump_dump_window_min", 15) or 15),
        15,
    )
    # Filter VWAP membaca window anchor + baseline lonjakan; tanpa ini buffer
    # terpotong dan hasil backtest berbeda dari live.
    vcfg = getattr(cfg.signal, "vwap", None)
    if vcfg is not None and getattr(vcfg, "enabled", False):
        need = max(need, int(vcfg.anchor_lookback_candles)
                   + int(vcfg.pump_baseline_candles))
    return need * 2 + 16


def scan_symbol(symbol: str, candles: list[Candle], cfg: Config,
                threshold: float, interval_min: int = 1,
                prune: bool = True) -> list[Entry]:
    """
    Pindai satu simbol dan hasilkan daftar Entry.

    Pruning: VolumeDetector dihitung lebih dulu. Batas atas skor adalah
    (w_pa x 100 + w_vol x vol_score) / jumlah_bobot karena price_action tidak
    pernah melebihi 100 dan penalti manipulasi hanya bisa menurunkan skor.
    Bila batas atas masih di bawah threshold, detector lain dilewati. Hasil
    dengan prune=True dan prune=False identik.
    """
    out: list[Entry] = []
    n = len(candles)
    if n < 2:
        return out

    w_pa, w_vol, wsum = _weights(cfg)
    min_candles = max(int(cfg.signal.min_candles), 1)

    pa_det = PriceActionDetector()
    vol_det = VolumeDetector()
    manip_det = ManipulationDetector()
    # Satu instance per pemanggilan supaya cache tidak bocor antar varian config.
    vwap_filter = VWAPFilter() if cfg.signal.vwap.enabled else None
    band_filter = (Change24hFilter()
                   if cfg.signal.change_24h.enabled else None)

    buf = SymbolBuffer(symbol, max_candles=_buffer_cap(cfg))
    rolling = RollingTicker(symbol, candles, interval_min)

    # Sinyal di candle i, entry di candle i+1 -> i berhenti di n-2.
    for i in range(n - 1):
        buf.on_candle(candles[i])
        if i + 1 < min_candles:
            continue

        vol_res = vol_det.score(buf, cfg)
        if not vol_res.eligible:
            continue

        upper = (w_pa * 100.0 + w_vol * vol_res.score) / wsum
        # Pembulatan monoton, jadi round(upper) >= round(skor final).
        if prune and round(upper, 1) < threshold:
            continue

        pa_res = pa_det.score(buf, cfg)
        if not pa_res.eligible:
            continue

        base = (w_pa * pa_res.score + w_vol * vol_res.score) / wsum

        buf.on_ticker(rolling.at(i))
        manip_res = manip_det.score(buf, cfg)
        penalty = cfg.signal.manipulation.weight * manip_res.score / 100.0
        final = clamp(base * (1.0 - penalty), 0.0, 100.0)

        if manip_res.veto:
            continue
        # Pembulatan disamakan dengan SignalEngine.evaluate.
        final = round(final, 1)
        if final < threshold:
            continue

        # Gate Anchored VWAP: setelah veto dan threshold, sebelum entry dibuat.
        # buf.last_price sudah = close candle i (di-set on_candle), jadi
        # keputusan memakai harga yang sama seperti jalur live.
        if vwap_filter is not None and not vwap_filter.score(buf, cfg).eligible:
            continue

        # Gate band perubahan 24 jam (nilai absolut): kelas filter yang SAMA
        # dengan jalur live, sehingga koin yang lolos di backtest sama dengan
        # koin yang lolos di bot.
        # buf.ticker sudah di-set di atas (ticker 24 jam sintetis dari candle),
        # jadi angkanya setara priceChangePercent Binance tanpa lookahead.
        if band_filter is not None and not band_filter.score(buf, cfg).eligible:
            continue

        nxt = candles[i + 1]
        out.append(Entry(symbol=symbol, entry_idx=i + 1, ts_entry=nxt.open_time,
                         entry_open=nxt.open, score=final))
    return out


# ---------------------------------------------------------------------------
# Paralelisasi per simbol
# ---------------------------------------------------------------------------

_CTX: dict = {}


def _init_worker(cfg: Config, threshold: float, interval_min: int) -> None:
    """Initializer proses pekerja: simpan config sekali per proses."""
    _CTX["cfg"] = cfg
    _CTX["threshold"] = threshold
    _CTX["interval_min"] = interval_min


def _scan_job(item: tuple[str, list[Candle]]) -> list[Entry]:
    """Pekerjaan satu proses: pindai satu simbol."""
    symbol, candles = item
    return scan_symbol(symbol, candles, _CTX["cfg"], _CTX["threshold"],
                       _CTX["interval_min"])


def scan_all(data: dict[str, list[Candle]], cfg: Config,
             threshold: Optional[float] = None, interval_min: int = 1,
             workers: int = 1,
             progress_cb: Optional[Callable[[int], None]] = None) -> list[Entry]:
    """
    Pindai banyak simbol dan kembalikan entry terurut waktu.

    workers <= 1 berjalan serial (memudahkan debug dan test).
    progress_cb bila ada dipanggil tiap satu simbol selesai dipindai.
    """
    thr = float(cfg.signal.score_threshold if threshold is None else threshold)
    items = [(sym, candles) for sym, candles in sorted(data.items()) if candles]
    entries: list[Entry] = []

    if workers <= 1 or len(items) <= 1:
        for sym, candles in items:
            entries.extend(scan_symbol(sym, candles, cfg, thr, interval_min))
            if progress_cb:
                progress_cb(1)
    else:
        max_workers = min(workers, len(items), (os.cpu_count() or 1) * 2)
        with ProcessPoolExecutor(max_workers=max_workers,
                                 initializer=_init_worker,
                                 initargs=(cfg, thr, interval_min)) as pool:
            for res in pool.map(_scan_job, items):
                entries.extend(res)
                if progress_cb:
                    progress_cb(1)

    entries.sort(key=lambda e: (e.ts_entry, e.symbol))
    return entries
