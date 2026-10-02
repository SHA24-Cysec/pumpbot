"""
Average True Range (ATR) - PURE FUNCTIONS (target unit test utama).

ATR mengukur volatilitas rata-rata dalam satuan HARGA (bukan persen), jadi
jarak SL/TP/BE/trailing menyesuaikan kondisi pasar secara otomatis: saat
volatilitas naik jarak melebar (tidak gampang kesapu noise), saat pasar
tenang jarak menyempit (stop lebih cepat mengunci profit).

Rujukan rumus: J. Welles Wilder Jr., "New Concepts in Technical Trading
Systems" (1978) untuk True Range dan pemulusan Wilder (RMA).

Rumus True Range (TR) satu candle:

    TR = max(high - low, |high - prev_close|, |low - prev_close|)

Candle PERTAMA tidak punya prev_close, jadi TR-nya sengaja TIDAK dihitung.
Akibatnya `true_ranges()` selalu mengembalikan len(candles) - 1 nilai dan
ATR butuh minimal ``period + 1`` candle tertutup. Konvensi ini dipilih agar
tidak ada TR yang lahir dari asumsi (mis. high - low saja), sehingga nilai
ATR tidak berubah hanya karena panjang data berbeda.

  Catatan: Pine Script (`ta.tr(true)`) memakai high - low untuk candle
  pertama sehingga ta.atr(period) sudah bernilai setelah `period` candle.
  Selisih satu candle ini disengaja: data candel bot selalu berasal dari
  kline Binance yang punya prev_close, jadi tidak perlu asumsi apa pun.

Dua metode pemulusan yang didukung (`atr.method`):

  * wilder (RMA): seed = rata-rata sederhana `period` TR pertama, lalu
        RMA_t = ((period - 1) x RMA_(t-1) + TR_t) / period
    Hasilnya identik dengan ta.atr(period) di TradingView.
  * sma: rata-rata sederhana `period` TR terakhir. Lebih responsif terhadap
    lonjakan volatilitas, tapi lebih bergerigi.

Semua fungsi bebas side-effect supaya perilakunya mudah diverifikasi.
"""

from __future__ import annotations

import math
from typing import Iterable, Sequence

WILDER = "wilder"
SMA = "sma"
METHODS: tuple[str, ...] = (WILDER, SMA)

# Batas ATR yang tidak masuk akal dipakai: mencegah kelipatan ATR yang
# membalik posisi (mis. SL di atas entry) lolos tanpa terlihat.
MIN_PERIOD = 2
MAX_PERIOD = 200


def candles_needed(period: int) -> int:
    """Jumlah candle tertutup minimum agar ATR(period) bernilai: period + 1."""
    return max(MIN_PERIOD, int(period)) + 1


def true_ranges(candles: Sequence) -> list[float]:
    """
    Daftar True Range dari candle tertutup, urut lama ke baru.

    Panjang hasil = len(candles) - 1 karena candle pertama tidak punya
    prev_close. Mengembalikan list kosong bila:

      * candle kurang dari 2, atau
      * ada nilai high/low/close yang bukan angka positif berhingga, atau
      * high < low pada salah satu candle.

    Sikap "tolak hitung" dipilih secara sadar: ATR yang dihitung dari data
    rusak dengan TR=0 akan MELAPORKAN volatilitas lebih kecil daripada
    kenyataan. Efeknya jarak SL menyempit dan posisi lebih mudah kesapu,
    jadi lebih aman mengembalikan kosong dan membiarkan pemanggil memakai
    fallback persen.
    """
    n = len(candles)
    if n < 2:
        return []

    out: list[float] = []
    prev_close = _f(getattr(candles[0], "close", 0.0))
    for i in range(1, n):
        c = candles[i]
        high = _f(getattr(c, "high", 0.0))
        low = _f(getattr(c, "low", 0.0))
        if not (high > 0 and low > 0 and prev_close > 0 and high >= low):
            return []
        out.append(max(high - low, abs(high - prev_close),
                       abs(low - prev_close)))
        prev_close = _f(getattr(c, "close", prev_close))
        if not math.isfinite(prev_close) or prev_close <= 0:
            return []
    return out


def atr_wilder(trs: Iterable[float], period: int) -> float:
    """
    ATR dengan pemulusan Wilder (RMA) dari daftar TR yang sudah terurut.

    Seed = rata-rata sederhana `period` TR pertama. Setiap TR berikutnya
    menggeser nilai dengan bobot 1/period. Butuh minimal `period` TR.
    """
    values = [float(v) for v in trs]
    p = int(period)
    if p < MIN_PERIOD or len(values) < p:
        return 0.0
    if any(not math.isfinite(v) for v in values):
        return 0.0

    rma = sum(values[:p]) / p
    for v in values[p:]:
        rma = ((p - 1) * rma + v) / p
    return rma


def atr_sma(trs: Iterable[float], period: int) -> float:
    """ATR dengan rata-rata sederhana `period` TR TERAKHIR."""
    values = [float(v) for v in trs]
    p = int(period)
    if p < MIN_PERIOD or len(values) < p:
        return 0.0
    window = values[-p:]
    if any(not math.isfinite(v) for v in window):
        return 0.0
    return sum(window) / p


def compute_atr(candles: Sequence, period: int = 14,
                method: str = WILDER) -> float:
    """
    ATR dari candle tertutup. 0.0 berarti "tidak bisa dihitung".

    Pemanggil WAJIB memperlakukan 0.0 sebagai sinyal untuk memakai jalur
    fallback (persen), bukan sebagai ATR bernilai nol. Data yang dipakai
    harus candle yang sudah CLOSE: ATR dari candle berjalan berubah-ubah
    dan membuat level SL/TP bergerak tanpa alasan.
    """
    if method not in METHODS:
        raise ValueError(
            f"atr.method '{method}' tidak dikenal (pilihan: {', '.join(METHODS)})")
    trs = true_ranges(candles)
    if not trs:
        return 0.0
    return (atr_wilder(trs, period) if method == WILDER
            else atr_sma(trs, period))


def atr_percent_of_price(atr: float, price: float) -> float:
    """
    ATR relatif terhadap harga, dalam persen (untuk log dan dashboard).

    Berguna untuk membandingkan volatilitas antar koin berharga beda jauh:
    ATR 0.5 pada koin 10 USDT artinya 5%, sedangkan pada BTC itu 0,0006%.
    """
    if atr <= 0 or price <= 0:
        return 0.0
    return atr / price * 100.0


def _f(v) -> float:
    """Konversi longgar ke float; nilai tak bisa dikonversi jadi 0.0."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
