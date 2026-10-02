"""
Gate entry: BAND perubahan 24 jam (`signal.change_24h`).

Masalah yang diselesaikan: bot momentum mudah tergoda membeli koin yang sudah
naik terlalu jauh dalam sehari. Koin seperti itu berisiko dump tepat setelah
entry (membeli di puncak). Di sisi lain, koin yang sedang jatuh tajam juga
berbahaya (menangkap pisau jatuh). Gate ini menuntut koin SEDANG BERGERAK
dengan besar pergerakan di dalam band yang ditentukan config, default 6%
sampai 10%.

Sumber angka: `Ticker24h.price_change_pct`, yaitu `priceChangePercent` dari
ticker 24 jam Binance (harga sekarang dibanding 24 jam bergulir ke belakang).

Nilai yang dipakai adalah NILAI ABSOLUT, sama seperti ManipulationDetector di
repo ini. Alasannya: pump dan dump adalah dua wajah manipulasi yang sama, jadi
keduanya diukur dengan ukuran yang sama. Yang dinyatakan lolos atau ditolak
adalah BESAR PERGERAKAN, bukan arahnya:

  * |perubahan| >= min_pct dan |perubahan| <= max_pct -> LOLOS
    (naik 6 sampai 10% lolos, turun 6 sampai 10% juga lolos)
  * |perubahan| <  min_pct -> DITOLAK, koin terlalu tenang untuk dikejar
  * |perubahan| >  max_pct -> DITOLAK, pergerakan terlalu ekstrem:
    kalau naik berisiko beli di puncak, kalau turun itu pisau jatuh

Batas kedua ujung bersifat INKLUSIF: 6.0% dan 10.0% sama-sama lolos.

Konsekuensi yang perlu diketahui operator: koin yang TURUN 6 sampai 10% ikut
lolos gate ini, karena besar pergerakannya sama dengan koin yang naik 6 sampai
10%. Penalti manipulasi 24 jam yang juga berlaku di repo ini memakai angka
absolut yang sama, sehingga koin seperti itu tetap dipotong skornya dan butuh
skor dasar jauh lebih tinggi untuk benar-benar tereksekusi.

FILTER INI GATE KERAS, bukan penalti skor. Kontraknya sama dengan VWAPFilter:
tidak menyumbang skor, hanya menentukan `eligible`, dan alasannya dicatat di
snapshot dashboard supaya operator bisa melihat koin mana yang dilewati serta
angkanya berapa.

Fail-closed: bila ticker 24 jam belum tersedia (tidak ada `buf.ticker`), koin
DITOLAK dengan alasan eksplisit. Sikap ini dipilih karena gate ini justru ada
untuk mencegah entry tanpa informasi perubahan 24 jam. Data ticker sudah
tersedia sejak watchlist dibangun, jadi kondisi ini hanya muncul sesaat setelah
startup atau saat stream terputus.
"""

from __future__ import annotations

import math

from bot.config import Config
from bot.models import DetectorResult
from bot.data_collector.buffers import SymbolBuffer


def _fmt_pct(nilai: float) -> str:
    """
    Angka persen seperlunya: 6.0 menjadi "6", 5.999 tetap "5.999".

    Dipakai di pesan alasan supaya angka di dekat batas tidak salah baca
    (5,999% yang dibulatkan menjadi "6.00%" akan terlihat seperti kontradiksi
    dengan batas minimal 6%).
    """
    teks = f"{nilai:.3f}".rstrip("0").rstrip(".")
    return teks if teks not in ("", "-") else "0"


def arah(nilai: float) -> str:
    """Arah pergerakan untuk laporan: naik, turun, atau datar."""
    if nilai > 0:
        return "naik"
    if nilai < 0:
        return "turun"
    return "datar"


def decide(change_pct, min_pct: float, max_pct: float) -> dict:
    """
    Putuskan satu nilai perubahan 24 jam terhadap band. Fungsi murni.

    `change_pct` boleh None (tidak ada data). Return dict berisi `passed`,
    `change_pct` (nilai bertanda apa adanya), `abs_pct` (besar pergerakan yang
    diuji), `direction`, `min_pct`, `max_pct`, dan `reason` (kosong bila lolos).
    Dipakai bersama oleh bot live dan backtest supaya keputusannya identik.
    """
    hasil = {
        "passed": False,
        "change_pct": None,
        "abs_pct": None,
        "direction": "",
        "min_pct": float(min_pct),
        "max_pct": float(max_pct),
        "reason": "",
    }
    if change_pct is None:
        hasil["reason"] = ("ticker 24 jam belum tersedia, perubahan 24 jam "
                           "tidak bisa dipastikan")
        return hasil

    try:
        nilai = float(change_pct)
    except (TypeError, ValueError):
        hasil["reason"] = f"perubahan 24 jam tidak bisa dibaca ({change_pct!r})"
        return hasil
    if not math.isfinite(nilai):
        hasil["reason"] = f"perubahan 24 jam tidak wajar ({nilai})"
        return hasil
    hasil["change_pct"] = nilai
    hasil["abs_pct"] = abs(nilai)
    hasil["direction"] = arah(nilai)
    besar = hasil["abs_pct"]

    if besar < hasil["min_pct"]:
        hasil["reason"] = (f"pergerakan 24 jam {_fmt_pct(nilai)}% terlalu "
                           f"kecil, minimal {_fmt_pct(hasil['min_pct'])}% pada "
                           f"nilai absolut")
        return hasil
    if besar > hasil["max_pct"]:
        if nilai > 0:
            hasil["reason"] = (f"kenaikan 24 jam {_fmt_pct(nilai)}% di atas "
                               f"maksimal {_fmt_pct(hasil['max_pct'])}% "
                               f"(risiko beli di puncak)")
        else:
            hasil["reason"] = (f"penurunan 24 jam {_fmt_pct(nilai)}% lebih "
                               f"dalam dari maksimal "
                               f"{_fmt_pct(hasil['max_pct'])}% (koin sedang "
                               f"dibuang, risiko pisau jatuh)")
        return hasil

    hasil["passed"] = True
    return hasil


class Change24hFilter:
    """
    Gate entry berbasis band perubahan 24 jam (nilai absolut).

    Mengembalikan DetectorResult dengan `eligible` = lolos band, sehingga
    engine memperlakukannya sama seperti filter lain (menentukan `eligible`,
    bukan menambah skor). Tidak ada cache: angkanya datang dari ticker yang
    sudah disimpan buffer, jadi perhitungannya sangat murah.
    """

    name = "change24h"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        ccfg = cfg.signal.change_24h
        ticker = buf.ticker
        change = None
        if ticker is not None:
            change = getattr(ticker, "price_change_pct", None)
        info = decide(change, ccfg.min_pct, ccfg.max_pct)
        return DetectorResult(
            self.name,
            score=100.0 if info["passed"] else 0.0,
            eligible=bool(info["passed"]),
            details={
                "passed": bool(info["passed"]),
                "change_pct": (round(info["change_pct"], 4)
                               if info["change_pct"] is not None else None),
                "abs_pct": (round(info["abs_pct"], 4)
                            if info["abs_pct"] is not None else None),
                "direction": info["direction"],
                "min_pct": info["min_pct"],
                "max_pct": info["max_pct"],
                "reason": info["reason"],
            },
        )


# Filter TIDAK ikut ALL_DETECTORS: bukan sumber skor, hanya gerbang kelayakan.
FILTER_DETECTORS = {"change24h": Change24hFilter}
