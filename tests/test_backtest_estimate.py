"""
Test perkiraan kebutuhan sumber daya backtest (tools/backtest/estimate.py).

Perkiraan ini murni informasi bagi operator: tidak memblokir job apa pun.
Yang dijaga di sini adalah kebenaran hitungannya, karena angka inilah yang
ditampilkan di dashboard sebelum job dijalankan dan dicatat ke log saat job
dimulai.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.backtest.estimate import (  # noqa: E402
    BYTES_PER_CANDLE,
    CSV_BYTES_PER_ROW,
    estimate_resources,
    ram_mesin,
)


def test_konstanta_terukur_masuk_akal():
    """Konstanta berasal dari pengukuran nyata, bukan angka bebas."""
    assert 100 <= BYTES_PER_CANDLE <= 1000
    assert 50 <= CSV_BYTES_PER_ROW <= 500


def test_hitungan_candle_memori_dan_unduhan():
    # 2 simbol, 1 hari, candle 1m (1440/hari), 2 worker
    e = estimate_resources(2, 1, 60_000, 2)
    assert e["candles"] == 2 * 1440
    assert e["mem_one_copy_bytes"] == 2 * 1440 * BYTES_PER_CANDLE
    # puncak = proses utama + tiap worker memegang salinan sendiri
    assert e["mem_peak_bytes"] == 3 * e["mem_one_copy_bytes"]
    assert e["csv_bytes"] == 2 * 1440 * CSV_BYTES_PER_ROW
    # ceil(1440/1000) = 2 request per simbol, bobot 2 per request
    assert e["api_requests"] == 4
    assert e["api_weight"] == 8


def test_interval_besar_menghemat_memori_lima_kali():
    e1 = estimate_resources(10, 30, 60_000, 1)
    e5 = estimate_resources(10, 30, 300_000, 1)
    assert e5["candles"] * 5 == e1["candles"]
    assert e5["mem_peak_bytes"] < e1["mem_peak_bytes"]


def test_tingkat_risiko_ram():
    dasar = dict(n_symbols=10, days=30, interval_ms=60_000, workers=8)
    puncak = estimate_resources(**dasar)["mem_peak_bytes"]
    assert estimate_resources(
        **dasar, ram_available_bytes=puncak * 2)["risk"] == "aman"
    assert estimate_resources(
        **dasar, ram_available_bytes=puncak / 0.7)["risk"] == "waspada"
    assert estimate_resources(
        **dasar, ram_available_bytes=puncak / 2)["risk"] == "berisiko"
    assert estimate_resources(
        **dasar, ram_available_bytes=None)["risk"] == "tidak_diketahui"


def test_simbol_nol_tidak_meledak():
    e = estimate_resources(0, 30, 60_000, 4)
    assert e["candles"] == 0
    assert e["mem_peak_bytes"] == 0
    assert e["api_requests"] == 0


def test_ram_mesin_angka_atau_none():
    total, avail = ram_mesin()
    assert total is None or total > 0
    assert avail is None or avail > 0
    # di mesin Linux (CI/sandbox) angka harus terbaca dari /proc/meminfo
    if os.path.exists("/proc/meminfo"):
        assert total and total > 0
        assert avail and avail > 0
