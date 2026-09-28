"""
Perkiraan kebutuhan sumber daya sebuah job backtest.

Murni INFORMASI: tidak ada yang diblokir atau dibatasi di sini. Angka
ditampilkan di dashboard sebelum job dijalankan (via /api/backtest/defaults
+ hitungan di browser) dan dicatat ke log saat job dimulai, supaya ukuran
job terlihat sebelum terlambat. Sesuai kebijakan pemilik bot, keputusan
tetap di tangan operator.

Dasar angka (terukur, bukan karangan):
- BYTES_PER_CANDLE: ±314 byte per objek Candle, terukur lewat delta RSS
  pada Python 3.13 (dataclass 10 field); dibulatkan ke 320 agar sedikit
  konservatif. Dataset dimuat SELURUHNYA ke RAM sekaligus.
- CSV_BYTES_PER_ROW: ±99 byte terukur dari format cache CSV asli
  (tools/backtest/download.py); dibulatkan ke 100.
- Salinan data: proses service memegang satu salinan penuh, lalu pada fase
  grid exit tiap worker ProcessPoolExecutor menerima salinan sendiri lewat
  initargs, sehingga perkiraan puncak = (1 + workers) x salinan. Ini batas
  atas konservatif; di Linux dengan fork start method bisa lebih hemat
  berkat copy-on-write.
- Unduhan: endpoint /api/v3/klines Binance mengembalikan maksimum 1000
  candle per request dengan bobot 2, dan batas bobot 6000 per menit.
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional

# Konstanta terukur (lihat docstring modul).
BYTES_PER_CANDLE = 320
CSV_BYTES_PER_ROW = 100

# Batas API Binance untuk perkiraan unduhan.
KLINES_PER_REQUEST = 1000
KLINES_REQUEST_WEIGHT = 2
WEIGHT_LIMIT_PER_MINUTE = 6000

GB = 1_000_000_000


def _ram_linux() -> tuple[Optional[int], Optional[int]]:
    try:
        with open("/proc/meminfo", encoding="ascii") as fh:
            info = {}
            for baris in fh:
                k, _, v = baris.partition(":")
                info[k.strip()] = int(v.strip().split()[0]) * 1024
        return info.get("MemTotal"), info.get("MemAvailable")
    except Exception:
        return None, None


def _ram_windows() -> tuple[Optional[int], Optional[int]]:
    try:
        import ctypes

        class _MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        st = _MEMORYSTATUSEX()
        st.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st)):
            return int(st.ullTotalPhys), int(st.ullAvailPhys)
    except Exception:
        pass
    return None, None


def _ram_macos() -> tuple[Optional[int], Optional[int]]:
    try:
        out = subprocess.run(["sysctl", "-n", "hw.memsize"],
                             capture_output=True, timeout=5)
        return int(out.stdout.strip()), None
    except Exception:
        return None, None


def ram_mesin() -> tuple[Optional[int], Optional[int]]:
    """(total, tersedia) RAM mesin dalam byte; (None, None) bila tak diketahui.

    Tersedia lebih relevan daripada total karena aplikasi lain ikut memakai
    RAM. Di macOS angka tersedia tidak tersedia lewat sysctl, jadi None.
    """
    if os.name == "nt":
        return _ram_windows()
    if os.path.exists("/proc/meminfo"):
        return _ram_linux()
    if sys_platform() == "darwin":
        return _ram_macos()
    return None, None


def sys_platform() -> str:
    """Kemas sys.platform agar mudah di-mock saat test."""
    import sys
    return sys.platform


def estimate_resources(n_symbols: int, days: int, interval_ms: int,
                       workers: int,
                       ram_available_bytes: Optional[int] = None) -> dict:
    """Perkiraan candle, memori, ukuran cache CSV, dan beban unduhan.

    Semua angka adalah perkiraan konservatif untuk membantu keputusan,
    bukan janji pasti. Jumlah entry hasil scan tidak bisa diramal dari
    sini (bergantung strategi), jadi memori daftar entry tidak dihitung;
    pengalaman menunjukkan data candle tetap komponen terbesar.
    """
    n_symbols = max(0, int(n_symbols))
    days = max(0, int(days))
    interval_ms = max(1, int(interval_ms))
    workers = max(1, int(workers))

    per_hari = 86_400_000 // interval_ms
    candles_per_symbol = days * per_hari
    candles = n_symbols * candles_per_symbol

    mem_one = candles * BYTES_PER_CANDLE
    # Fase grid exit: proses utama + tiap worker memegang salinan sendiri.
    mem_peak = mem_one * (1 + workers)
    csv_bytes = candles * CSV_BYTES_PER_ROW

    api_requests = n_symbols * (
        (candles_per_symbol + KLINES_PER_REQUEST - 1) // KLINES_PER_REQUEST)
    api_weight = api_requests * KLINES_REQUEST_WEIGHT
    menit_bobot = api_weight / WEIGHT_LIMIT_PER_MINUTE

    ram = ram_available_bytes
    if ram and ram > 0:
        if mem_peak <= ram * 0.5:
            risk = "aman"
        elif mem_peak <= ram * 0.9:
            risk = "waspada"
        else:
            risk = "berisiko"
    else:
        risk = "tidak_diketahui"

    return {
        "n_symbols": n_symbols,
        "days": days,
        "interval_ms": interval_ms,
        "workers": workers,
        "candles": candles,
        "candles_per_symbol": candles_per_symbol,
        "mem_one_copy_bytes": mem_one,
        "mem_peak_bytes": mem_peak,
        "csv_bytes": csv_bytes,
        "api_requests": api_requests,
        "api_weight": api_weight,
        "download_min_minutes": menit_bobot,
        "ram_available_bytes": ram,
        "risk": risk,
        "mem_one_copy_gb": mem_one / GB,
        "mem_peak_gb": mem_peak / GB,
        "csv_gb": csv_bytes / GB,
        "ram_gb": (ram / GB) if ram else None,
    }
