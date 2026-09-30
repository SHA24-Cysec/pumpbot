"""
Regresi MemoryError pada fase grid exit backtest.

Gejala asli (Windows, Python 3.14, metode start `spawn`):

    File "...multiprocessing\\spawn.py", line 132, in _main
        self = reduction.pickle.load(from_parent)
    MemoryError

Penyebabnya `run_grid` mengirim SELURUH dict candle ke tiap pekerja lewat
`initargs`, sehingga puncak pemakaian RAM menjadi (1 + jumlah_pekerja) kali
ukuran dataset.

Yang dijaga berkas ini:

  1. Pekerja bisa memuat sendiri candle dari cache CSV lewat DataSpec, dan
     potongannya IDENTIK dengan hasil split di proses induk.
  2. Jumlah pekerja dibatasi oleh RAM yang benar-benar tersedia.
  3. Bila pool tetap kehabisan RAM, grid dilanjutkan serial, bukan gagal
     total setelah fase scan yang berjam-jam.
  4. Hasil grid dengan jalur hemat memori sama persis dengan jalur lama.
"""

from __future__ import annotations

import csv
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.backtest import optimize as opt          # noqa: E402
from tools.backtest.engine import Params            # noqa: E402
from tools.backtest.signals import Entry            # noqa: E402
from tools.backtest.util import load_backtest_config  # noqa: E402

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "config", "config.yaml")

KOLOM = ["open_time", "close_time", "open", "high", "low", "close",
         "volume", "quote_volume", "trades", "taker_buy_volume"]


def _tulis_csv(data_dir, symbol, interval, n=200, mulai=1_700_000_000_000):
    """Buat satu file cache CSV berisi n candle naik perlahan."""
    path = os.path.join(data_dir, f"{symbol}_{interval}.csv")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=KOLOM)
        w.writeheader()
        harga = 100.0
        for i in range(n):
            t = mulai + i * 60_000
            harga *= 1.001 if i % 3 else 0.999
            w.writerow({
                "open_time": t, "close_time": t + 59_999,
                "open": round(harga, 6), "high": round(harga * 1.004, 6),
                "low": round(harga * 0.996, 6), "close": round(harga * 1.001, 6),
                "volume": 1000 + i, "quote_volume": (1000 + i) * harga,
                "trades": 50 + i, "taker_buy_volume": 500 + i,
            })
    return path


@pytest.fixture()
def data_dir(tmp_path):
    d = tmp_path / "cache"
    d.mkdir()
    _tulis_csv(str(d), "AAAUSDT", "1m", n=200)
    _tulis_csv(str(d), "BBBUSDT", "1m", n=200)
    return str(d)


# ------------------------------------------------ 1. DataSpec == split induk

class TestPemuatanDiPekerja:

    def test_potongan_is_identik_dengan_induk(self, data_dir):
        data = opt.load_data([], "1m", 0, data_dir)
        assert data and len(data) == 2

        cut = opt.hitung_cut_ms(data, 0.3)
        is_data, oos_data = opt.split_chronological(data, 0.3)

        spec = opt.DataSpec(symbols=tuple(sorted(is_data.keys())),
                            interval="1m", days=0, data_dir=data_dir,
                            cut_ms=cut, bagian="is")
        dari_pekerja = opt.load_data_spec(spec)

        assert sorted(dari_pekerja.keys()) == sorted(is_data.keys())
        for sym in is_data:
            a = is_data[sym]
            b = dari_pekerja[sym]
            assert len(a) == len(b)
            assert [c.open_time for c in a] == [c.open_time for c in b]
            assert [c.close for c in a] == [c.close for c in b]
        # potongan IS memang lebih pendek dari data penuh
        assert len(dari_pekerja["AAAUSDT"]) < len(data["AAAUSDT"])
        assert len(oos_data["AAAUSDT"]) > 0

    def test_potongan_oos_identik_dengan_induk(self, data_dir):
        data = opt.load_data([], "1m", 0, data_dir)
        cut = opt.hitung_cut_ms(data, 0.25)
        _, oos_data = opt.split_chronological(data, 0.25)
        spec = opt.DataSpec(symbols=tuple(sorted(oos_data.keys())),
                            interval="1m", days=0, data_dir=data_dir,
                            cut_ms=cut, bagian="oos")
        dari_pekerja = opt.load_data_spec(spec)
        for sym in oos_data:
            assert ([c.open_time for c in oos_data[sym]]
                    == [c.open_time for c in dari_pekerja[sym]])

    def test_tanpa_split_memuat_semuanya(self, data_dir):
        data = opt.load_data([], "1m", 0, data_dir)
        spec = opt.DataSpec(symbols=(), interval="1m", days=0,
                            data_dir=data_dir, cut_ms=None, bagian="all")
        semua = opt.load_data_spec(spec)
        assert {k: len(v) for k, v in semua.items()} == \
               {k: len(v) for k, v in data.items()}

    def test_spec_murah_dikirim_antar_proses(self, data_dir):
        """Inti perbaikan: yang dipickle harus kecil, bukan seluruh candle."""
        import pickle
        data = opt.load_data([], "1m", 0, data_dir)
        spec = opt.DataSpec(symbols=tuple(sorted(data.keys())), interval="1m",
                            days=0, data_dir=data_dir, cut_ms=None,
                            bagian="all")
        assert len(pickle.dumps(spec)) < 500
        assert len(pickle.dumps(spec)) < len(pickle.dumps(data)) / 50


# --------------------------------------------------- 2. batas pekerja vs RAM

class TestBatasRam:

    def test_pekerja_dikurangi_saat_ram_sempit(self, monkeypatch):
        # 8 GB total, 2 GB tersedia, tiap pekerja butuh 1 GB.
        monkeypatch.setattr("tools.backtest.estimate.ram_mesin",
                            lambda: (8_000_000_000, 2_000_000_000))
        n, alasan = opt.batasi_workers_ram(8, 1_000_000_000)
        assert n == 1                      # anggaran 1,5 GB / 1 GB = 1
        assert "pekerja dikurangi" in alasan

    def test_tidak_dikurangi_saat_ram_lega(self, monkeypatch):
        monkeypatch.setattr("tools.backtest.estimate.ram_mesin",
                            lambda: (32_000_000_000, 24_000_000_000))
        n, alasan = opt.batasi_workers_ram(8, 500_000_000)
        assert n == 8
        assert alasan == ""

    def test_ram_tidak_diketahui_tidak_membatasi(self, monkeypatch):
        monkeypatch.setattr("tools.backtest.estimate.ram_mesin",
                            lambda: (None, None))
        assert opt.batasi_workers_ram(6, 1_000_000_000) == (6, "")

    def test_satu_pekerja_tidak_pernah_dihitung(self):
        assert opt.batasi_workers_ram(1, 10 ** 12) == (1, "")

    def test_estimasi_bytes_data_memakai_konstanta_terukur(self, data_dir):
        data = opt.load_data([], "1m", 0, data_dir)
        total = sum(len(c) for c in data.values())
        assert opt.estimasi_bytes_data(data) == total * opt.BYTES_PER_CANDLE


# ------------------------------------------- 3/4. run_grid tetap benar & aman

def _konteks(data_dir):
    cfg = load_backtest_config(CONFIG)
    data = opt.load_data([], "1m", 0, data_dir)
    entries = [Entry(symbol="AAAUSDT", entry_idx=10, ts_entry=
                     data["AAAUSDT"][10].open_time,
                     entry_open=data["AAAUSDT"][10].open, score=80.0),
               Entry(symbol="BBBUSDT", entry_idx=20, ts_entry=
                     data["BBBUSDT"][20].open_time,
                     entry_open=data["BBBUSDT"][20].open, score=75.0)]
    grid = [Params(sl_pct=1.0, tp_rr=2.0), Params(sl_pct=2.0, tp_rr=3.0)]
    return cfg, data, entries, grid


class TestRunGrid:

    def test_hasil_sama_dengan_dan_tanpa_data_spec(self, data_dir):
        cfg, data, entries, grid = _konteks(data_dir)
        spec = opt.DataSpec(symbols=tuple(sorted(data.keys())), interval="1m",
                            days=0, data_dir=data_dir, cut_ms=None,
                            bagian="all")

        serial = opt.run_grid(grid, entries, data, cfg, workers=1)
        paralel = opt.run_grid(grid, entries, data, cfg, workers=2,
                               data_spec=spec)

        assert len(paralel) == len(serial) == len(grid)
        for a, b in zip(serial, paralel):
            assert a["params"] == b["params"]
            assert a["metrics"] == b["metrics"]

    def test_memory_error_jatuh_ke_serial(self, data_dir, monkeypatch):
        """Pool gagal dinyalakan tidak boleh membuang hasil fase scan."""
        cfg, data, entries, grid = _konteks(data_dir)
        catatan = []

        class _PoolRusak:
            def __init__(self, *a, **kw):
                raise MemoryError("tidak cukup RAM untuk pool")

        monkeypatch.setattr(opt, "ProcessPoolExecutor", _PoolRusak)
        hasil = opt.run_grid(grid, entries, data, cfg, workers=4,
                             log_cb=catatan.append)

        assert len(hasil) == len(grid)
        assert any("satu proses" in t for t in catatan)

    def test_ram_sempit_memaksa_serial_tanpa_pool(self, data_dir, monkeypatch):
        cfg, data, entries, grid = _konteks(data_dir)
        # Dataset uji kecil (400 candle), jadi RAM tersedia dibuat sangat
        # sempit agar anggarannya tidak cukup untuk satu pekerja pun.
        monkeypatch.setattr("tools.backtest.estimate.ram_mesin",
                            lambda: (8_000_000_000, 150_000))

        class _PoolTerlarang:
            def __init__(self, *a, **kw):
                raise AssertionError("pool tidak boleh dinyalakan")

        monkeypatch.setattr(opt, "ProcessPoolExecutor", _PoolTerlarang)
        catatan = []
        hasil = opt.run_grid(grid, entries, data, cfg, workers=8,
                             log_cb=catatan.append)
        assert len(hasil) == len(grid)
        assert any("pekerja dikurangi" in t for t in catatan)


# ------------------------------------- 5. emulasi Windows (metode `spawn`)

class TestJalurSpawn:
    """
    Windows dan macOS memakai metode start `spawn`: proses pekerja adalah
    interpreter baru yang menerima initargs lewat pickle. Inilah jalur yang
    dulu mati dengan MemoryError, jadi diuji secara eksplisit di sini
    (di Linux pun `spawn` bisa dipaksa).
    """

    def test_pool_spawn_memuat_data_sendiri(self, data_dir):
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor

        cfg, data, entries, grid = _konteks(data_dir)
        spec = opt.DataSpec(symbols=tuple(sorted(data.keys())), interval="1m",
                            days=0, data_dir=data_dir, cut_ms=None,
                            bagian="all")

        with ProcessPoolExecutor(
                max_workers=2, mp_context=mp.get_context("spawn"),
                initializer=opt._init_worker_spec,
                initargs=(entries, spec, cfg, 1000.0, None, False)) as pool:
            hasil = list(pool.map(opt._run_combo, grid))

        serial = opt.run_grid(grid, entries, data, cfg, workers=1)
        assert [h["metrics"] for h in hasil] == [s["metrics"] for s in serial]
