"""
Test penulis config.yaml milik dashboard.

Modul ini menulis ulang file konfigurasi milik pengguna, jadi risikonya
paling tinggi di antara semua fitur backtest dashboard. Yang dijaga di sini:

  * komentar, urutan kunci, dan format file tidak boleh hilang,
  * kunci di luar daftar putih wajib ditolak,
  * jalur bertitik harus menunjuk kunci yang benar walau nama kuncinya
    muncul di banyak blok (misal `enabled` dan `spike_scale`),
  * setiap kegagalan harus meninggalkan file asli dalam keadaan utuh,
  * cadangan selalu dibuat sebelum file diganti.
"""

import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.dashboard import config_writer as cw  # noqa: E402

CONFIG_ASLI = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "config", "config.yaml")


@pytest.fixture()
def cfg(tmp_path):
    """Salinan config.yaml sungguhan di direktori sementara."""
    tujuan = tmp_path / "config.yaml"
    with open(CONFIG_ASLI, encoding="utf-8") as fh:
        tujuan.write_text(fh.read(), encoding="utf-8")
    return str(tujuan)


def baca(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def doc(path):
    return yaml.safe_load(baca(path))


def n_komentar(teks):
    return sum(1 for l in teks.splitlines() if l.strip().startswith("#"))


# ---------------------------------------------------------------------------
# Integritas file
# ---------------------------------------------------------------------------

def test_komentar_dan_jumlah_baris_tidak_berubah(cfg):
    sebelum = baca(cfg)
    cw.apply_updates(cfg, {"stops.percent_pct": 1.25,
                           "take_profit.rr": 2.0,
                           "signal.volume.ma_period": 25})
    sesudah = baca(cfg)
    assert len(sebelum.splitlines()) == len(sesudah.splitlines())
    assert n_komentar(sebelum) == n_komentar(sesudah)
    assert n_komentar(sesudah) > 100, "config contoh harus tetap kaya komentar"


def test_komentar_ujung_baris_dipertahankan(cfg):
    # baris ini punya komentar di ujung: "volume: 0.30  # lonjakan volume ..."
    cw.apply_updates(cfg, {"signal.weights.volume": 0.44})
    baris = [l for l in baca(cfg).splitlines()
             if l.strip().startswith("volume:") and "#" in l]
    assert baris, "komentar ujung baris hilang setelah penulisan"
    assert "0.44" in baris[0]
    assert "lonjakan volume" in baris[0]


def test_hanya_baris_yang_diminta_yang_berubah(cfg):
    sebelum = baca(cfg).splitlines()
    cw.apply_updates(cfg, {"stops.percent_pct": 2.5})
    sesudah = baca(cfg).splitlines()
    beda = [i for i, (a, b) in enumerate(zip(sebelum, sesudah)) if a != b]
    assert len(beda) == 1, f"ada {len(beda)} baris berubah, harusnya 1"


def test_file_tetap_bisa_dimuat_bot(cfg):
    from tools.backtest.util import load_backtest_config
    cw.apply_updates(cfg, {"stops.percent_pct": 1.0, "take_profit.rr": 2.0})
    c = load_backtest_config(cfg)
    assert c.stops.percent_pct == 1.0
    assert c.take_profit.rr == 2.0


# ---------------------------------------------------------------------------
# Pemilihan kunci yang benar
# ---------------------------------------------------------------------------

def test_enabled_tidak_tertukar_antar_blok(cfg):
    awal = doc(cfg)
    cw.apply_updates(cfg, {"trailing.enabled": True,
                           "breakeven.enabled": False})
    akhir = doc(cfg)
    assert akhir["trailing"]["enabled"] is True
    assert akhir["breakeven"]["enabled"] is False
    # blok lain yang juga punya kunci `enabled` tidak boleh tersentuh
    assert akhir["signal"]["vwap"]["enabled"] == awal["signal"]["vwap"]["enabled"]
    assert akhir["dust_sweep"]["enabled"] == awal["dust_sweep"]["enabled"]


def test_spike_scale_bersarang_tidak_tertukar(cfg):
    awal = doc(cfg)
    cw.apply_updates(cfg, {"signal.volume.spike_scale": 4.25})
    akhir = doc(cfg)
    assert akhir["signal"]["volume"]["spike_scale"] == 4.25
    assert (akhir["signal"]["trade_flow"]["spike_scale"]
            == awal["signal"]["trade_flow"]["spike_scale"])


def test_percent_pct_bersarang_tidak_tertukar(cfg):
    cw.apply_updates(cfg, {"stops.percent_pct": 1.75,
                           "trailing.percent_pct": 0.9})
    d = doc(cfg)
    assert d["stops"]["percent_pct"] == 1.75
    assert d["trailing"]["percent_pct"] == 0.9


def test_kunci_hilang_disisipkan_di_blok_yang_benar(cfg):
    teks = baca(cfg)
    teks = "\n".join(l for l in teks.splitlines()
                     if l.strip() != "update_step_pct: 0.15")
    with open(cfg, "w", encoding="utf-8") as fh:
        fh.write(teks + "\n")
    assert "update_step_pct" not in baca(cfg)

    cw.apply_updates(cfg, {"trailing.update_step_pct": 0.2})
    d = doc(cfg)
    assert d["trailing"]["update_step_pct"] == 0.2
    # tidak boleh nyasar ke blok lain
    assert "update_step_pct" not in d.get("stops", {})


# ---------------------------------------------------------------------------
# Penolakan
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kunci", [
    "mode", "quote_asset", "database.path", "dashboard.host", "dashboard.port",
    "paper.start_balance", "risk.risk_per_trade_pct", "dust_sweep.enabled",
    "logging.file", "universe.max_symbols",
])
def test_kunci_di_luar_daftar_putih_ditolak(cfg, kunci):
    sebelum = baca(cfg)
    with pytest.raises(cw.ConfigWriteError, match="tidak diizinkan"):
        cw.apply_updates(cfg, {kunci: "apa saja"})
    assert baca(cfg) == sebelum


@pytest.mark.parametrize("kunci,nilai", [
    ("stops.percent_pct", 999.0),        # di luar 0.1..20
    ("take_profit.rr", -1.0),            # harus > 0
    ("trailing.percent_pct", 0.0),       # di luar 0.1..20
])
def test_nilai_yang_membuat_config_tidak_sah_dibatalkan(cfg, kunci, nilai):
    sebelum = baca(cfg)
    with pytest.raises(cw.ConfigWriteError, match="ditolak validator"):
        cw.apply_updates(cfg, {kunci: nilai})
    assert baca(cfg) == sebelum, "file harus utuh saat validasi gagal"


@pytest.mark.parametrize("kunci,nilai", [
    ("stops.mode", "ngawur"),
    ("take_profit.mode", "entah"),
    ("trailing.percent_pct", "abc"),
    ("breakeven.enabled", "mungkin"),
    ("signal.volume.ma_period", "abc"),
    ("stops.percent_pct", "bukan angka"),
])
def test_nilai_bertipe_salah_ditolak(cfg, kunci, nilai):
    sebelum = baca(cfg)
    with pytest.raises(cw.ConfigWriteError):
        cw.apply_updates(cfg, {kunci: nilai})
    assert baca(cfg) == sebelum


def test_update_kosong_ditolak(cfg):
    with pytest.raises(cw.ConfigWriteError, match="tidak ada perubahan"):
        cw.apply_updates(cfg, {})


def test_file_tidak_ada_ditolak(tmp_path):
    with pytest.raises(cw.ConfigWriteError, match="tidak ditemukan"):
        cw.apply_updates(str(tmp_path / "hantu.yaml"),
                         {"stops.percent_pct": 1.0})


def test_blok_induk_tidak_ada_ditolak(tmp_path):
    p = tmp_path / "mini.yaml"
    p.write_text("mode: paper\n", encoding="utf-8")
    with pytest.raises(cw.ConfigWriteError, match="tidak ditemukan"):
        cw.apply_updates(str(p), {"stops.percent_pct": 1.0})


# ---------------------------------------------------------------------------
# Cadangan dan dry run
# ---------------------------------------------------------------------------

def test_cadangan_dibuat_dan_isinya_sama_dengan_file_lama(cfg):
    sebelum = baca(cfg)
    hasil = cw.apply_updates(cfg, {"stops.percent_pct": 1.1})
    assert hasil["backup"] and os.path.exists(hasil["backup"])
    assert baca(hasil["backup"]) == sebelum
    assert baca(cfg) != sebelum


def test_dry_run_tidak_menyentuh_file_dan_tidak_membuat_cadangan(cfg):
    sebelum = baca(cfg)
    hasil = cw.apply_updates(cfg, {"stops.percent_pct": 1.1}, dry_run=True)
    assert hasil["dry_run"] is True
    assert hasil["backup"] is None
    assert baca(cfg) == sebelum
    assert not os.path.exists(cw.backup_dir_for(cfg))


def test_tidak_ada_file_sementara_yang_tertinggal(cfg):
    d = os.path.dirname(cfg)
    with pytest.raises(cw.ConfigWriteError):
        cw.apply_updates(cfg, {"stops.percent_pct": 999.0})
    sisa = [f for f in os.listdir(d) if ".tmp-" in f]
    assert not sisa, f"file sementara tertinggal: {sisa}"


def test_cadangan_lama_dipangkas(cfg, monkeypatch):
    monkeypatch.setattr(cw, "MAX_BACKUPS", 3)
    for i in range(6):
        cw.apply_updates(cfg, {"stops.percent_pct": 1.0 + i * 0.1})
    d = cw.backup_dir_for(cfg)
    assert len(os.listdir(d)) == 3


def test_laporan_perubahan_menandai_nilai_yang_benar_benar_berubah(cfg):
    lama = doc(cfg)["stops"]["percent_pct"]
    hasil = cw.apply_updates(cfg, {"stops.percent_pct": lama,
                                   "take_profit.rr": 2.0})
    peta = {c["key"]: c for c in hasil["changes"]}
    assert peta["stops.percent_pct"]["changed"] is False
    assert peta["take_profit.rr"]["changed"] is True
    assert peta["take_profit.rr"]["before"] != 2.0


# ---------------------------------------------------------------------------
# Format nilai
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("nilai,harap", [
    (True, "true"), (False, "false"), (3, "3"), (2.0, "2.0"),
    (0.25, "0.25"), ("percent", "percent"),
])
def test_format_nilai(nilai, harap):
    assert cw.format_value(nilai) == harap


def test_float_bulat_tetap_ditulis_sebagai_pecahan(cfg):
    cw.apply_updates(cfg, {"stops.percent_pct": 2.0})
    baris = [l for l in baca(cfg).splitlines()
             if l.strip().startswith("percent_pct:")][0]
    assert "2.0" in baris
    assert isinstance(doc(cfg)["stops"]["percent_pct"], float)


def test_int_ditulis_tanpa_koma(cfg):
    cw.apply_updates(cfg, {"signal.volume.ma_period": 30.0})
    assert doc(cfg)["signal"]["volume"]["ma_period"] == 30
    assert isinstance(doc(cfg)["signal"]["volume"]["ma_period"], int)


@pytest.mark.parametrize("teks,harap", [
    ("true", True), ("1", True), ("ya", True),
    ("false", False), ("0", False), ("tidak", False),
])
def test_bool_dari_teks(teks, harap):
    assert cw.coerce("breakeven.enabled", teks) is harap


def test_pemisah_komentar_menghormati_tanda_kutip():
    assert cw._split_inline_comment(' "a # b"  # nyata') == (
        ' "a # b"  ', "# nyata")
    assert cw._split_inline_comment(" 1.0") == (" 1.0", "")
    assert cw._split_inline_comment(" nilai#bukankomentar") == (
        " nilai#bukankomentar", "")
