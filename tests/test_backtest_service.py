"""
Test layanan backtest yang dipakai dashboard (tools/backtest/service.py).

Seluruh test di sini berjalan OFFLINE: candle dibuat sintetis lalu ditulis
sebagai cache CSV, sehingga tidak ada satu pun permintaan jaringan.

Yang dijaga:
  * normalisasi dan validasi parameter job dari JSON mentah,
  * aliran NDJSON tetap JSON yang sah (tidak boleh ada Infinity atau NaN,
    karena JSON.parse di browser akan gagal),
  * print() dari modul lama tidak boleh mencemari aliran NDJSON,
  * angka berapa pun diterima tanpa batas atas; hanya nilai yang mustahil
    (jumlah simbol 0, modal <= 0, oos di luar 0..1, dst.) yang ditolak
    dengan pesan error yang jelas,
  * satu putaran optimasi penuh menghasilkan struktur hasil yang dipakai UI.
"""

import csv
import io
import json
import os
import random
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.backtest.download import CSV_HEADER  # noqa: E402
from tools.backtest.engine import Params  # noqa: E402
from tools.backtest.optimize import SignalParams  # noqa: E402
from tools.backtest import service as svc  # noqa: E402
from tools.backtest.util import load_backtest_config  # noqa: E402

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "config", "config.yaml")


# ---------------------------------------------------------------------------
# Cache candle sintetis
# ---------------------------------------------------------------------------

def tulis_cache(path, n=900, seed=7):
    """Tulis CSV candle 1m dengan lonjakan volume berkala agar ada entry."""
    r = random.Random(seed)
    t0 = 1_700_000_000_000
    harga = 100.0
    rows = []
    for i in range(n):
        pump = (i % 70) in (30, 31, 32, 33, 34)
        drift = 0.012 if pump else r.uniform(-0.002, 0.002)
        o = harga
        c = o * (1 + drift)
        h = max(o, c) * (1 + abs(r.uniform(0, 0.002)))
        l = min(o, c) * (1 - abs(r.uniform(0, 0.002)))
        vol = r.uniform(900, 1100) * (12 if pump else 1)
        rows.append({
            "open_time": t0 + i * 60_000, "close_time": t0 + i * 60_000 + 59_999,
            "open": round(o, 4), "high": round(h, 4), "low": round(l, 4),
            "close": round(c, 4), "volume": round(vol, 4),
            "quote_volume": round(vol * c, 4), "trades": int(vol / 8),
            "taker_buy_volume": round(vol * 0.55, 4),
        })
        harga = c
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_HEADER)
        w.writeheader()
        w.writerows(rows)


@pytest.fixture()
def cache(tmp_path):
    """Direktori cache berisi dua simbol sintetis."""
    d = tmp_path / "backtest"
    d.mkdir()
    for i, s in enumerate(["AAAUSDT", "BBBUSDT"]):
        tulis_cache(str(d / f"{s}_1m.csv"), seed=7 + i)
    return str(d)


def jalankan(cache_dir, tmp_path, **override):
    """Jalankan satu job offline dan kembalikan daftar event."""
    dasar = dict(symbols="AAAUSDT,BBBUSDT", days=1, download=False,
                 data_dir=cache_dir, config=CONFIG,
                 results=str(tmp_path / "hasil.csv"),
                 min_trades=1, oos=0.3, thr="20,30", wpa="0.6",
                 sl="1.0,2.0", tp="1.5,2.0", be="0,0.5", be_buffer="0.3",
                 trail="0", top_rows=5)
    dasar.update(override)
    ev = []
    req = svc.JobRequest.from_dict(dasar)
    rc = svc.run_job(req, lambda e, **kw: ev.append({"ev": e, **kw}))
    return rc, ev


def ambil(ev, jenis):
    return [e for e in ev if e["ev"] == jenis]


# ---------------------------------------------------------------------------
# Normalisasi parameter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("masuk,harap", [
    ("1,2,3", [1.0, 2.0, 3.0]),
    ("1, 2 ,3 ", [1.0, 2.0, 3.0]),
    ([1, 2.5], [1.0, 2.5]),
    ("1,,2", [1.0, 2.0]),
    ("1,abc,2", [1.0, 2.0]),
])
def test_daftar_angka(masuk, harap):
    assert svc._as_float_list(masuk, [9.0]) == harap


@pytest.mark.parametrize("kosong", ["", None, [], "  "])
def test_daftar_angka_kosong_pakai_cadangan(kosong):
    assert svc._as_float_list(kosong, [4.0]) == [4.0]


def test_daftar_angka_semua_sampah_pakai_cadangan():
    assert svc._as_float_list("abc,def", [4.0]) == [4.0]


def test_daftar_bilangan_bulat_dibulatkan():
    assert svc._as_int_list("1.4,2.6", []) == [1, 3]


@pytest.mark.parametrize("masuk,harap", [
    ("btcusdt, ethusdt", ["BTCUSDT", "ETHUSDT"]),
    ("BTCUSDT;ETHUSDT", ["BTCUSDT", "ETHUSDT"]),
    ("BTCUSDT,BTCUSDT", ["BTCUSDT"]),
    ("", []),
    (None, []),
])
def test_normalisasi_simbol(masuk, harap):
    assert svc._as_symbols(masuk) == harap


def test_angka_besar_diterima_apa_adanya():
    """Tidak ada lagi batas atas: 9999 hari / simbol / workers diterima."""
    r = svc.JobRequest.from_dict({"top": 9999, "days": 9999, "top_rows": 9999,
                                  "workers": 10_000, "min_trades": 500})
    assert r.top == 9999
    assert r.days == 9999
    assert r.top_rows == 9999
    assert r.workers == 10_000
    assert r.min_trades == 500


def test_workers_tidak_dibatasi_jumlah_cpu():
    """Jumlah proses paralel diputuskan pemakai, bukan dipaksa CPU."""
    r = svc.JobRequest.from_dict({"workers": 10_000})
    assert r.workers == 10_000


def test_nilai_mustahil_ditolak_dengan_pesan_jelas():
    for bad, pesan in [
        ({"top": 0}, "top"),
        ({"days": 0}, "days"),
        ({"workers": 0}, "workers"),
        ({"top_rows": 0}, "top_rows"),
        ({"equity": 0}, "equity"),
        ({"equity": -5}, "equity"),
        ({"oos": 1.5}, "out of sample"),
        ({"oos": -1}, "out of sample"),
        ({"w_pnl": 0, "w_pf": 0, "w_dd": 0}, "bobot"),
    ]:
        with pytest.raises(ValueError, match=pesan):
            svc.JobRequest.from_dict(bad)


def test_nilai_di_batas_teknis_tetap_sah():
    r = svc.JobRequest.from_dict({"oos": 0.0, "equity": 0.01,
                                  "min_trades": 0})
    assert r.oos == 0.0
    assert r.equity == 0.01
    assert r.min_trades == 0


def test_interval_baru_diterima():
    for itv in ("15m", "30m", "1h", "4h", "1d"):
        assert svc.JobRequest.from_dict({"interval": itv}).interval == itv


def test_interval_tidak_didukung_ditolak():
    with pytest.raises(ValueError, match="tidak didukung"):
        svc.JobRequest.from_dict({"interval": "7m"})


def test_vwap_tidak_sah_ditolak():
    with pytest.raises(ValueError, match="vwap"):
        svc.JobRequest.from_dict({"vwap": "kadang"})


def test_simbol_tidak_dipotong():
    """Daftar simbol manual diterima utuh berapa pun panjangnya."""
    banyak = ",".join(f"S{i}USDT" for i in range(200))
    assert len(svc.JobRequest.from_dict({"symbols": banyak}).symbols) == 200


# ---------------------------------------------------------------------------
# Syntax rentang otomatis: awal..akhir atau awal..akhir:langkah
# ---------------------------------------------------------------------------

def test_rentang_diperluas_menjadi_daftar():
    assert svc._as_float_list("0.5..2.0:0.25", []) == \
        [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]
    assert svc._as_float_list("1..5", []) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert svc._as_float_list("2..1:0.5", []) == [2.0, 1.5, 1.0]
    assert svc._as_float_list("0.5,2..3:0.5,5", []) == [0.5, 2.0, 2.5, 3.0, 5.0]
    assert svc._as_float_list("0.1..0.3:0.05", []) == [0.1, 0.15, 0.2, 0.25, 0.3]
    assert svc._as_int_list("10..30:10", []) == [10, 20, 30]


def test_rentang_salah_format_ditolak_jelas():
    for tolakan in ("0.5..abc:1", "1...5", "1..5:0"):
        with pytest.raises(ValueError, match="[Rr]entang"):
            svc._as_float_list(tolakan, [])


def test_rentang_meledak_ditolak():
    """Rentang salah ketik yang menghasilkan jutaan nilai harus terdengar."""
    with pytest.raises(ValueError, match="10,000"):
        svc._as_float_list("0..1000000:0.001", [])


def test_rentang_bisa_dari_job_json():
    r = svc.JobRequest.from_dict({"sl": "1..3:0.5", "thr": "40..60:10"})
    assert r.sl == [1.0, 1.5, 2.0, 2.5, 3.0]
    assert r.thr == [40.0, 50.0, 60.0]


# ---------------------------------------------------------------------------
# Serialisasi aman
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("nilai", [float("inf"), float("-inf"), float("nan")])
def test_nilai_tak_hingga_jadi_none(nilai):
    assert svc._finite(nilai) is None


def test_sanitasi_menelusuri_struktur_bersarang():
    masuk = {"a": [1.0, float("inf")], "b": {"c": float("nan")}}
    assert svc._finite(masuk) == {"a": [1.0, None], "b": {"c": None}}


def test_emitter_menghasilkan_json_ketat():
    buf = io.StringIO()
    emit = svc.make_emitter(buf)
    emit("done", pf=float("inf"), nan=float("nan"), ok=1.5)
    baris = buf.getvalue().strip()
    # parse_constant dipanggil hanya untuk Infinity/NaN, jadi ini menjamin
    # keluaran bisa dibaca JSON.parse di browser
    data = json.loads(baris, parse_constant=lambda c: pytest.fail(
        f"keluaran memuat konstanta tidak sah: {c}"))
    assert data["pf"] is None and data["nan"] is None and data["ok"] == 1.5


def test_emitter_satu_baris_per_event():
    buf = io.StringIO()
    emit = svc.make_emitter(buf)
    emit("log", text="baris satu")
    emit("log", text="baris dua")
    assert len(buf.getvalue().strip().splitlines()) == 2


@pytest.mark.parametrize("nilai,harap", [
    (float("inf"), "inf"), (2.0, "2.00"), (0.0, "0.00"),
    (float("nan"), "-"),
])
def test_format_profit_factor(nilai, harap):
    assert svc.fmt_pf(nilai) == harap


def test_stdout_dialihkan_jadi_event_log():
    keluar = []
    aliran = svc._StdoutToEvents(lambda e, **kw: keluar.append((e, kw)))
    aliran.write("halo\ndunia\n")
    aliran.write("belum selesai")
    aliran.flush()
    assert [kw["text"] for _, kw in keluar] == ["halo", "dunia", "belum selesai"]
    assert all(e == "log" for e, _ in keluar)


def test_stdout_mengabaikan_baris_kosong():
    keluar = []
    aliran = svc._StdoutToEvents(lambda e, **kw: keluar.append(kw["text"]))
    aliran.write("\n\n  \nisi\n")
    assert keluar == ["isi"]


# ---------------------------------------------------------------------------
# Payload untuk dashboard
# ---------------------------------------------------------------------------

@pytest.fixture()
def cfg():
    return load_backtest_config(CONFIG)


def _params(**kw):
    dasar = dict(sl_pct=1.0, tp_rr=2.0, be_rr=0.5, be_buffer_pct=0.3,
                 trail_pct=0.4, trail_step_pct=0.15, fee_pct=0.1)
    dasar.update(kw)
    return Params(**dasar)


def _signal():
    return SignalParams(threshold=55.0, w_pa=0.6, ma_period=20,
                        spike_scale=3.0, structure_candles=30,
                        breakout_lookback=20, swing_neighbors=2,
                        min_candles=30)


def test_apply_payload_punya_empat_kelompok(cfg):
    p = svc.apply_payload(_params(), _signal(), cfg)
    assert set(p) == {"exit", "lookback", "scoring", "cooldown"}


def test_apply_payload_exit_sesuai_parameter(cfg):
    p = svc.apply_payload(_params(sl_pct=1.5, tp_rr=3.0), _signal(), cfg)["exit"]
    assert p["stops.percent_pct"] == 1.5
    assert p["take_profit.rr"] == 3.0
    assert p["stops.mode"] == "percent"
    assert p["take_profit.mode"] == "rr"


def test_breakeven_mati_saat_be_rr_nol_dan_pakai_nilai_config(cfg):
    p = svc.apply_payload(_params(be_rr=0.0), _signal(), cfg)["exit"]
    assert p["breakeven.enabled"] is False
    assert p["breakeven.trigger_rr"] == cfg.breakeven.trigger_rr
    assert p["breakeven.buffer_pct"] == cfg.breakeven.buffer_pct


def test_trailing_mati_saat_trail_nol_dan_pakai_nilai_config(cfg):
    p = svc.apply_payload(_params(trail_pct=0.0), _signal(), cfg)["exit"]
    assert p["trailing.enabled"] is False
    assert p["trailing.percent_pct"] == cfg.trailing.percent_pct


def test_bobot_scoring_selalu_berjumlah_satu(cfg):
    s = svc.apply_payload(_params(), _signal(), cfg)["scoring"]
    total = s["signal.weights.price_action"] + s["signal.weights.volume"]
    assert abs(total - 1.0) < 1e-9


def test_cooldown_kosong_bila_tidak_dioptimasi(cfg):
    assert svc.apply_payload(_params(cooldown_min=None), _signal(), cfg)["cooldown"] == {}


def test_cooldown_terisi_bila_dioptimasi(cfg):
    p = svc.apply_payload(_params(cooldown_min=20.0), _signal(), cfg)["cooldown"]
    assert p == {"signal.cooldown_after_exit_min": 20.0}


def test_semua_kunci_apply_ada_di_daftar_putih_config_writer(cfg):
    """Jaring pengaman: apa pun yang ditawarkan UI harus boleh ditulis."""
    from bot.dashboard.config_writer import ALLOWED_KEYS
    p = svc.apply_payload(_params(cooldown_min=15.0), _signal(), cfg)
    for kelompok, isi in p.items():
        for kunci in isi:
            assert kunci in ALLOWED_KEYS, (
                f"{kunci} (kelompok {kelompok}) belum masuk daftar putih "
                f"config_writer sehingga tombol Terapkan akan gagal")


def test_metrics_payload_menangani_profit_factor_tak_hingga():
    m = svc._metrics_payload({"trades": 3, "profit_factor": float("inf")})
    assert m["profit_factor"] is None
    assert m["profit_factor_text"] == "inf"


# ---------------------------------------------------------------------------
# Batas pengaman
# ---------------------------------------------------------------------------

def test_kombinasi_sinyal_besar_tetap_dijalankan(cache, tmp_path):
    """Grid melebihi batas lama 64 kombinasi sinyal kini boleh dijalankan."""
    rc, ev = jalankan(cache, tmp_path, thr="10,20,30,40,50",
                      wpa="0.1,0.2,0.3,0.4,0.5", ma_period="10,20,30",
                      structure="20,30")
    assert rc == 0
    assert ambil(ev, "done")
    plan = ambil(ev, "plan")[0]
    assert plan["n_signal"] == 150      # 5 thr x 5 wpa x 3 ma x 2 struktur


def test_grid_sinyal_kosong_ditolak(cache, tmp_path):
    with pytest.raises(ValueError, match="Grid sinyal kosong"):
        jalankan(cache, tmp_path, wpa="0,1,2")


def test_grid_exit_kosong_ditolak(cache, tmp_path):
    with pytest.raises(ValueError, match="Grid exit kosong"):
        jalankan(cache, tmp_path, sl="0.01,50,99")


def test_tanpa_data_cache_ditolak(tmp_path):
    kosong = tmp_path / "kosong"
    kosong.mkdir()
    with pytest.raises(ValueError, match="Tidak ada data candle"):
        jalankan(str(kosong), tmp_path)


def test_pesan_tanpa_data_menyarankan_unduh_otomatis(tmp_path):
    kosong = tmp_path / "kosong2"
    kosong.mkdir()
    with pytest.raises(ValueError, match="Unduh data otomatis"):
        jalankan(str(kosong), tmp_path)


# ---------------------------------------------------------------------------
# Satu putaran penuh
# ---------------------------------------------------------------------------

def test_job_penuh_selesai_dan_menulis_csv(cache, tmp_path):
    rc, ev = jalankan(cache, tmp_path)
    assert rc == 0
    done = ambil(ev, "done")
    assert len(done) == 1
    assert os.path.exists(done[0]["csv"])


def test_urutan_fase_sesuai_harapan(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    fase = [e["key"] for e in ambil(ev, "phase")]
    assert fase[0] == "persiapan"
    for wajib in ("muat", "scan", "grid"):
        assert wajib in fase, f"fase {wajib} tidak dilaporkan"
    assert fase.index("scan") < fase.index("grid")


def test_tanpa_unduh_tidak_ada_fase_unduh(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    assert "unduh" not in [e["key"] for e in ambil(ev, "phase")]


def test_plan_melaporkan_ukuran_grid_sebenarnya(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    plan = ambil(ev, "plan")[0]
    assert plan["n_signal"] == 2          # thr 20 dan 30, wpa tunggal
    assert plan["total"] == plan["n_signal"] * plan["n_exit"]
    assert plan["symbols"] == ["AAAUSDT", "BBBUSDT"]


def test_progress_tidak_pernah_melebihi_total(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    for e in ambil(ev, "progress"):
        assert 0 <= e["current"] <= e["total"], e


def test_baris_hasil_lengkap_dan_terurut(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    rows = ambil(ev, "done")[0]["rows"]
    assert rows, "hasil kosong"
    assert [r["rank"] for r in rows] == list(range(1, len(rows) + 1))
    skor = [r["score"] for r in rows]
    assert skor == sorted(skor, reverse=True), "peringkat tidak terurut skor"
    for r in rows:
        assert set(r) >= {"rank", "score", "signal", "exit", "is", "oos",
                          "yaml", "apply"}
        assert r["yaml"].startswith("stops:")


def test_hasil_bisa_diserialkan_json_ketat(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    teks = json.dumps(ambil(ev, "done")[0])
    json.loads(teks, parse_constant=lambda c: pytest.fail(
        f"hasil memuat konstanta tidak sah: {c}"))


def test_top_rows_membatasi_jumlah_baris(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path, top_rows=2)
    assert len(ambil(ev, "done")[0]["rows"]) <= 2


def test_oos_nol_menghasilkan_kolom_oos_kosong(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path, oos=0.0)
    rows = ambil(ev, "done")[0]["rows"]
    assert all(r["oos"] is None for r in rows)


def test_oos_aktif_mengisi_metrik_oos(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path, oos=0.3)
    rows = ambil(ev, "done")[0]["rows"]
    assert any(r["oos"] is not None for r in rows)


def test_min_trades_tinggi_mendiskualifikasi_semua(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path, min_trades=10_000)
    pesan = " ".join(e.get("text", "") for e in ambil(ev, "log"))
    assert "didiskualifikasi" in pesan


def test_meta_melaporkan_parameter_yang_tidak_dioptimasi(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path)
    meta = ambil(ev, "done")[0]["meta"]
    assert meta["not_optimized"]
    assert meta["n_symbols"] == 2
    assert meta["n_candles"] > 0


def test_vwap_bisa_dipaksa_mati(cache, tmp_path):
    _, ev = jalankan(cache, tmp_path, vwap="off")
    assert ambil(ev, "plan")[0]["vwap"] is False
    assert ambil(ev, "done")[0]["meta"]["vwap_enabled"] is False


def test_csv_hasil_memuat_seluruh_kombinasi(cache, tmp_path):
    keluar = tmp_path / "penuh.csv"
    _, ev = jalankan(cache, tmp_path, results=str(keluar), top_rows=1)
    with open(keluar, encoding="utf-8") as fh:
        baris = list(csv.DictReader(fh))
    plan = ambil(ev, "plan")[0]
    assert len(baris) == plan["total"], "CSV harus memuat semua kombinasi"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def test_cli_job_rusak_melapor_error(tmp_path, capsys):
    p = tmp_path / "rusak.json"
    p.write_text("{bukan json", encoding="utf-8")
    rc = svc.main(["--job", str(p)])
    assert rc == 2
    keluar = json.loads(capsys.readouterr().out.strip())
    assert keluar["ev"] == "error"
    assert "Gagal membaca parameter job" in keluar["text"]


def test_cli_parameter_tidak_sah_melapor_error(tmp_path, capsys):
    p = tmp_path / "job.json"
    p.write_text(json.dumps({"interval": "7m"}), encoding="utf-8")
    rc = svc.main(["--job", str(p)])
    assert rc == 1
    baris = [json.loads(l) for l in capsys.readouterr().out.strip().splitlines()]
    assert any(e["ev"] == "error" and "tidak didukung" in e["text"]
               for e in baris)


def test_cli_keluaran_selalu_ndjson_sah(cache, tmp_path, capsys):
    p = tmp_path / "job.json"
    p.write_text(json.dumps({
        "symbols": "AAAUSDT,BBBUSDT", "days": 1, "download": False,
        "data_dir": cache, "config": CONFIG,
        "results": str(tmp_path / "r.csv"),
        "min_trades": 1, "thr": "20", "wpa": "0.6", "sl": "1.0",
        "tp": "2.0", "be": "0", "be_buffer": "0.3", "trail": "0",
    }), encoding="utf-8")
    rc = svc.main(["--job", str(p)])
    assert rc == 0
    baris = capsys.readouterr().out.strip().splitlines()
    assert baris, "tidak ada keluaran"
    for l in baris:
        e = json.loads(l, parse_constant=lambda c: pytest.fail(
            f"NDJSON memuat konstanta tidak sah: {c}"))
        assert "ev" in e
    assert any(json.loads(l)["ev"] == "done" for l in baris)


def test_cli_mengembalikan_stdout_setelah_selesai(tmp_path, capsys):
    p = tmp_path / "job.json"
    p.write_text(json.dumps({"interval": "1h"}), encoding="utf-8")
    svc.main(["--job", str(p)])
    assert sys.stdout is not None
    assert not isinstance(sys.stdout, svc._StdoutToEvents), (
        "stdout asli harus dipulihkan setelah job selesai")


def test_throttle_membatasi_laju():
    t = svc._Throttle(interval=3600)
    assert t.ready("a") is True
    assert t.ready("a") is False
    assert t.ready("b") is True
    assert t.ready("a", force=True) is True
