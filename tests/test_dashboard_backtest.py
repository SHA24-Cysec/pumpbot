"""
Test panel backtest di dashboard: pengelola job dan route HTTP-nya.

Route diuji dengan memanggil fungsi handler-nya langsung, bukan lewat
TestClient, supaya tidak menambah dependency test baru ke requirements.
Yang dijaga:

  * aturan WAJIB PAUSE benar-benar ditegakkan di sisi server, bukan hanya
    disembunyikan di UI,
  * hanya satu job boleh jalan pada satu waktu,
  * mesin state pengelola job mengolah tiap event NDJSON dengan benar,
  * pengambilan log bersifat delta supaya polling tetap murah,
  * tombol Terapkan menolak masukan yang tidak masuk akal,
  * snapshot WebSocket tetap ringan dan tidak bocor ke halaman lain.
"""

import asyncio
import os
import signal
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.dashboard import server as srv  # noqa: E402
from bot.dashboard.backtest_runner import (  # noqa: E402
    MAX_LOG_LINES, BacktestBusy, BacktestManager,
)

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "config", "config.yaml")


# ---------------------------------------------------------------------------
# Konteks bot tiruan
# ---------------------------------------------------------------------------

class _DB:
    def __init__(self):
        self.events = []

    def record_event(self, level, kind, msg):
        self.events.append((level, kind, msg))


class _Ctx:
    """Tiruan BotApp seperlunya untuk route backtest."""

    def __init__(self, cfg_path, paused=True):
        from tools.backtest.util import load_backtest_config
        self.cfg = load_backtest_config(cfg_path)
        self.cfg_path = cfg_path
        self.paused = paused
        self.mode = self.cfg.mode
        self.db = _DB()


@pytest.fixture()
def cfg_path(tmp_path):
    tujuan = tmp_path / "config.yaml"
    with open(CONFIG, encoding="utf-8") as fh:
        tujuan.write_text(fh.read(), encoding="utf-8")
    return str(tujuan)


@pytest.fixture()
def app_ctx(cfg_path):
    ctx = _Ctx(cfg_path, paused=True)
    app = srv.create_dashboard_app(ctx)
    return app, ctx


def route(app, path, method="POST"):
    """Ambil fungsi handler sebuah route agar bisa dipanggil langsung."""
    for r in app.routes:
        if getattr(r, "path", None) == path and method in getattr(r, "methods", ()):
            return r.endpoint
    raise AssertionError(f"route {method} {path} tidak terdaftar")


def jalankan(coro):
    return asyncio.run(coro)


def badan(model, **kw):
    return model(**kw)


# ---------------------------------------------------------------------------
# Route terdaftar
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("metode,path", [
    ("GET", "/api/backtest/defaults"),
    ("GET", "/api/backtest/state"),
    ("GET", "/api/backtest/results.csv"),
    ("POST", "/api/backtest/start"),
    ("POST", "/api/backtest/cancel"),
    ("POST", "/api/backtest/apply"),
])
def test_route_backtest_terdaftar(app_ctx, metode, path):
    app, _ = app_ctx
    assert route(app, path, metode) is not None


def test_route_lama_tidak_hilang(app_ctx):
    app, _ = app_ctx
    for metode, path in [("GET", "/"), ("POST", "/api/control/pause"),
                         ("POST", "/api/control/close"),
                         ("GET", "/api/params"), ("POST", "/api/params"),
                         ("GET", "/api/trades"), ("GET", "/api/health")]:
        assert route(app, path, metode) is not None


# ---------------------------------------------------------------------------
# Aturan wajib pause
# ---------------------------------------------------------------------------

def test_start_ditolak_saat_bot_tidak_pause(app_ctx):
    app, ctx = app_ctx
    ctx.paused = False
    r = jalankan(route(app, "/api/backtest/start")(badan(srv.BacktestBody)))
    assert r.status_code == 409
    assert b"need_pause" in r.body


def test_apply_ditolak_saat_bot_tidak_pause(app_ctx):
    app, ctx = app_ctx
    ctx.paused = False
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["exit"])))
    assert r.status_code == 409


def test_start_tidak_menyalakan_proses_saat_ditolak(app_ctx, monkeypatch):
    app, ctx = app_ctx
    ctx.paused = False
    dipanggil = []
    monkeypatch.setattr(app.state.backtest, "start",
                        lambda req: dipanggil.append(req))
    jalankan(route(app, "/api/backtest/start")(badan(srv.BacktestBody)))
    assert not dipanggil, "proses backtest tidak boleh dinyalakan tanpa pause"


def test_pesan_wajib_pause_menjelaskan_alasannya():
    assert "CPU" in srv.PESAN_WAJIB_PAUSE
    assert "pause" in srv.PESAN_WAJIB_PAUSE.lower()


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------

def test_start_meneruskan_jalur_config_yang_dipakai(app_ctx, monkeypatch):
    app, ctx = app_ctx
    terkirim = {}

    async def palsu(req):
        terkirim.update(req)
        return "job123"

    monkeypatch.setattr(app.state.backtest, "start", palsu)
    r = jalankan(route(app, "/api/backtest/start")(
        badan(srv.BacktestBody, days=5)))
    assert r["ok"] is True and r["job_id"] == "job123"
    assert terkirim["config"] == ctx.cfg_path
    assert terkirim["days"] == 5


def test_start_membuang_field_kosong(app_ctx, monkeypatch):
    app, _ = app_ctx
    terkirim = {}

    async def palsu(req):
        terkirim.update(req)
        return "x"

    monkeypatch.setattr(app.state.backtest, "start", palsu)
    jalankan(route(app, "/api/backtest/start")(badan(srv.BacktestBody, days=3)))
    assert "symbols" not in terkirim
    assert "oos" not in terkirim


def test_start_mencatat_event_ke_database(app_ctx, monkeypatch):
    app, ctx = app_ctx

    async def palsu(req):
        return "abc"

    monkeypatch.setattr(app.state.backtest, "start", palsu)
    jalankan(route(app, "/api/backtest/start")(badan(srv.BacktestBody)))
    assert any(k == "BACKTEST" for _, k, _ in ctx.db.events)


def test_start_kedua_ditolak_saat_masih_berjalan(app_ctx, monkeypatch):
    app, _ = app_ctx

    async def sibuk(req):
        raise BacktestBusy("masih ada backtest yang berjalan")

    monkeypatch.setattr(app.state.backtest, "start", sibuk)
    r = jalankan(route(app, "/api/backtest/start")(badan(srv.BacktestBody)))
    assert r.status_code == 409


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

def test_defaults_mengikuti_config_yang_aktif(app_ctx):
    app, ctx = app_ctx
    d = jalankan(route(app, "/api/backtest/defaults", "GET")())
    assert d["min_stop_pct"] == ctx.cfg.stops.min_stop_pct
    assert d["max_stop_pct"] == ctx.cfg.stops.max_stop_pct
    assert d["ma_period"] == ctx.cfg.signal.volume.ma_period
    assert d["breakout_lookback"] == ctx.cfg.signal.price_action.breakout_lookback
    assert d["cpu_count"] >= 1


def test_defaults_rasio_bobot_di_antara_nol_dan_satu(app_ctx):
    app, _ = app_ctx
    d = jalankan(route(app, "/api/backtest/defaults", "GET")())
    assert 0.0 < d["w_pa_ratio"] < 1.0


def test_defaults_tidak_membocorkan_kredensial(app_ctx):
    app, _ = app_ctx
    d = jalankan(route(app, "/api/backtest/defaults", "GET")())
    teks = repr(d).lower()
    for rahasia in ("api_key", "secret", "token", "password"):
        assert rahasia not in teks


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def _isi_hasil(mgr, cfg):
    """Isi manager dengan satu baris hasil palsu yang bentuknya sah."""
    from tools.backtest.engine import Params
    from tools.backtest.optimize import SignalParams
    from tools.backtest.service import row_payload

    p = Params(sl_pct=1.5, tp_rr=2.0, be_rr=0.5, be_buffer_pct=0.3,
               trail_pct=0.4, trail_step_pct=0.15, fee_pct=0.1)
    sp = SignalParams(threshold=55.0, w_pa=0.6, ma_period=22, spike_scale=3.5,
                      structure_candles=28, breakout_lookback=18,
                      swing_neighbors=2, min_candles=30)
    baris = {"params": p, "signal": sp, "score": 0.9, "disqualified": False,
             "metrics": {"trades": 10, "win_rate": 60.0,
                         "net_return_pct": 3.0, "profit_factor": 2.0,
                         "max_dd_pct": 1.0, "avg_r": 0.4, "expectancy": 1.0,
                         "end_equity": 1030.0}}
    mgr.rows = [row_payload(1, baris, {}, cfg)]
    mgr.status = "done"


def test_apply_menulis_parameter_exit(app_ctx):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["exit"])))
    assert r["ok"] is True
    assert r["restart_required"] is True
    assert r["backup"] and os.path.exists(r["backup"])

    from tools.backtest.util import load_backtest_config
    baru = load_backtest_config(ctx.cfg_path)
    assert baru.stops.percent_pct == 1.5
    assert baru.take_profit.rr == 2.0


def test_apply_dry_run_tidak_menulis(app_ctx):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    sebelum = open(ctx.cfg_path, encoding="utf-8").read()
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["exit"], dry_run=True)))
    assert r["ok"] is True and r["dry_run"] is True
    assert r["restart_required"] is False
    assert open(ctx.cfg_path, encoding="utf-8").read() == sebelum


def test_apply_kelompok_lookback(app_ctx):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["lookback"])))
    from tools.backtest.util import load_backtest_config
    baru = load_backtest_config(ctx.cfg_path)
    assert baru.signal.volume.ma_period == 22
    assert baru.signal.price_action.breakout_lookback == 18
    # kelompok exit tidak ikut berubah
    assert baru.stops.percent_pct == ctx.cfg.stops.percent_pct


def test_apply_ditolak_sebelum_ada_hasil(app_ctx):
    app, _ = app_ctx
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["exit"])))
    assert r.status_code == 409


def test_apply_ditolak_saat_job_belum_selesai(app_ctx, cfg_path):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    app.state.backtest.status = "running"
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["exit"])))
    assert r.status_code == 409


def test_apply_peringkat_tidak_ada(app_ctx):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=99, groups=["exit"])))
    assert r.status_code == 404


@pytest.mark.parametrize("groups", [[], ["ngawur"], ["mode", "database"]])
def test_apply_kelompok_tidak_sah(app_ctx, groups):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=groups)))
    assert r.status_code == 400


def test_apply_cooldown_kosong_ditolak(app_ctx):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)   # cooldown_min None
    r = jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["cooldown"])))
    assert r.status_code == 400


def test_apply_mencatat_event_dan_tidak_mengubah_mode(app_ctx):
    app, ctx = app_ctx
    _isi_hasil(app.state.backtest, ctx.cfg)
    jalankan(route(app, "/api/backtest/apply")(
        badan(srv.ApplyBody, rank=1, groups=["exit"])))
    assert any(k == "BACKTEST" for _, k, _ in ctx.db.events)
    isi = open(ctx.cfg_path, encoding="utf-8").read()
    assert "mode: paper" in isi


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def test_csv_belum_ada_menghasilkan_404(app_ctx):
    app, _ = app_ctx
    r = jalankan(route(app, "/api/backtest/results.csv", "GET")())
    assert r.status_code == 404


def test_csv_tersedia_setelah_job(app_ctx, tmp_path):
    app, _ = app_ctx
    p = tmp_path / "hasil.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")
    app.state.backtest.csv_path = str(p)
    r = jalankan(route(app, "/api/backtest/results.csv", "GET")())
    assert getattr(r, "status_code", 200) == 200


# ---------------------------------------------------------------------------
# Mesin state pengelola job
# ---------------------------------------------------------------------------

@pytest.fixture()
def mgr():
    return BacktestManager()


def test_state_awal_idle(mgr):
    s = mgr.state()
    assert s["status"] == "idle"
    assert s["rows"] == [] and s["logs"] == [] and s["error"] == ""
    assert mgr.summary()["running"] is False


def test_event_plan_disimpan(mgr):
    mgr._terapkan({"ev": "plan", "n_signal": 3, "n_exit": 10, "total": 30})
    assert mgr.plan["total"] == 30
    assert any("3 kombinasi sinyal" in l["text"] for l in mgr._logs)


def test_event_phase_mengatur_ulang_progress(mgr):
    mgr._terapkan({"ev": "progress", "current": 5, "total": 10})
    assert mgr.progress["pct"] == 50.0
    mgr._terapkan({"ev": "phase", "key": "grid", "label": "Grid", "total": 100})
    assert mgr.phase["key"] == "grid"
    assert mgr.progress["pct"] == 0.0


def test_persen_progress_selalu_di_rentang_wajar(mgr):
    mgr._terapkan({"ev": "progress", "current": 999, "total": 10})
    assert mgr.progress["pct"] == 100.0
    mgr._terapkan({"ev": "progress", "current": -5, "total": 10})
    assert mgr.progress["pct"] == 0.0


def test_progress_total_nol_tidak_bikin_bagi_nol(mgr):
    mgr._terapkan({"ev": "progress", "current": 1, "total": 0})
    assert mgr.progress["pct"] == 100.0


def test_event_done_mengisi_hasil(mgr):
    mgr._terapkan({"ev": "done", "rows": [{"rank": 1}], "csv": "a.csv",
                   "meta": {"n_symbols": 2}})
    assert mgr.status == "done"
    assert mgr.rows == [{"rank": 1}]
    assert mgr.csv_path == "a.csv"
    assert mgr.progress["pct"] == 100.0


def test_event_error_mengisi_pesan(mgr):
    mgr._terapkan({"ev": "error", "text": "grid kosong"})
    assert mgr.status == "error"
    assert mgr.error == "grid kosong"
    assert mgr._logs[-1]["level"] == "error"


def test_log_delta_lewat_parameter_since(mgr):
    for i in range(5):
        mgr._log(f"baris {i}")
    semua = mgr.state(since=0)
    assert len(semua["logs"]) == 5
    assert semua["log_seq"] == 5
    sebagian = mgr.state(since=3)
    assert [l["text"] for l in sebagian["logs"]] == ["baris 3", "baris 4"]
    assert mgr.state(since=5)["logs"] == []


def test_log_dipangkas_agar_memori_tidak_membengkak(mgr):
    for i in range(MAX_LOG_LINES + 250):
        mgr._log(f"baris {i}")
    assert len(mgr._logs) == MAX_LOG_LINES
    # nomor urut tetap naik walau baris lama dibuang
    assert mgr._logs[-1]["i"] == MAX_LOG_LINES + 250
    assert mgr.state(since=0)["log_seq"] == MAX_LOG_LINES + 250


def test_baris_bukan_json_tetap_masuk_log_apa_adanya(mgr):
    mgr._terapkan({"ev": "log", "text": "pesan biasa"})
    assert mgr._logs[-1]["text"] == "pesan biasa"


def test_event_tidak_dikenal_diabaikan_tanpa_error(mgr):
    mgr._terapkan({"ev": "entah", "x": 1})
    assert mgr.status == "idle"


def test_summary_ringkas_untuk_snapshot(mgr):
    mgr._terapkan({"ev": "phase", "key": "grid", "label": "Grid exit",
                   "total": 10})
    mgr._terapkan({"ev": "progress", "current": 3, "total": 10})
    s = mgr.summary()
    assert set(s) == {"status", "job_id", "phase", "pct", "running"}
    assert s["phase"] == "Grid exit" and s["pct"] == 30.0


def test_cancel_saat_idle_tidak_melakukan_apa_apa(mgr):
    assert jalankan(mgr.cancel()) is False


def test_shutdown_aman_saat_idle(mgr):
    jalankan(mgr.shutdown())


# ---------------------------------------------------------------------------
# Snapshot WebSocket
# ---------------------------------------------------------------------------

def test_snapshot_memuat_ringkasan_backtest():
    mgr = BacktestManager()
    mgr._terapkan({"ev": "phase", "key": "scan", "label": "Scan", "total": 4})
    ringkas = mgr.summary()
    assert ringkas["status"] == "idle"
    assert set(ringkas) == {"status", "job_id", "phase", "pct", "running"}


def test_ringkasan_backtest_tidak_memuat_log_atau_baris():
    """Snapshot dikirim tiap detik, jadi harus tetap kecil."""
    mgr = BacktestManager()
    for i in range(100):
        mgr._log("x" * 200)
    mgr.rows = [{"besar": "y" * 5000}]
    assert len(repr(mgr.summary())) < 300


# ---------------------------------------------------------------------------
# Proses anak tidak boleh jadi yatim
# ---------------------------------------------------------------------------

def test_atexit_dipasang_saat_job_jalan_dan_dilepas_setelah_selesai(monkeypatch):
    """Job berjalan di sesi proses terpisah sehingga TIDAK ikut mati bersama
    grup proses bot. Harus ada jaring pengaman atexit yang dipasang saat job
    dinyalakan dan dilepas lagi setelah job berhenti."""
    from bot.dashboard import backtest_runner as br

    dipasang, dilepas = [], []
    monkeypatch.setattr(br.atexit, "register",
                        lambda fn, *a: dipasang.append(fn) or fn)
    monkeypatch.setattr(br.atexit, "unregister", lambda fn: dilepas.append(fn))

    m = BacktestManager(python_exe=sys.executable)

    async def skenario():
        # parameter sengaja tidak sah supaya proses anak berhenti cepat
        await m.start({"interval": "7m"})
        assert br._bunuh_paksa in dipasang, "atexit tidak dipasang saat start"
        await m._task

    jalankan(skenario())
    assert m.status == "error", m.status
    assert br._bunuh_paksa in dilepas, "atexit tidak dilepas setelah selesai"


def test_job_nyata_melaporkan_error_parameter_lewat_ndjson():
    """Uji jalur penuh subprocess: parameter salah harus sampai ke UI."""
    m = BacktestManager(python_exe=sys.executable)

    async def skenario():
        await m.start({"interval": "7m"})
        await m._task

    jalankan(skenario())
    assert m.status == "error"
    assert "tidak didukung" in m.error
    assert m.state()["logs"], "log kosong padahal job sudah jalan"


def test_bunuh_paksa_aman_untuk_proses_yang_sudah_mati():
    from bot.dashboard.backtest_runner import _bunuh_paksa

    class _Mati:
        returncode = 0
        pid = -1

    _bunuh_paksa(_Mati())          # tidak boleh melempar


def test_bunuh_grup_aman_saat_proses_hilang():
    from bot.dashboard.backtest_runner import _bunuh_grup

    class _Hantu:
        pid = 2 ** 30
        returncode = None

        def send_signal(self, sig):
            raise ProcessLookupError

    _bunuh_grup(_Hantu(), signal.SIGTERM)   # tidak boleh melempar


def test_ctx_menerima_referensi_manager(app_ctx):
    """bot/main.py memakai ctx.backtest untuk mematikan job saat bot berhenti."""
    app, ctx = app_ctx
    assert getattr(ctx, "backtest", None) is app.state.backtest


def test_main_memanggil_shutdown_backtest():
    import inspect
    from bot import main as bm
    src = inspect.getsource(bm.BotApp.shutdown)
    assert "backtest" in src and "shutdown()" in src
