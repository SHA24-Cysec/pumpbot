"""
Regresi pengerasan dashboard (temuan audit F-10).

Yang dijaga:

  1. TANPA DASHBOARD_TOKEN, dashboard hanya melayani klien loopback.
     Permintaan dari alamat lain ditolak 403 (dulu: semua endpoint terbuka,
     termasuk /api/control/close dan /api/params).
  2. Header Host divalidasi (anti DNS rebinding). Host asing ditolak 400
     walaupun koneksinya datang dari 127.0.0.1.
  3. Daftar host tambahan bisa diisi lewat DASHBOARD_ALLOWED_HOSTS.
  4. Dengan DASHBOARD_TOKEN aktif, akses non-lokal tetap wajib membawa token.
  5. `run_dashboard` MENOLAK start bila host bukan loopback sementara token
     kosong (dulu hanya logger.warning lalu tetap jalan).

Berbeda dengan test lama, berkas ini memakai TestClient terhadap aplikasi
DASHBOARD YANG SEBENARNYA (`create_dashboard_app`), bukan tiruan middleware,
supaya proteksi yang diuji tidak bisa "menyimpang" dari yang dipakai bot.
"""

from __future__ import annotations

import asyncio
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient    # noqa: E402

from bot.dashboard import server as srv      # noqa: E402

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "config", "config.yaml")


class _DB:
    def __init__(self):
        self.events = []

    def record_event(self, level, kind, msg, symbol=None):
        self.events.append((level, kind, msg))


class _Ctx:
    """Tiruan BotApp seperlunya untuk membangun aplikasi dashboard."""

    def __init__(self, cfg_path):
        from tools.backtest.util import load_backtest_config
        self.cfg = load_backtest_config(cfg_path)
        self.cfg_path = cfg_path
        self.paused = True
        self.mode = self.cfg.mode
        self.db = _DB()
        self.executor = types.SimpleNamespace(positions={})


@pytest.fixture()
def ctx(tmp_path):
    tujuan = tmp_path / "config.yaml"
    with open(CONFIG, encoding="utf-8") as fh:
        tujuan.write_text(fh.read(), encoding="utf-8")
    return _Ctx(str(tujuan))


def _client(ctx, host_header="127.0.0.1", client_ip="127.0.0.1"):
    """TestClient dengan header Host dan alamat klien yang bisa diatur."""
    app = srv.create_dashboard_app(ctx)
    c = TestClient(app, base_url=f"http://{host_header}",
                   client=(client_ip, 12345))
    return c


# ------------------------------------------------------- 1. akses non-lokal

class TestTanpaTokenHanyaLokal:

    def test_klien_lokal_diterima(self, monkeypatch, ctx):
        monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        monkeypatch.delenv("DASHBOARD_ALLOWED_HOSTS", raising=False)
        r = _client(ctx).get("/api/health")
        assert r.status_code == 200

    def test_klien_jaringan_ditolak(self, monkeypatch, ctx):
        monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        monkeypatch.delenv("DASHBOARD_ALLOWED_HOSTS", raising=False)
        r = _client(ctx, client_ip="192.168.1.50").get("/api/health")
        assert r.status_code == 403
        assert "mesin yang sama" in r.json()["error"]

    def test_endpoint_kontrol_juga_ditolak(self, monkeypatch, ctx):
        """Yang paling berbahaya: tutup posisi dan ubah parameter risiko."""
        monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        monkeypatch.delenv("DASHBOARD_ALLOWED_HOSTS", raising=False)
        c = _client(ctx, client_ip="10.0.0.9")
        assert c.post("/api/control/close",
                      json={"trade_id": 1}).status_code == 403
        assert c.post("/api/control/pause",
                      json={"paused": False}).status_code == 403
        assert c.post("/api/params",
                      json={"risk_per_trade_pct": 100}).status_code == 403

    def test_ipv4_terpeta_di_ipv6_tetap_dianggap_lokal(self):
        assert srv._client_is_local("::ffff:127.0.0.1") is True
        assert srv._client_is_local("::1") is True
        assert srv._client_is_local("192.168.0.2") is False
        assert srv._client_is_local(None) is True


# ------------------------------------------------- 2/3. validasi header Host

class TestValidasiHost:

    def test_host_asing_ditolak_meski_dari_loopback(self, monkeypatch, ctx):
        """Inti serangan DNS rebinding: koneksi lokal, Host milik penyerang."""
        monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        monkeypatch.delenv("DASHBOARD_ALLOWED_HOSTS", raising=False)
        r = _client(ctx, host_header="penyerang.example.com").get("/api/health")
        assert r.status_code == 400
        assert "Host" in r.json()["error"]

    def test_host_dari_env_diterima(self, monkeypatch, ctx):
        monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        monkeypatch.setenv("DASHBOARD_ALLOWED_HOSTS",
                           "bot.internal, 192.168.1.10")
        r = _client(ctx, host_header="bot.internal").get("/api/health")
        assert r.status_code == 200

    def test_port_diabaikan_saat_membandingkan(self):
        allowed = srv._allowed_hosts("127.0.0.1")
        assert srv._host_header_ok("127.0.0.1:8000", allowed) is True
        assert srv._host_header_ok("[::1]:8000", allowed) is True
        assert srv._host_header_ok("jahat.example:8000", allowed) is False

    def test_host_kosong_tidak_memblokir(self):
        # HTTP/1.0 tanpa Host: tidak bisa dipakai rebinding lewat browser,
        # proteksi loopback/token tetap berlaku sesudahnya.
        assert srv._host_header_ok("", srv._allowed_hosts("127.0.0.1")) is True

    def test_wildcard_membuka_semua_host(self, monkeypatch):
        monkeypatch.setenv("DASHBOARD_ALLOWED_HOSTS", "*")
        allowed = srv._allowed_hosts("127.0.0.1")
        assert srv._host_header_ok("apa.saja.example", allowed) is True


# ----------------------------------------------------------- 4. token aktif

class TestDenganToken:

    def test_akses_jaringan_wajib_membawa_token(self, monkeypatch, ctx):
        monkeypatch.setenv("DASHBOARD_TOKEN", "rahasia-panjang")
        monkeypatch.setenv("DASHBOARD_ALLOWED_HOSTS", "bot.internal")
        c = _client(ctx, host_header="bot.internal", client_ip="192.168.1.50")
        assert c.get("/api/health").status_code == 401
        assert c.get("/api/health?token=salah").status_code == 401
        assert c.get("/api/health?token=rahasia-panjang").status_code == 200

    def test_token_tidak_menembus_validasi_host(self, monkeypatch, ctx):
        monkeypatch.setenv("DASHBOARD_TOKEN", "rahasia-panjang")
        monkeypatch.delenv("DASHBOARD_ALLOWED_HOSTS", raising=False)
        c = _client(ctx, host_header="penyerang.example.com")
        assert c.get("/api/health?token=rahasia-panjang").status_code == 400


# ------------------------------------------------- 5. penolakan bind terbuka

class TestBindTerbukaDitolak:

    def test_host_publik_tanpa_token_menolak_start(self, monkeypatch, ctx):
        monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        ctx.cfg.dashboard.host = "0.0.0.0"

        async def jalankan():
            # Batas waktu wajib: pada kode SEBELUM patch, run_dashboard
            # benar-benar menghidupkan uvicorn dan tidak pernah kembali.
            await asyncio.wait_for(srv.run_dashboard(ctx), timeout=3)

        with pytest.raises(RuntimeError) as err:
            asyncio.run(jalankan())
        assert "DASHBOARD_TOKEN" in str(err.value)

    def test_loopback_tetap_boleh_tanpa_token(self):
        assert srv.is_loopback_host("127.0.0.1") is True
        assert srv.is_loopback_host("localhost") is True
        assert srv.is_loopback_host("::1") is True
        # 0.0.0.0 mendengarkan SEMUA interface, jadi bukan loopback
        assert srv.is_loopback_host("0.0.0.0") is False
        assert srv.is_loopback_host("192.168.1.10") is False
