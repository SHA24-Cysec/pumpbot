"""
Dashboard web real-time.

- GET  /                 : halaman UI (static/index.html)
- WS   /ws               : push snapshot lengkap tiap detik (tanpa refresh manual)
- POST /api/control/...  : pause/resume, tutup posisi manual
- GET/POST /api/params   : lihat / ubah parameter risiko TANPA restart bot
- POST /api/backtest/... : jalankan, batalkan, dan terapkan hasil grid search
- GET  /api/backtest/... : status job dan unduhan CSV hasil

KEAMANAN: dashboard mengontrol bot trading (tutup posisi, ubah risk %).
Tiga lapis proteksi:

1. DASHBOARD_TOKEN (opsional). Bila diisi, semua endpoint HTTP dan WebSocket
   mensyaratkan token tersebut (cookie, query ?token=, atau header
   X-Auth-Token).
2. TANPA token, dashboard hanya melayani klien LOOPBACK. Permintaan dari
   alamat lain ditolak 403, dan bot MENOLAK START bila dashboard.host bukan
   loopback sementara token kosong. Jadi tidak ada lagi kondisi "terbuka ke
   jaringan tanpa login".
3. Validasi header Host untuk mencegah DNS rebinding. Daftar host yang sah
   dibangun otomatis dari dashboard.host + loopback, dan bisa ditambah lewat
   environment DASHBOARD_ALLOWED_HOSTS (dipisah koma).

Snapshot berisi: saldo, posisi terbuka, histori, statistik, equity curve,
skor sinyal terbaru, status bot, dan ringkasan job backtest.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Optional, Set

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from bot.dashboard import config_writer
from bot.dashboard.backtest_runner import BacktestBusy, BacktestManager
from bot.risk_management.stats import compute_stats, daily_pnl, max_drawdown
from bot.utils import now_ms

logger = logging.getLogger("pumpbot.dash")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# Nama cookie sesi dashboard (dipakai bila DASHBOARD_TOKEN di-set).
TOKEN_COOKIE = "pumpbot_token"

# Backtest hanya boleh jalan saat bot di-pause: grid search memakan CPU dan
# tidak boleh berebut sumber daya dengan pengawasan posisi yang sedang hidup.
PESAN_WAJIB_PAUSE = ("Pause bot dulu sebelum menjalankan backtest. "
                     "Grid search memakai CPU penuh dan tidak boleh berebut "
                     "sumber daya dengan bot yang sedang trading.")


def _dashboard_token() -> str:
    """Token akses dashboard dari environment (kosong = tanpa autentikasi)."""
    return (os.getenv("DASHBOARD_TOKEN") or "").strip()


def _request_token(request: Request) -> str:
    """Token dari salah satu sumber: query param, header, atau cookie."""
    token = (request.query_params.get("token")
             or request.headers.get("x-auth-token") or "")
    if not token:
        token = request.cookies.get(TOKEN_COOKIE, "")
    return token


def _token_valid(token: str) -> bool:
    """Perbandingan konstan-waktu terhadap token yang diharapkan."""
    expected = _dashboard_token()
    if not expected:
        return True  # autentikasi tidak diaktifkan
    return hmac.compare_digest(token, expected)


# Nama host yang selalu dianggap lokal.
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]", "0.0.0.0"})


def is_loopback_host(host: str) -> bool:
    """True bila alamat bind/host termasuk loopback (bukan 0.0.0.0)."""
    return (host or "").strip().lower() in {"127.0.0.1", "localhost", "::1",
                                            "[::1]"}


def _client_is_local(client_host: Optional[str]) -> bool:
    """True bila koneksi datang dari mesin yang sama."""
    if not client_host:
        # Tidak ada info klien (mis. transport uji). Dianggap lokal agar
        # TestClient dan unix socket tetap bisa dipakai.
        return True
    host = client_host.strip().lower()
    if host.startswith("::ffff:"):      # IPv4 yang dipetakan ke IPv6
        host = host[7:]
    return host in {"127.0.0.1", "::1", "localhost"} or host.startswith("127.")


def _allowed_hosts(cfg_host: str) -> set:
    """
    Daftar nama host yang boleh muncul di header Host.

    Dibangun otomatis dari `dashboard.host` + loopback, ditambah isi
    environment DASHBOARD_ALLOWED_HOSTS (dipisah koma). Validasi ini
    mencegah DNS rebinding: situs jahat yang memetakan domainnya ke
    127.0.0.1 tidak bisa lagi memanggil endpoint kontrol, karena header
    Host yang dikirim browser adalah domain penyerang.
    """
    hosts = set(LOOPBACK_HOSTS)
    if cfg_host:
        hosts.add(cfg_host.strip().lower())
    extra = os.getenv("DASHBOARD_ALLOWED_HOSTS", "")
    for item in extra.split(","):
        item = item.strip().lower()
        if item:
            hosts.add(item)
    return hosts


def _host_header_ok(raw_host: str, allowed: set) -> bool:
    """Bandingkan header Host (tanpa port) dengan whitelist."""
    if not raw_host:
        # HTTP/1.0 tanpa Host. Tidak bisa dipakai serangan rebinding lewat
        # browser modern, jadi dibiarkan lolos ke lapis proteksi berikutnya.
        return True
    host = raw_host.strip().lower()
    if host.startswith("["):                 # IPv6 literal: [::1]:8000
        tutup = host.find("]")
        host = host[:tutup + 1] if tutup != -1 else host
    elif ":" in host:
        host = host.rsplit(":", 1)[0]
    if host in allowed:
        return True
    # Izinkan wildcard sederhana "*" untuk operator yang memang ingin terbuka
    return "*" in allowed


# ---------------------------------------------------------------------------
# Model body untuk endpoint POST (di level modul, lihat catatan di bawah)
# ---------------------------------------------------------------------------

class PauseBody(BaseModel):
    paused: bool


class CloseBody(BaseModel):
    trade_id: int
    fraction: float = 1.0  # 1.0 = tutup semua


class ParamsBody(BaseModel):
    risk_per_trade_pct: Optional[float] = None
    allow_multiple_positions: Optional[bool] = None
    max_open_positions: Optional[int] = None
    daily_loss_enabled: Optional[bool] = None
    daily_loss_limit_pct: Optional[float] = None
    score_threshold: Optional[float] = None
    trailing_enabled: Optional[bool] = None
    breakeven_enabled: Optional[bool] = None
    vwap_filter_enabled: Optional[bool] = None
    change24h_enabled: Optional[bool] = None


class BacktestBody(BaseModel):
    """Parameter satu job optimasi; validasi rinci ada di service.JobRequest."""
    symbols: Optional[str] = None
    top: Optional[int] = None
    days: Optional[int] = None
    interval: Optional[str] = None
    download: Optional[bool] = None
    oos: Optional[float] = None
    workers: Optional[int] = None
    min_trades: Optional[int] = None
    equity: Optional[float] = None
    risk_pct: Optional[float] = None
    w_pnl: Optional[float] = None
    w_pf: Optional[float] = None
    w_dd: Optional[float] = None
    top_rows: Optional[int] = None
    basis: Optional[str] = None
    sl: Optional[str] = None
    tp: Optional[str] = None
    be: Optional[str] = None
    be_buffer: Optional[str] = None
    trail: Optional[str] = None
    atr_sl: Optional[str] = None
    atr_tp: Optional[str] = None
    atr_be: Optional[str] = None
    atr_trail: Optional[str] = None
    atr_period: Optional[int] = None
    atr_method: Optional[str] = None
    cooldown: Optional[str] = None
    thr: Optional[str] = None
    wpa: Optional[str] = None
    ma_period: Optional[str] = None
    spike_scale: Optional[str] = None
    structure: Optional[str] = None
    breakout: Optional[str] = None
    swing: Optional[str] = None
    min_candles: Optional[str] = None
    vwap: Optional[str] = None
    change24h: Optional[str] = None


class ApplyBody(BaseModel):
    """Permintaan menulis hasil backtest ke config.yaml."""
    rank: int = 1
    groups: list[str] = ["exit"]
    dry_run: bool = False


# ---------------------------------------------------------------------------
# Builder snapshot (dipakai WS tiap detik)
# ---------------------------------------------------------------------------

def build_snapshot(ctx, backtest=None) -> dict:
    """Kumpulkan seluruh state bot jadi satu payload ringan untuk frontend."""
    db = ctx.db
    executor = ctx.executor
    positions = list(executor.positions.values())

    # ---- saldo & equity ----
    positions_value = 0.0
    pos_rows = []
    for pos in positions:
        price = getattr(pos, "_last_price", 0.0) or pos.entry_price
        positions_value += pos.qty_remaining * price
        pos_rows.append({
            "trade_id": pos.trade_id,
            "symbol": pos.symbol,
            "entry_time": pos.entry_time,
            "entry_price": pos.entry_price,
            "price": price,
            "qty_total": pos.qty_total,
            "qty_remaining": pos.qty_remaining,
            "value": pos.qty_remaining * price,
            "stop_loss": pos.stop_loss,
            "take_profits": pos.take_profits,
            # ATR yang dibekukan saat entry; 0 = basis ATR tidak tersedia
            # untuk posisi ini (trailing/BE-nya memakai basis persen/R).
            "atr_entry": float(getattr(pos, "atr_entry", 0.0) or 0.0),
            "be_triggered": pos.be_triggered,
            "trail_active": pos.trail_active,
            "highest_price": pos.highest_price,
            "score": pos.score,
            "realized_pnl": round(pos.realized_pnl, 4),
            "unrealized_pnl": round(pos.unrealized_pnl(price), 4),
            "unrealized_pnl_pct": round(pos.unrealized_pnl_pct(price), 3),
            "exit_mode": pos.exit_mode + (" (fallback manual)" if pos.oco_fallback else ""),
            "oco_failure_code": getattr(pos, "oco_failure_code", ""),
            "oco_failure_detail": getattr(pos, "oco_failure_detail", ""),
        })

    equity = ctx.portfolio.quote_free + ctx.portfolio.quote_locked + positions_value

    # ---- statistik & histori ----
    history = db.get_trade_history(200)
    all_closed = db.get_trade_history(100000)
    stats = compute_stats(all_closed)
    curve = db.get_equity_curve(800)
    mdd = max_drawdown([p["equity"] for p in curve]) if curve else {
        "max_dd_pct": 0.0, "peak": equity, "trough": equity}
    daily = daily_pnl(equity, ctx.risk.equity_start_of_day or equity)

    # ---- sinyal ----
    signals = ctx.engine.top_signals(40)
    events = db.get_recent_events(50)

    return {
        "type": "snapshot",
        "ts": now_ms(),
        "mode": ctx.mode.upper(),
        "paused": ctx.paused,
        "uptime_sec": int((now_ms() - ctx.started_at) / 1000),
        "watchlist": ctx.collector.watchlist,
        "ws_alive": ctx.collector.ws_alive,
        "balance": {
            "quote_asset": ctx.cfg.quote_asset,
            "free": round(ctx.portfolio.quote_free, 2),
            "locked": round(ctx.portfolio.quote_locked, 2),
            "in_positions": round(positions_value, 2),
            "equity": round(equity, 2),
            "start_equity": round(ctx.portfolio.start_equity or equity, 2),
            "total_return_pct": round(
                (equity / ctx.portfolio.start_equity - 1) * 100, 3
            ) if ctx.portfolio.start_equity else 0.0,
        },
        "idr": {
            "rate": round(getattr(ctx, "idr_rate", 0.0) or 0.0, 2),
            "symbol": getattr(ctx, "idr_rate_symbol", ""),
            "source": getattr(ctx, "idr_rate_source", ""),
            "updated_at": getattr(ctx, "idr_rate_updated_at", 0),
        },
        "daily": {
            "pnl": daily["pnl"], "pnl_pct": daily["pnl_pct"],
            "enabled": ctx.risk.params.daily_loss_enabled,
            "loss_limit_pct": ctx.risk.params.daily_loss_limit_pct,
            "reset_timezone": "UTC",
            "halted": bool(ctx.risk.daily_halt_reason),
            "halt_reason": ctx.risk.daily_halt_reason,
        },
        "positions": pos_rows,
        "history": history,
        "stats": {**stats, "max_dd_pct": mdd["max_dd_pct"]},
        "equity_curve": [[p["ts"], round(p["equity"], 2)] for p in curve],
        "signals": signals,
        "events": events,
        "backtest": (backtest.summary() if backtest else
                     {"status": "idle", "running": False, "pct": 0.0,
                      "phase": "", "job_id": ""}),
    }


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_dashboard_app(ctx) -> FastAPI:
    clients: Set[WebSocket] = set()
    backtest = BacktestManager()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        """Bersih-bersih saat dashboard berhenti.

        Memakai lifespan, bukan @app.on_event("shutdown") yang sudah
        deprecated sejak FastAPI 0.93. Job backtest dimatikan lebih dulu
        supaya tidak ada proses anak yang tertinggal hidup.
        """
        yield
        await backtest.shutdown()
        for ws in list(clients):
            try:
                await ws.close()
            except Exception:
                pass

    app = FastAPI(title="PumpBot Dashboard", docs_url=None, redoc_url=None,
                  lifespan=lifespan)
    app.state.backtest = backtest
    # Bot memegang referensi ini supaya jalur shutdown-nya bisa mematikan job
    # backtest lebih dulu. Proses anak dijalankan di sesi terpisah sehingga
    # TIDAK ikut mati kalau hanya mengandalkan sinyal ke grup proses bot.
    ctx.backtest = backtest

    # ---------------- keamanan: Host, loopback, token ----------------
    allowed_hosts = _allowed_hosts(getattr(ctx.cfg.dashboard, "host", ""))

    @app.middleware("http")
    async def _auth_middleware(request: Request, call_next):
        # 1. Validasi header Host (anti DNS rebinding). Dilakukan lebih dulu
        #    karena serangan ini justru menargetkan dashboard di loopback.
        if not _host_header_ok(request.headers.get("host", ""), allowed_hosts):
            logger.warning("Permintaan dashboard ditolak: header Host '%s' "
                           "tidak dikenal (klien %s).",
                           request.headers.get("host", ""),
                           request.client.host if request.client else "?")
            return JSONResponse(
                {"ok": False,
                 "error": "Akses ditolak: header Host tidak dikenal. "
                          "Tambahkan ke DASHBOARD_ALLOWED_HOSTS bila memang "
                          "host yang sah."},
                status_code=400)

        # 2. Tanpa DASHBOARD_TOKEN, dashboard HANYA melayani klien lokal.
        if not _dashboard_token():
            if not _client_is_local(request.client.host if request.client
                                    else None):
                logger.warning("Permintaan dashboard dari %s ditolak: "
                               "DASHBOARD_TOKEN belum diisi.",
                               request.client.host if request.client else "?")
                return JSONResponse(
                    {"ok": False,
                     "error": "Akses ditolak: dashboard tanpa DASHBOARD_TOKEN "
                              "hanya bisa diakses dari mesin yang sama."},
                    status_code=403)
            return await call_next(request)

        # 3. Token aktif: semua endpoint wajib menyertakannya.
        token = _request_token(request)
        if not _token_valid(token):
            return JSONResponse(
                {"ok": False,
                 "error": "Akses ditolak: DASHBOARD_TOKEN aktif, "
                          "kirim ?token=... / header X-Auth-Token."},
                status_code=401)
        # token valid via query param -> simpan sebagai cookie agar
        # fetch() dan WebSocket berikutnya tidak perlu menyertakan token
        response = await call_next(request)
        if request.query_params.get("token") and not request.cookies.get(TOKEN_COOKIE):
            response.set_cookie(TOKEN_COOKIE, token, httponly=True,
                                samesite="strict")
        return response

    # ---------------- cache snapshot WebSocket ----------------
    # build_snapshot membaca histori + statistik dari SQLite; tanpa cache,
    # tiap klien WS memicu query penuh (termasuk seluruh trade closed) tiap
    # detik. Cache 1 detik membuat semua klien berbagi payload yang sama.
    _snap_cache: dict = {"ts": 0, "payload": None}

    def _snapshot_payload() -> str:
        ts = now_ms()
        cached = _snap_cache["payload"]
        if cached is None or ts - _snap_cache["ts"] >= 900:
            _snap_cache["payload"] = json.dumps(
                build_snapshot(ctx, backtest), default=str)
            _snap_cache["ts"] = ts
            cached = _snap_cache["payload"]
        return cached

    # ---------------- halaman utama ----------------
    @app.get("/")
    async def index():
        # no-cache memaksa browser memvalidasi ulang ke server setiap kali
        # halaman dibuka. Tanpa header ini browser boleh menebak sendiri
        # berapa lama file dianggap segar (heuristic caching), sehingga
        # index.html versi lama bisa terus tampil setelah update walaupun
        # file di disk sudah baru. FileResponse tetap mengirim ETag dan
        # Last-Modified, jadi kalau file tidak berubah balasannya cuma 304
        # dan tidak ada biaya transfer tambahan.
        return FileResponse(
            os.path.join(STATIC_DIR, "index.html"),
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )

    # ---------------- WebSocket broadcast ----------------
    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        # Middleware HTTP TIDAK berlaku untuk handshake WebSocket, jadi
        # ketiga lapis proteksi diulang di sini.
        if not _host_header_ok(ws.headers.get("host", ""), allowed_hosts):
            await ws.close(code=4403)
            return
        if not _dashboard_token():
            if not _client_is_local(ws.client.host if ws.client else None):
                await ws.close(code=4403)
                return
        else:
            # proteksi token untuk WS: cookie (dikirim browser saat handshake
            # same-origin) atau query param ?token=
            token = ws.query_params.get("token") or ws.cookies.get(TOKEN_COOKIE, "")
            if not _token_valid(token):
                await ws.close(code=4401)
                return
        await ws.accept()
        clients.add(ws)
        try:
            # kirim snapshot pertama seketika
            await ws.send_text(_snapshot_payload())
            while True:
                await asyncio.sleep(1.0)
                await ws.send_text(_snapshot_payload())
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            logger.debug(f"WS client error: {exc}")
        finally:
            clients.discard(ws)

    # ---------------- kontrol bot ----------------
    # CATATAN: model body HARUS di level modul (bukan lokal dalam fungsi).
    # file ini memakai `from __future__ import annotations` (PEP 563) sehingga
    # anotasi berupa string; FastAPI me-resolve-nya lewat globals modul dan
    # tidak bisa melihat kelas lokal -> parameter salah dianggap query param.
    @app.post("/api/control/pause")
    async def set_pause(body: PauseBody):
        ctx.set_paused(body.paused)
        return {"ok": True, "paused": body.paused}

    @app.post("/api/control/close")
    async def close_position(body: CloseBody):
        pos = ctx.executor.positions.get(body.trade_id)
        if not pos:
            return JSONResponse({"ok": False, "error": "posisi tidak ditemukan"},
                                status_code=404)
        ok = await ctx.executor.close_position(pos, "MANUAL (dashboard)",
                                               fraction=body.fraction)
        return {"ok": ok}

    # ---------------- parameter risiko live ----------------
    @app.get("/api/params")
    async def get_params():
        return ctx.risk.params.to_dict()

    @app.post("/api/params")
    async def set_params(body: ParamsBody):
        updates = {k: v for k, v in body.model_dump().items() if v is not None}
        if not updates:
            return {"ok": False, "errors": ["tidak ada perubahan"]}
        errors = ctx.risk.update_params(updates)
        # threshold sinyal ikut parameter runtime
        ctx.engine._override_threshold = ctx.risk.params.score_threshold
        ctx.apply_runtime_flags()
        return {"ok": not errors, "errors": errors,
                "params": ctx.risk.params.to_dict()}

    @app.get("/api/trades")
    async def trades(limit: int = 500):
        return ctx.db.get_trade_history(limit)

    @app.get("/api/health")
    async def health():
        return {"ok": True, "mode": ctx.mode, "paused": ctx.paused,
                "open_positions": len(ctx.executor.positions)}

    # ---------------- backtest & optimasi ----------------
    @app.get("/api/backtest/defaults")
    async def backtest_defaults():
        """Nilai awal formulir, diambil dari config yang sedang dipakai."""
        cfg = ctx.cfg
        weights = dict(cfg.signal.weights or {})
        w_pa = float(weights.get("price_action", 0.0))
        w_vol = float(weights.get("volume", 0.0))
        rasio = (w_pa / (w_pa + w_vol)) if (w_pa + w_vol) > 0 else 0.6
        return {
            "quote_asset": cfg.quote_asset,
            "cpu_count": os.cpu_count() or 1,
            "fee_pct": cfg.risk.fee_pct,
            "risk_per_trade_pct": cfg.risk.risk_per_trade_pct,
            "min_stop_pct": cfg.stops.min_stop_pct,
            "max_stop_pct": cfg.stops.max_stop_pct,
            "score_threshold": cfg.signal.score_threshold,
            "w_pa_ratio": round(rasio, 3),
            "ma_period": cfg.signal.volume.ma_period,
            "spike_scale": cfg.signal.volume.spike_scale,
            "structure_candles": cfg.signal.price_action.structure_candles,
            "breakout_lookback": cfg.signal.price_action.breakout_lookback,
            "swing_neighbors": cfg.signal.price_action.swing_neighbors,
            "min_candles": cfg.signal.min_candles,
            "cooldown_after_exit_min": cfg.signal.cooldown_after_exit_min,
            "vwap_enabled": bool(cfg.signal.vwap.enabled),
            # Gate band perubahan 24 jam (nilai absolut); dikirim supaya
            # formulir backtest menampilkan band yang sedang dipakai.
            "change24h_enabled": bool(cfg.signal.change_24h.enabled),
            "change24h_min_pct": cfg.signal.change_24h.min_pct,
            "change24h_max_pct": cfg.signal.change_24h.max_pct,
            "atr_period": cfg.atr.period,
            "atr_method": cfg.atr.method,
            "atr_sl": cfg.stops.atr_multiplier,
            "atr_tp": cfg.take_profit.atr_multiplier,
            "atr_be": cfg.breakeven.trigger_atr_mult,
            "atr_trail": cfg.trailing.atr_multiplier,
            "atr_min_multiplier": cfg.stops.atr_min_multiplier,
            "atr_max_multiplier": cfg.stops.atr_max_multiplier,
            "current_exit": {
                "stops_mode": cfg.stops.mode,
                "sl_pct": cfg.stops.percent_pct,
                "sl_atr_mult": cfg.stops.atr_multiplier,
                "tp_mode": cfg.take_profit.mode,
                "tp_rr": getattr(cfg.take_profit, "rr", None),
                "tp_atr_mult": cfg.take_profit.atr_multiplier,
                "be_enabled": cfg.breakeven.enabled,
                "be_trigger_mode": cfg.breakeven.trigger_mode,
                "be_trigger_rr": cfg.breakeven.trigger_rr,
                "be_trigger_atr_mult": cfg.breakeven.trigger_atr_mult,
                "be_buffer_pct": cfg.breakeven.buffer_pct,
                "trail_enabled": cfg.trailing.enabled,
                "trail_mode": cfg.trailing.mode,
                "trail_pct": cfg.trailing.percent_pct,
                "trail_atr_mult": cfg.trailing.atr_multiplier,
                "trail_step_pct": cfg.trailing.update_step_pct,
                "atr_period": cfg.atr.period,
                "atr_method": cfg.atr.method,
            },
        }

    @app.get("/api/backtest/state")
    async def backtest_state(since: int = 0):
        return backtest.state(since=max(0, since))

    @app.post("/api/backtest/start")
    async def backtest_start(body: BacktestBody):
        if not ctx.paused:
            return JSONResponse(
                {"ok": False, "error": PESAN_WAJIB_PAUSE, "need_pause": True},
                status_code=409)
        payload = {k: v for k, v in body.model_dump().items() if v is not None}
        payload.setdefault("config", ctx.cfg_path)
        try:
            job_id = await backtest.start(payload)
        except BacktestBusy as exc:
            return JSONResponse({"ok": False, "error": str(exc)},
                                status_code=409)
        except OSError as exc:
            return JSONResponse({"ok": False, "error": str(exc)},
                                status_code=500)
        ctx.db.record_event("INFO", "BACKTEST",
                            f"job optimasi {job_id} dimulai dari dashboard")
        return {"ok": True, "job_id": job_id}

    @app.post("/api/backtest/cancel")
    async def backtest_cancel():
        ok = await backtest.cancel()
        return {"ok": ok, "status": backtest.status}

    @app.get("/api/backtest/results.csv")
    async def backtest_csv():
        path = backtest.csv_path
        if not path or not os.path.exists(path):
            return JSONResponse({"ok": False, "error": "CSV belum tersedia"},
                                status_code=404)
        return FileResponse(path, media_type="text/csv",
                            filename=os.path.basename(path))

    @app.post("/api/backtest/apply")
    async def backtest_apply(body: ApplyBody):
        if not ctx.paused:
            return JSONResponse(
                {"ok": False, "error": PESAN_WAJIB_PAUSE, "need_pause": True},
                status_code=409)
        if backtest.status != "done" or not backtest.rows:
            return JSONResponse(
                {"ok": False, "error": "belum ada hasil backtest yang selesai"},
                status_code=409)

        baris = next((r for r in backtest.rows
                      if int(r.get("rank", 0)) == body.rank), None)
        if baris is None:
            return JSONResponse(
                {"ok": False, "error": f"baris peringkat {body.rank} "
                                       f"tidak ada di hasil"},
                status_code=404)

        sah = {"exit", "lookback", "scoring", "cooldown"}
        diminta = [g for g in body.groups if g in sah]
        if not diminta:
            return JSONResponse(
                {"ok": False, "error": "pilih minimal satu kelompok "
                                       "parameter untuk diterapkan"},
                status_code=400)

        updates: dict[str, Any] = {}
        for g in diminta:
            updates.update(baris.get("apply", {}).get(g, {}) or {})
        if not updates:
            return JSONResponse(
                {"ok": False, "error": "kelompok yang dipilih tidak "
                                       "menghasilkan perubahan apa pun"},
                status_code=400)

        try:
            hasil = config_writer.apply_updates(ctx.cfg_path, updates,
                                                dry_run=body.dry_run)
        except config_writer.ConfigWriteError as exc:
            logger.warning("Penulisan config ditolak: %s", exc)
            return JSONResponse({"ok": False, "error": str(exc)},
                                status_code=400)

        if not body.dry_run:
            berubah = [c["key"] for c in hasil["changes"] if c["changed"]]
            ctx.db.record_event(
                "INFO", "BACKTEST",
                f"config.yaml diperbarui dari hasil backtest peringkat "
                f"{body.rank}: {len(berubah)} nilai berubah")
            logger.info("config.yaml diperbarui dari backtest "
                        "(peringkat %s), cadangan: %s",
                        body.rank, hasil.get("backup"))
        return {"ok": True, **hasil,
                "restart_required": not body.dry_run,
                "note": ("Perubahan tersimpan di config.yaml. Bot membaca "
                         "config saat start, jadi jalankan ulang bot agar "
                         "parameter baru dipakai.")}

    return app


async def run_dashboard(ctx) -> None:
    """Jalankan uvicorn di event loop yang sama dengan bot."""
    import uvicorn
    app = create_dashboard_app(ctx)
    host = ctx.cfg.dashboard.host
    port = ctx.cfg.dashboard.port
    # Peringatan keras: dashboard punya kontrol penuh atas bot trading tanpa
    # login bawaan. Bind ke interface selain loopback tanpa token = siapa pun
    # di jaringan itu bisa menutup posisi / mengubah risk % / menjalankan bot.
    loopback = is_loopback_host(host)
    if not loopback and not _dashboard_token():
        # Dulu ini hanya peringatan, dan bot tetap jalan dengan dashboard
        # terbuka ke seluruh jaringan tanpa login sama sekali. Sekarang
        # dianggap kesalahan konfigurasi yang fatal.
        raise RuntimeError(
            f"KEAMANAN: dashboard.host='{host}' bukan loopback sementara "
            f"DASHBOARD_TOKEN kosong. Siapa pun di jaringan itu dapat "
            f"menutup posisi, mengubah risk %, dan menjalankan bot. "
            f"Isi DASHBOARD_TOKEN di .env, atau ubah dashboard.host menjadi "
            f"127.0.0.1 (akses jarak jauh sebaiknya lewat SSH tunnel)."
        )
    config = uvicorn.Config(app, host=host, port=port,
                            log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    ctx.uvicorn_server = server
    if _dashboard_token():
        logger.info("Dashboard: http://%s:%s (autentikasi DASHBOARD_TOKEN aktif)",
                    host, port)
    else:
        logger.info(f"Dashboard: http://{host}:{port}")
    await server.serve()
