"""
Dashboard web real-time.

- GET  /                : halaman UI (static/index.html)
- WS   /ws              : push snapshot lengkap tiap detik (tanpa refresh manual)
- POST /api/control/... : pause/resume, tutup posisi manual
- GET/POST /api/params  : lihat / ubah parameter risiko TANPA restart bot

Snapshot berisi: saldo, posisi terbuka, histori, statistik, equity curve,
skor sinyal terbaru, dan status bot.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from bot.risk_management.stats import compute_stats, daily_pnl, max_drawdown
from bot.utils import now_ms

logger = logging.getLogger("pumpbot.dash")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


# ---------------------------------------------------------------------------
# Model body untuk endpoint POST (di level modul — lihat catatan di bawah)
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


# ---------------------------------------------------------------------------
# Builder snapshot (dipakai WS tiap detik)
# ---------------------------------------------------------------------------

def build_snapshot(ctx) -> dict:
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
    }


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_dashboard_app(ctx) -> FastAPI:
    app = FastAPI(title="PumpBot Dashboard", docs_url=None, redoc_url=None)
    clients: Set[WebSocket] = set()

    # ---------------- halaman utama ----------------
    @app.get("/")
    async def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    # ---------------- WebSocket broadcast ----------------
    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        clients.add(ws)
        try:
            # kirim snapshot pertama seketika
            await ws.send_text(json.dumps(build_snapshot(ctx), default=str))
            while True:
                await asyncio.sleep(1.0)
                payload = json.dumps(build_snapshot(ctx), default=str)
                await ws.send_text(payload)
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

    @app.on_event("shutdown")
    async def _shutdown():
        for ws in list(clients):
            try:
                await ws.close()
            except Exception:
                pass

    return app


async def run_dashboard(ctx) -> None:
    """Jalankan uvicorn di event loop yang sama dengan bot."""
    import uvicorn
    app = create_dashboard_app(ctx)
    config = uvicorn.Config(app, host=ctx.cfg.dashboard.host,
                            port=ctx.cfg.dashboard.port,
                            log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    ctx.uvicorn_server = server
    logger.info(f"Dashboard: http://{ctx.cfg.dashboard.host}:{ctx.cfg.dashboard.port}")
    await server.serve()
