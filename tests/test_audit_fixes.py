"""
Test regresi hasil audit 2026-09 (lihat AUDIT_REPORT).

Setiap test di sini menjaga perbaikan bug yang pernah ditemukan agar tidak
kambuh lagi:
  1. Galat float IEEE 754 pada SymbolFilters.round_qty / round_price
     (0.3 dengan step 0.1 pernah dibulatkan jadi 0.2).
  2. Posisi menggantung status OPEN setelah SL terisi lewat jalur
     re-place OCO (_reconcile_chunks_locked tidak memanggil _finalize).
  3. OCO chunk lain tidak dibatalkan di exchange saat SL salah satu chunk
     terisi (OCO yatim yang bisa terisi tanpa catatan).
  4. Duplikat open_time di SymbolBuffer (seed REST vs event close WS).
  5. Interval kline WS mengikuti config (bukan hardcode "1m").
  6. Parsing take_profit.targets yang tidak berbentuk map -> ConfigError.
  7. update_trade menolak nama kolom di luar whitelist.
  8. Autentikasi dashboard via DASHBOARD_TOKEN.
"""

from __future__ import annotations

import asyncio


import pytest

from bot.config import Config, ConfigError
from bot.data_collector.buffers import SymbolBuffer
from bot.models import Candle, ExitChunk, Position, SymbolFilters


# ===========================================================================
# 1. Galat float round_qty / round_price
# ===========================================================================

class TestPembulatanFloat:
    @pytest.mark.parametrize("qty,step", [
        (0.3, 0.1), (0.7, 0.1), (1.4, 0.1), (1.1, 0.1),
        (0.03, 0.01), (0.006, 0.001), (12.3, 0.1),
    ])
    def test_round_qty_tepat_untuk_kelipatan_sah(self, qty, step):
        sf = SymbolFilters(symbol="T", step_size=step)
        out = sf.round_qty(qty)
        assert out == pytest.approx(qty, abs=1e-12), (
            f"round_qty({qty}, step={step}) menghasilkan {out!r}")

    def test_round_qty_tetap_floor_untuk_nilai_non_kelipatan(self):
        sf = SymbolFilters(symbol="T", step_size=0.1)
        assert sf.round_qty(0.34) == pytest.approx(0.3, abs=1e-12)
        assert sf.round_qty(2.99) == pytest.approx(2.9, abs=1e-12)

    def test_round_qty_hasil_adalah_kelipatan_step_yang_bersih(self):
        """Hasil tidak boleh mengandung noise float (ditolak LOT_SIZE)."""
        sf = SymbolFilters(symbol="T", step_size=0.1)
        for qty in (0.7, 1.4, 2.3, 3.5):
            out = sf.round_qty(qty)
            steps = out / 0.1
            assert abs(steps - round(steps)) < 1e-9

    @pytest.mark.parametrize("price,tick", [
        (1.2, 0.0001), (0.0003, 0.00001), (2.3, 0.00001),
        (0.1, 0.0001), (2.0, 0.0001),
    ])
    def test_round_price_down_tidak_buangsatu_tick(self, price, tick):
        sf = SymbolFilters(symbol="T", tick_size=tick)
        assert sf.round_price(price, "down") == pytest.approx(price, abs=1e-12)

    def test_round_price_up_tidak_overshoot_satu_tick(self):
        sf = SymbolFilters(symbol="T", tick_size=0.0001)
        assert sf.round_price(2.0, "up") == pytest.approx(2.0, abs=1e-12)
        assert sf.round_price(1.99995, "up") == pytest.approx(2.0, abs=1e-12)


# ===========================================================================
# 2 & 3. SL terisi lewat jalur re-place OCO
# ===========================================================================

class _FakeDB:
    def __init__(self):
        self.events = []
        self.closed = None

    def record_event(self, *a, **k):
        self.events.append(a)

    def add_trade_event(self, *a, **k):
        self.events.append(a)

    def update_trade(self, tid, **f):
        pass

    def close_trade(self, tid, exit_time, exit_price, pnl, reason, qty_remaining=0.0):
        self.closed = (tid, reason, qty_remaining)


class _SlFilledGateway:
    """Gateway palsu: OCO chunk-1 sudah terisi SL, chunk-2 masih hidup."""

    mode = "sim"
    cancelled: list[int] = []

    async def get_oco_status(self, symbol, order_list_id):
        if order_list_id == 111:
            return {"list_status": "EXECUTED", "done": True,
                    "any_filled": True, "filled_qty": 5.0,
                    "filled_quote": 45.0, "avg_price": 9.0, "which": "sl"}
        return {"list_status": "EXECUTING", "done": False, "any_filled": False,
                "filled_qty": 0.0, "filled_quote": 0.0, "avg_price": 0.0,
                "which": ""}

    async def cancel_oco(self, symbol, order_list_id):
        self.cancelled.append(order_list_id)
        return True

    async def place_oco_sell(self, symbol, qty, tp_price, stop_price):
        return 999

    async def market_sell(self, symbol, qty):
        raise AssertionError("tidak boleh menjual lewat jalur ini di test")

    async def get_quote_balance(self):
        return (1000.0, 0.0)


def _posisi_dua_chunk() -> Position:
    return Position(
        trade_id=1, symbol="XYZUSDT", entry_time=0, entry_price=10.0,
        qty_total=10.0, qty_remaining=10.0, quote_value=100.0,
        stop_loss=9.5, initial_stop=9.5, take_profits=[11.0, 12.0],
        chunks=[ExitChunk(qty=5.0, tp_price=11.0, oco_list_id=111),
                ExitChunk(qty=5.0, tp_price=12.0, oco_list_id=112)],
        exit_mode="oco",
    )


def _executor_dengan_gateway(gw):
    from bot.execution.executor import Executor
    from bot.portfolio import Portfolio
    from bot.risk_management.manager import RiskManager

    cfg = Config()
    cfg.execution.exit_mode = "oco"
    ex = Executor(cfg, gw, _FakeDB(), Portfolio(cfg, gw), RiskManager(cfg))
    ex.filters["XYZUSDT"] = SymbolFilters(symbol="XYZUSDT")
    return ex


class TestSlTerisiSaatRePlaceOco:
    def test_posisi_tidak_menggantung_open(self):
        async def run():
            gw = _SlFilledGateway()
            ex = _executor_dengan_gateway(gw)
            pos = _posisi_dua_chunk()
            ex.positions[1] = pos
            await ex.place_exit_orders(pos, force=True)
            return pos

        pos = asyncio.run(run())
        assert pos.status == "CLOSED", (
            "posisi harus CLOSED setelah SL chunk terisi via re-place OCO")
        assert pos.exit_reason == "SL (OCO)"

    def test_oco_chunk_lain_dibatalkan_di_exchange(self):
        async def run():
            gw = _SlFilledGateway()
            ex = _executor_dengan_gateway(gw)
            pos = _posisi_dua_chunk()
            ex.positions[1] = pos
            await ex.reconcile_oco(pos)
            return gw

        gw = asyncio.run(run())
        assert 112 in gw.cancelled, (
            "OCO chunk lain (order list terpisah) wajib dibatalkan eksplisit")

    def test_reconcile_oco_menutup_posisi(self):
        async def run():
            gw = _SlFilledGateway()
            ex = _executor_dengan_gateway(gw)
            pos = _posisi_dua_chunk()
            ex.positions[1] = pos
            await ex.reconcile_oco(pos)
            return pos

        pos = asyncio.run(run())
        assert pos.status == "CLOSED"
        assert all(c.status != "PENDING" for c in pos.chunks)


# ===========================================================================
# 4. Duplikat open_time di buffer
# ===========================================================================

class TestBufferDedup:
    def _candle(self, ot, *, volume=50.0, closed=True, close=10.2):
        return Candle(open_time=ot, close_time=ot + 59_999, open=10, high=10.5,
                      low=9.9, close=close, volume=volume, quote_volume=510.0,
                      trades=25, taker_buy_volume=30.0, closed=closed)

    def test_candle_close_ws_mengganti_seed_rest(self):
        buf = SymbolBuffer("XYZ", max_candles=100)
        buf.on_candle(self._candle(1000))                 # seed REST (parsial)
        buf.on_candle(self._candle(1000, volume=80.0, closed=False))
        buf.on_candle(self._candle(1000, volume=80.0))    # close final dari WS
        ots = [c.open_time for c in buf.candles]
        assert len(ots) == len(set(ots)), "tidak boleh ada open_time ganda"
        assert buf.candles[-1].volume == 80.0, "data final yang harus menang"

    def test_replay_reconnect_dibuang(self):
        buf = SymbolBuffer("XYZ", max_candles=100)
        buf.on_candle(self._candle(1000))
        buf.on_candle(self._candle(2000))
        buf.on_candle(self._candle(1000, volume=99.0))    # replay lama
        assert [c.open_time for c in buf.candles] == [1000, 2000]
        assert buf.candles[0].volume == 50.0, "replay tidak boleh menimpa"


# ===========================================================================
# 5. Interval kline WS mengikuti config
# ===========================================================================

class TestIntervalKline:
    def test_collector_meneruskan_interval_config(self):
        from bot.data_collector.collector import DataCollector

        captured = {}

        class Gw:
            async def get_symbol_filters(self):
                return {}

            async def get_universe(self):
                return []

            async def get_klines(self, symbol, interval, limit):
                return []

            async def subscribe(self, symbols, on_candle, on_trade, on_book,
                                on_ticker, kline_interval="1m"):
                captured["kline_interval"] = kline_interval

            async def stop(self):
                pass

        cfg = Config()
        cfg.data.kline_interval = "5m"
        cfg.universe.include_symbols = ["BTCUSDT"]
        cfg.universe.refresh_minutes = 30
        coll = DataCollector(cfg, Gw())
        asyncio.run(coll.start())
        assert captured.get("kline_interval") == "5m", (
            "collector harus meneruskan data.kline_interval ke subscribe()")

    def test_binance_gateway_menyimpan_interval(self):
        from bot.exchange.binance_gateway import BinanceGateway
        gw = BinanceGateway(mode="paper", api_key="", api_secret="",
                            quote_asset="USDT")
        assert gw._kline_interval == "1m"

        async def run():
            await gw.subscribe([], lambda *a: None, lambda *a: None,
                               lambda *a: None, lambda *a: None,
                               kline_interval="3m")

        asyncio.run(run())
        assert gw._kline_interval == "3m"


# ===========================================================================
# 6. Parsing take_profit.targets yang salah bentuk
# ===========================================================================

class TestParsingTargets:
    def test_targets_bukan_daftar_ditolak(self, tmp_path):
        import yaml
        p = tmp_path / "config.yaml"
        base = yaml.safe_load(open("config/config.yaml", encoding="utf-8"))
        base["take_profit"]["targets"] = "odeh"
        p.write_text(yaml.safe_dump(base), encoding="utf-8")
        with pytest.raises(ConfigError):
            from bot.config import load_config
            load_config(str(p))

    def test_targets_item_bukan_map_ditolak(self, tmp_path):
        import yaml
        p = tmp_path / "config.yaml"
        base = yaml.safe_load(open("config/config.yaml", encoding="utf-8"))
        base["take_profit"]["mode"] = "multi"
        base["take_profit"]["targets"] = [2.0, 4.0]  # float, bukan map
        p.write_text(yaml.safe_dump(base), encoding="utf-8")
        with pytest.raises(ConfigError):
            from bot.config import load_config
            load_config(str(p))


# ===========================================================================
# 7. Whitelist kolom update_trade
# ===========================================================================

class TestUpdateTradeWhitelist:
    def test_kolom_tak_dikenal_ditolak(self):
        from bot.database.db import Database
        db = Database(":memory:")
        try:
            with pytest.raises(ValueError):
                db.update_trade(1, qty_remaining=1.0,
                                symbol="x; DROP TABLE trades--")
        finally:
            db.close()

    def test_kolom_sah_diterima(self):
        from bot.database.db import Database
        db = Database(":memory:")
        try:
            tid = db.open_trade(
                symbol="BTCUSDT", entry_time=1, entry_price=2.0, qty=3.0,
                quote_value=6.0, stop_loss=1.9, take_profits=[2.2],
                score=50.0, entry_reason="tes", exit_mode="oco")
            db.update_trade(tid, qty_remaining=2.5, stop_loss=1.95)
            rows = db.get_open_trades()
            assert rows[0]["qty_remaining"] == 2.5
        finally:
            db.close()


# ===========================================================================
# 8. Autentikasi dashboard DASHBOARD_TOKEN
# ===========================================================================

class TestDashboardToken:
    @staticmethod
    def _make_client(monkeypatch, token):
        """App FastAPI mini dengan middleware auth yang sama persis dengan
        yang dipasang create_dashboard_app (sumber: bot.dashboard.server)."""
        from bot.dashboard import server as srv
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient

        if token is None:
            monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
        else:
            monkeypatch.setenv("DASHBOARD_TOKEN", token)

        app = FastAPI()

        @app.get("/api/health")
        async def health():
            return {"ok": True}

        @app.middleware("http")
        async def _auth(request, call_next):
            if srv._dashboard_token():
                if not srv._token_valid(srv._request_token(request)):
                    return JSONResponse({"ok": False}, status_code=401)
                response = await call_next(request)
                if (request.query_params.get("token")
                        and not request.cookies.get(srv.TOKEN_COOKIE)):
                    response.set_cookie(srv.TOKEN_COOKIE, token,
                                        httponly=True, samesite="strict")
                return response
            return await call_next(request)

        return TestClient(app)

    def test_tanpa_token_endpoint_terbuka(self, monkeypatch):
        client = self._make_client(monkeypatch, None)
        assert client.get("/api/health").status_code == 200

    def test_dengan_token_query_param_lolos(self, monkeypatch):
        client = self._make_client(monkeypatch, "rahasia")
        assert client.get("/api/health?token=rahasia").status_code == 200
        # token salah dan tanpa token sama sekali ditolak (client baru agar
        # tidak membawa cookie dari request pertama)
        client2 = self._make_client(monkeypatch, "rahasia")
        assert client2.get("/api/health?token=salah").status_code == 401
        client3 = self._make_client(monkeypatch, "rahasia")
        assert client3.get("/api/health").status_code == 401

    def test_dengan_token_header_lolos(self, monkeypatch):
        client = self._make_client(monkeypatch, "rahasia")
        r = client.get("/api/health", headers={"X-Auth-Token": "rahasia"})
        assert r.status_code == 200

    def test_token_disimpan_sebagai_cookie(self, monkeypatch):
        client = self._make_client(monkeypatch, "rahasia")
        r = client.get("/api/health?token=rahasia")
        assert r.status_code == 200
        # permintaan berikutnya memakai cookie yang sudah tersimpan
        assert client.get("/api/health").status_code == 200
