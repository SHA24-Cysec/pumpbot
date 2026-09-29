"""
Regresi untuk dua migrasi lanjutan hasil audit:

  F-13  OCO dikirim lewat endpoint baru POST /api/v3/orderList/oco
        (endpoint lama POST /api/v3/order/oco deprecated sejak 2024-04-02),
        dengan fallback aman ke endpoint lama.
  F-14  Deteksi fill memakai User Data Stream sebagai pemicu rekonsiliasi
        seketika, bukan hanya polling REST tiap `reconcile_sec`.

Semua memakai stub; tidak ada koneksi jaringan, tidak ada API key, tidak ada
order sungguhan.
"""

from __future__ import annotations

import asyncio
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import load_config                   # noqa: E402
from bot.execution.executor import Executor          # noqa: E402
from bot.models import ExitChunk, Position, SymbolFilters  # noqa: E402
from bot.portfolio import Portfolio                  # noqa: E402
from bot.position_manager import PositionManager     # noqa: E402
from bot.risk_management.manager import RiskManager  # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CFG = os.path.join(_ROOT, "config", "config.yaml")

SYM = "TESTUSDT"


class _FakeDB:
    def __init__(self):
        self.events = []

    def record_event(self, level, type_, message, symbol=None):
        self.events.append((level, type_, message, symbol))

    def add_trade_event(self, *a, **k):
        pass

    def update_trade(self, *a, **k):
        pass

    def close_trade(self, *a, **k):
        pass


def _gateway_live(**kw):
    pytest.importorskip("binance_common")
    pytest.importorskip("binance_sdk_spot")
    from bot.exchange.binance_gateway import BinanceGateway
    return BinanceGateway(mode="live", api_key="x" * 8, api_secret="y" * 8,
                          quote_asset="USDT", **kw)


# =========================================================== F-13 endpoint OCO

class TestEndpointOcoBaru:

    def _rekam(self, gw, hasil=None, error=None):
        """Ganti _rest dengan perekam yang mengembalikan hasil / melempar error."""
        rekam = {}

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            if "orig_client_order_id" in kwargs:
                rekam["query"] = kwargs
                return {"orderListId": 4242}
            rekam["fn"] = getattr(fn, "__name__", str(fn))
            rekam["kwargs"] = kwargs
            rekam["retry_transient"] = retry_transient
            rekam.setdefault("n", 0)
            rekam["n"] += 1
            if error is not None and rekam["n"] == 1:
                raise error
            return hasil if hasil is not None else {"orderListId": 777}

        gw._rest = fake_rest
        return rekam

    def test_memakai_order_list_oco_secara_default(self):
        gw = _gateway_live()
        rekam = self._rekam(gw)
        list_id = asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))

        assert list_id == 777
        assert rekam["fn"] == "order_list_oco"
        kw = rekam["kwargs"]
        # leg atas = TP LIMIT_MAKER, leg bawah = SL STOP_LOSS_LIMIT
        assert kw["above_type"].value == "LIMIT_MAKER"
        assert kw["below_type"].value == "STOP_LOSS_LIMIT"
        assert kw["side"].value == "SELL"
        assert float(kw["above_price"]) == pytest.approx(1.2)
        assert float(kw["below_stop_price"]) == pytest.approx(0.98)
        # harga stop-limit harus DI BAWAH trigger agar tetap terisi saat jatuh
        assert float(kw["below_price"]) < float(kw["below_stop_price"])
        assert kw["below_time_in_force"].value == "GTC"
        assert kw["list_client_order_id"].startswith("pb-oco")
        # pemasangan order tidak boleh di-retry buta
        assert rekam["retry_transient"] is False

    def test_flag_config_memaksa_endpoint_lama(self):
        gw = _gateway_live(oco_legacy_endpoint=True)
        rekam = self._rekam(gw)
        asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))

        assert rekam["fn"] == "order_oco"
        kw = rekam["kwargs"]
        assert float(kw["price"]) == pytest.approx(1.2)
        assert float(kw["stop_price"]) == pytest.approx(0.98)
        assert float(kw["stop_limit_price"]) < float(kw["stop_price"])

    def test_fallback_ke_endpoint_lama_saat_parameter_tidak_dikenal(self):
        from binance_common.errors import BadRequestError
        gw = _gateway_live()
        urutan = []

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            nama = getattr(fn, "__name__", str(fn))
            urutan.append(nama)
            if nama == "order_list_oco":
                raise BadRequestError(
                    error_message="Unknown parameter 'aboveType'.",
                    status_code=-1100)
            return {"orderListId": 31337}

        gw._rest = fake_rest
        list_id = asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))

        assert urutan == ["order_list_oco", "order_oco"]
        assert list_id == 31337
        # sesudah fallback, panggilan berikutnya langsung ke endpoint lama
        urutan.clear()
        asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))
        assert urutan == ["order_oco"]

    def test_penolakan_order_tidak_memicu_fallback(self):
        from binance_common.errors import BadRequestError
        gw = _gateway_live()
        urutan = []

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            urutan.append(getattr(fn, "__name__", str(fn)))
            raise BadRequestError(
                error_message="Account has insufficient balance for "
                              "requested action.", status_code=-2010)

        gw._rest = fake_rest
        with pytest.raises(BadRequestError):
            asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))
        assert urutan == ["order_list_oco"]

    def test_rekonsiliasi_tetap_jalan_di_endpoint_baru(self):
        from binance_common.errors import ServerError
        gw = _gateway_live()
        rekam = self._rekam(
            gw, error=ServerError(error_message="Server error: 502",
                                  status_code=502))
        list_id = asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))

        assert rekam["n"] == 1                     # tidak ada OCO ganda
        assert list_id == 4242                     # hasil query clientOrderId
        assert rekam["query"]["orig_client_order_id"].startswith("pb-oco")


# ==================================================== F-14 user data stream

class TestPemicuUserDataStream:

    def _executor(self):
        cfg = load_config(_CFG)
        gw = types.SimpleNamespace()
        return Executor(cfg, gw, _FakeDB(), Portfolio(cfg, gw),
                        RiskManager(cfg))

    def test_execution_report_sell_menandai_simbol(self):
        ex = self._executor()
        ex.note_user_event({"e": "executionReport", "s": SYM, "S": "SELL",
                            "X": "FILLED"})
        assert ex.urgent_reconcile == {SYM}
        assert ex.take_urgent_symbols() == {SYM}
        assert ex.take_urgent_symbols() == set()   # sekali pakai

    def test_partially_filled_juga_ditandai(self):
        ex = self._executor()
        ex.note_user_event({"e": "executionReport", "s": SYM, "S": "SELL",
                            "X": "PARTIALLY_FILLED"})
        assert ex.urgent_reconcile == {SYM}

    def test_event_tidak_relevan_diabaikan(self):
        ex = self._executor()
        ex.note_user_event({"e": "executionReport", "s": SYM, "S": "BUY",
                            "X": "FILLED"})
        ex.note_user_event({"e": "executionReport", "s": SYM, "S": "SELL",
                            "X": "NEW"})
        ex.note_user_event({"e": "outboundAccountPosition", "B": []})
        ex.note_user_event({"e": "balanceUpdate", "s": SYM})
        ex.note_user_event("bukan dict")
        assert ex.urgent_reconcile == set()

    def test_list_status_menandai_simbol(self):
        ex = self._executor()
        ex.note_user_event({"e": "listStatus", "s": SYM, "g": 12,
                            "l": "ALL_DONE"})
        assert ex.urgent_reconcile == {SYM}

    def test_callback_tidak_pernah_melempar(self):
        ex = self._executor()

        class _Jahat(dict):
            def get(self, *a, **k):
                raise RuntimeError("payload rusak")

        ex.note_user_event(_Jahat())   # tidak boleh mematikan callback SDK

    def test_gateway_membuka_bungkus_subscription_id(self):
        gw = _gateway_live()
        diterima = []
        gw._uds_cb = diterima.append
        gw._on_user_data_event({"subscriptionId": 9,
                                "event": {"e": "executionReport", "s": SYM,
                                          "S": "SELL", "X": "FILLED"}})
        gw._on_user_data_event({"e": "listStatus", "s": SYM})
        assert diterima[0]["e"] == "executionReport"
        assert diterima[1]["e"] == "listStatus"
        assert gw.user_stream_last_event_ts > 0

    def test_mode_paper_tidak_membuka_user_stream(self):
        from bot.exchange.binance_gateway import BinanceGateway
        gw = BinanceGateway(mode="paper", api_key="", api_secret="")
        assert asyncio.run(gw.start_user_data_stream(lambda e: None)) is False
        assert gw.user_stream_active is False

    def test_langganan_live_memasang_handler_dan_meneruskan_event(self):
        gw = _gateway_live()
        diterima = []

        class _Handle:
            def __init__(self):
                self.cb = None
                self.unsubscribed = False

            def on(self, event, callback):
                assert event == "message"
                self.cb = callback

            async def unsubscribe(self):
                self.unsubscribed = True

        handle = _Handle()

        class _WsApi:
            async def create_connection(self):
                return object()

            async def user_data_stream_subscribe_signature(self):
                return types.SimpleNamespace(stream=handle)

            async def close_connection(self):
                return None

        gw._client = types.SimpleNamespace(websocket_api=_WsApi())
        assert asyncio.run(gw.start_user_data_stream(diterima.append)) is True
        assert gw.user_stream_active is True

        handle.cb({"e": "executionReport", "s": SYM, "S": "SELL",
                   "X": "FILLED"})
        assert diterima and diterima[0]["s"] == SYM

        asyncio.run(gw.stop_user_data_stream())
        assert handle.unsubscribed is True
        assert gw.user_stream_active is False

    def test_kegagalan_langganan_tidak_mematikan_bot(self):
        gw = _gateway_live()

        class _WsApiRusak:
            async def create_connection(self):
                raise ConnectionError("ws api tidak bisa dihubungi")

            async def user_data_stream_subscribe_signature(self):
                raise AssertionError("tidak boleh sampai ke sini")

        gw._client = types.SimpleNamespace(websocket_api=_WsApiRusak())
        assert asyncio.run(gw.start_user_data_stream(lambda e: None)) is False
        assert gw.user_stream_active is False

    def test_position_manager_melewati_throttle_untuk_simbol_urgen(self):
        cfg = load_config(_CFG)
        dipanggil = []

        pos = Position(
            trade_id=1, symbol=SYM, entry_time=0, entry_price=1.0,
            qty_total=10.0, qty_remaining=10.0, quote_value=10.0,
            stop_loss=0.9, initial_stop=0.9, take_profits=[1.2],
            chunks=[ExitChunk(qty=10.0, tp_price=1.2, oco_list_id=5)],
            highest_price=1.0, score=80, entry_reason="test", exit_mode="oco",
        )

        class _Ex:
            positions = {1: pos}
            last_prices: dict = {}
            filters = {SYM: SymbolFilters(symbol=SYM, tick_size=0.0001,
                                          step_size=0.001, min_qty=0.001,
                                          min_notional=5.0)}
            db = _FakeDB()
            urgent_reconcile = {SYM}

            def take_urgent_symbols(self):
                s, self.urgent_reconcile = self.urgent_reconcile, set()
                return s

            async def reconcile_oco(self, p):
                dipanggil.append(p.symbol)

            async def sync_exit_orders(self, p):
                pass

            async def close_position(self, p, reason):
                pass

        class _Col:
            def last_price(self, symbol):
                return 1.0

            def data_is_stale(self, max_age):
                return False

        pm = PositionManager(cfg, _Ex(), _Col())
        pm._last_reconcile = 10 ** 12      # throttle jauh dari jatuh tempo

        async def satu_siklus():
            task = asyncio.create_task(pm._loop())
            await asyncio.sleep(0.05)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        asyncio.run(satu_siklus())
        assert dipanggil == [SYM]
