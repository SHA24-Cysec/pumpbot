"""
Regresi temuan audit keselamatan dana mode LIVE.

Setiap test di file ini GAGAL pada kode sebelum patch audit dan LULUS
sesudahnya:

  F-01  status order tidak pasti (5xx/timeout) -> harus direkonsiliasi
        lewat origClientOrderId, bukan dianggap gagal / diulang buta.
  F-02  cancel_oco tidak boleh melaporkan sukses untuk error yang bukan
        "order tidak ada".
  F-03  market sell penutup gagal -> posisi tidak boleh ditinggal tanpa
        proteksi exchange (OCO wajib dipasang ulang).
  F-04  pembatalan OCO tidak pasti -> penutupan market dibatalkan (anti
        sell ganda).
  F-05  gate data basi memblokir entry baru.
  F-06  restore restart merekonsiliasi qty database dengan saldo nyata.

Semua memakai mock/stub; tidak ada panggilan jaringan dan tidak ada API key.
"""

from __future__ import annotations

import asyncio
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import load_config                       # noqa: E402
from bot.execution.executor import Executor              # noqa: E402
from bot.models import ExitChunk, Fill, Position, SymbolFilters  # noqa: E402
from bot.portfolio import Portfolio                      # noqa: E402
from bot.risk_management.manager import RiskManager      # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CFG = os.path.join(_ROOT, "config", "config.yaml")

SYM = "TESTUSDT"


# ---------------------------------------------------------------- util umum

class _FakeDB:
    """Database tiruan: cukup mencatat event agar assert bisa membacanya."""

    def __init__(self):
        self.events: list[tuple] = []
        self.trade_events: list[tuple] = []
        self.updates: list[dict] = []
        self.closed: list[tuple] = []

    def record_event(self, level, type_, message, symbol=None):
        self.events.append((level, type_, message, symbol))

    def add_trade_event(self, trade_id, event, price, qty, pnl, detail=""):
        self.trade_events.append((trade_id, event, price, qty, pnl, detail))

    def update_trade(self, trade_id, **fields):
        self.updates.append({"id": trade_id, **fields})

    def close_trade(self, trade_id, exit_time, exit_price, pnl, reason,
                    qty_remaining=0.0):
        self.closed.append((trade_id, exit_price, reason))

    def record_signal(self, symbol, price, score, breakdown):
        pass

    def kv_get(self, key, default=None):
        return default

    def kv_set(self, key, value):
        pass


def _filters() -> SymbolFilters:
    return SymbolFilters(symbol=SYM, tick_size=0.0001, step_size=0.001,
                         min_qty=0.001, min_notional=5.0)


def _posisi() -> Position:
    chunks = [ExitChunk(qty=10.0, tp_price=1.2, oco_list_id=101)]
    return Position(
        trade_id=1, symbol=SYM, entry_time=0, entry_price=1.0,
        qty_total=10.0, qty_remaining=10.0, quote_value=10.0,
        stop_loss=0.98, initial_stop=0.98, take_profits=[1.2],
        chunks=chunks, highest_price=1.0, score=80, entry_reason="test",
        exit_mode="oco",
    )


def _executor(gw) -> Executor:
    cfg = load_config(_CFG)
    ex = Executor(cfg, gw, _FakeDB(), Portfolio(cfg, gw), RiskManager(cfg))
    ex.filters = {SYM: _filters()}
    ex.last_prices[SYM] = 1.0
    return ex


# --------------------------------------------------- F-01 & F-02 (gateway)

def _gateway_live():
    """BinanceGateway mode live tanpa jaringan (semua REST di-monkeypatch)."""
    pytest.importorskip("binance_common")
    pytest.importorskip("binance_sdk_spot")
    from bot.exchange.binance_gateway import BinanceGateway
    return BinanceGateway(mode="live", api_key="x" * 8, api_secret="y" * 8,
                          quote_asset="USDT")


class TestStatusOrderTidakPasti:
    """F-01: 5xx/timeout saat kirim order = status TIDAK DIKETAHUI."""

    def test_market_buy_direkonsiliasi_lewat_client_order_id(self):
        from binance_common.errors import ServerError
        gw = _gateway_live()
        panggilan = {"new_order": 0, "query": 0}

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            if "orig_client_order_id" in kwargs:
                panggilan["query"] += 1
                return {
                    "symbol": SYM,
                    "orderId": 555,
                    "status": "FILLED",
                    "executedQty": "10",
                    "cummulativeQuoteQty": "10",
                    "price": "1.0",
                }
            panggilan["new_order"] += 1
            raise ServerError(error_message="Server error: 503", status_code=503)

        gw._rest = fake_rest
        fill = asyncio.run(gw.market_buy(SYM, 10.0))

        # order hanya DIKIRIM sekali (tidak ada retry buta = tidak ada order ganda)
        assert panggilan["new_order"] == 1
        assert panggilan["query"] >= 1
        # fill nyata dari exchange dipakai, bukan exception
        assert fill.order_id == 555
        assert fill.qty == pytest.approx(10.0)

    def test_market_buy_tetap_gagal_bila_order_benar_benar_tidak_ada(self):
        from binance_common.errors import BadRequestError, ServerError
        gw = _gateway_live()

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            if "orig_client_order_id" in kwargs:
                raise BadRequestError(error_message="Order does not exist.",
                                      status_code=-2013)
            raise ServerError(error_message="Server error: 500", status_code=500)

        gw._rest = fake_rest
        with pytest.raises(ServerError):
            asyncio.run(gw.market_buy(SYM, 10.0))

    def test_oco_direkonsiliasi_lewat_list_client_order_id(self):
        from binance_common.errors import NetworkError
        gw = _gateway_live()
        kirim = {"n": 0}

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            if "orig_client_order_id" in kwargs:
                return {"orderListId": 9911, "listStatusType": "EXEC_STARTED",
                        "orders": []}
            kirim["n"] += 1
            raise NetworkError(error_message="Network error: timeout")

        gw._rest = fake_rest
        list_id = asyncio.run(gw.place_oco_sell(SYM, 10.0, 1.2, 0.98))
        assert kirim["n"] == 1          # tidak ada OCO ganda
        assert list_id == 9911


class TestCancelOcoJujur:
    """F-02: cancel_oco hanya True bila OCO benar-benar tidak aktif lagi."""

    def test_error_bukan_unknown_order_mengembalikan_false(self):
        from binance_common.errors import BadRequestError
        gw = _gateway_live()

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            raise BadRequestError(
                error_message="Timestamp for this request is outside of the "
                              "recvWindow.", status_code=-1021)

        gw._rest = fake_rest
        assert asyncio.run(gw.cancel_oco(SYM, 101)) is False

    def test_unknown_order_dianggap_sudah_batal(self):
        from binance_common.errors import BadRequestError
        gw = _gateway_live()

        async def fake_rest(fn, *args, retry_transient=True, **kwargs):
            raise BadRequestError(error_message="Unknown order sent.",
                                  status_code=-2011)

        gw._rest = fake_rest
        assert asyncio.run(gw.cancel_oco(SYM, 101)) is True


# --------------------------------------------------------- F-03 & F-04

class _GwCloseGagal:
    """Cancel OCO sukses, tetapi market sell selalu gagal (jaringan)."""

    def __init__(self):
        self.oco_dipasang = 0
        self.sell_dicoba = 0

    async def get_oco_status(self, symbol, order_list_id):
        return {"list_status": "EXEC_STARTED", "done": False,
                "any_filled": False, "filled_qty": 0.0, "filled_quote": 0.0,
                "avg_price": 0.0, "which": ""}

    async def cancel_oco(self, symbol, order_list_id):
        return True

    async def place_oco_sell(self, symbol, qty, tp_price, stop_price):
        self.oco_dipasang += 1
        return 900 + self.oco_dipasang

    async def market_sell(self, symbol, qty):
        self.sell_dicoba += 1
        raise ConnectionError("Network error: connection reset")

    async def get_base_balance(self, symbol):
        return 10.0, 0.0

    async def get_quote_balance(self):
        return 1000.0, 0.0


class _GwCancelTidakPasti:
    """cancel_oco gagal dan status OCO tetap aktif -> close harus batal."""

    def __init__(self):
        self.sell_dicoba = 0

    async def get_oco_status(self, symbol, order_list_id):
        return {"list_status": "EXEC_STARTED", "done": False,
                "any_filled": False, "filled_qty": 0.0, "filled_quote": 0.0,
                "avg_price": 0.0, "which": ""}

    async def cancel_oco(self, symbol, order_list_id):
        return False

    async def place_oco_sell(self, symbol, qty, tp_price, stop_price):
        return 777

    async def market_sell(self, symbol, qty):
        self.sell_dicoba += 1
        return Fill(symbol=symbol, price=1.0, qty=qty, quote_qty=qty,
                    fee_quote=0.0, order_id=1)

    async def get_base_balance(self, symbol):
        return 10.0, 0.0

    async def get_quote_balance(self):
        return 1000.0, 0.0


class TestPosisiTidakPernahTanpaProteksi:

    def test_close_gagal_memasang_ulang_oco(self):
        gw = _GwCloseGagal()
        ex = _executor(gw)
        pos = _posisi()
        ex.positions[pos.trade_id] = pos

        ok = asyncio.run(ex.close_position(pos, "MANUAL (test)"))

        assert ok is False
        assert pos.status == "OPEN"
        assert gw.sell_dicoba == 1
        # chunk kembali PENDING dan OCO baru terpasang
        assert all(c.status == "PENDING" for c in pos.chunks)
        assert gw.oco_dipasang == 1
        assert any(c.oco_list_id for c in pos.chunks)
        assert any(e[1] == "EXIT_ORDERS_REARMED" for e in ex.db.events)

    def test_close_dibatalkan_bila_cancel_oco_tidak_pasti(self):
        gw = _GwCancelTidakPasti()
        ex = _executor(gw)
        pos = _posisi()
        ex.positions[pos.trade_id] = pos

        ok = asyncio.run(ex.close_position(pos, "MANUAL (test)"))

        assert ok is False
        assert gw.sell_dicoba == 0          # tidak menjual di atas OCO hidup
        assert pos.status == "OPEN"
        assert any(e[1] == "CLOSE_ABORTED_OCO_ACTIVE" for e in ex.db.events)


# ------------------------------------------------------------------ F-05

class _GwData:
    def __init__(self, ts):
        self.last_data_ts = ts
        self.is_ws_connected = True


def _collector(ts):
    from bot.data_collector.collector import DataCollector
    cfg = load_config(_CFG)
    col = DataCollector(cfg, _GwData(ts))
    col.ws_alive = True
    return col


class TestGateDataBasi:

    def test_data_segar_tidak_dianggap_basi(self):
        import time
        col = _collector(time.time())
        assert col.data_is_stale(90) is False

    def test_data_lama_dianggap_basi(self):
        import time
        col = _collector(time.time() - 600)
        assert col.data_is_stale(90) is True

    def test_stream_belum_pernah_kirim_data_dianggap_basi(self):
        col = _collector(0.0)
        assert col.data_is_stale(90) is True

    def test_on_signal_memblokir_entry_saat_data_basi(self):
        import time
        from bot.main import BotApp
        from bot.models import Signal

        cfg = load_config(_CFG)
        db = _FakeDB()
        masuk = {"n": 0}

        class _Ex:
            positions: dict = {}

            async def try_enter(self, sig):
                masuk["n"] += 1

        ctx = types.SimpleNamespace(
            cfg=cfg, db=db, paused=False, collector=_collector(time.time() - 600),
            executor=_Ex(), risk=RiskManager(cfg),
            portfolio=types.SimpleNamespace(equity=lambda pos: 1000.0),
            _last_stale_log_ms=0,
        )
        sig = Signal(ts=0, symbol=SYM, price=1.0, score=90, breakdown={},
                     suggested_stop=0.98, reason="test")
        asyncio.run(BotApp.on_signal(ctx, sig))

        assert masuk["n"] == 0
        assert any(e[1] == "DATA_STALE" for e in db.events)


# ------------------------------------------------------------------ F-06

class TestRekonsiliasiRestart:

    def _ctx(self, free, locked):
        from bot.main import BotApp
        db = _FakeDB()

        class _Gw:
            async def get_base_balance(self, symbol):
                return free, locked

        ex = types.SimpleNamespace(filters={SYM: _filters()},
                                   positions={},
                                   _finalize_if_done=None)

        async def _finalize(pos, price, reason, force=False):
            pos.status = "CLOSED"

        ex._finalize_if_done = _finalize
        ctx = types.SimpleNamespace(db=db, gateway=_Gw(), executor=ex,
                                    mode="live")
        return BotApp._reconcile_restored_qty, ctx, db

    def test_aset_sudah_habis_menutup_record(self):
        fn, ctx, db = self._ctx(0.0, 0.0)
        pos = _posisi()
        lanjut = asyncio.run(fn(ctx, pos))
        assert lanjut is False
        assert pos.status == "CLOSED"
        assert any(e[1] == "RESTORE_EXTERNAL_CLOSE" for e in db.events)

    def test_qty_dipangkas_ke_saldo_nyata(self):
        fn, ctx, db = self._ctx(8.0, 0.0)
        pos = _posisi()
        lanjut = asyncio.run(fn(ctx, pos))
        assert lanjut is True
        assert pos.qty_remaining == pytest.approx(8.0)
        assert pos.chunks[0].qty == pytest.approx(8.0)
        assert any(e[1] == "RESTORE_QTY_ADJUSTED" for e in db.events)

    def test_saldo_cukup_tidak_mengubah_apa_apa(self):
        fn, ctx, db = self._ctx(10.0, 0.0)
        pos = _posisi()
        lanjut = asyncio.run(fn(ctx, pos))
        assert lanjut is True
        assert pos.qty_remaining == pytest.approx(10.0)
