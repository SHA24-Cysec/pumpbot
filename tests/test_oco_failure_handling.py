"""Regresi: OCO gagal diklasifikasikan, muncul di event dashboard, dan posisi ditutup."""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import Config
from bot.database.db import Database
from bot.exchange.simulated_gateway import SimulatedGateway
from bot.execution.executor import Executor, classify_oco_failure
from bot.models import Signal
from bot.portfolio import Portfolio
from bot.risk_management.manager import RiskManager
from bot.utils import now_ms


def test_classify_oco_failure_basic_causes():
    cases = [
        ("(-1013, 'Filter failure: PRICE_FILTER')", "PRICE_FILTER"),
        ("Filter failure: LOT_SIZE", "LOT_SIZE"),
        ("Filter failure: MIN_NOTIONAL", "MIN_NOTIONAL"),
        ("Account has insufficient balance", "INSUFFICIENT_BALANCE"),
        ("Order would immediately trigger", "IMMEDIATE_TRIGGER"),
        ("429 Too many requests", "RATE_LIMIT"),
        ("Network timeout", "NETWORK"),
    ]
    for raw, code in cases:
        assert classify_oco_failure(Exception(raw))[0] == code


def test_oco_failure_closes_position_and_records_dashboard_events():
    asyncio.run(_oco_failure_scenario())


async def _oco_failure_scenario():
    cfg = Config()
    db = Database(os.path.join(tempfile.mkdtemp(), "oco_fail.db"))
    gw = SimulatedGateway(start_equity=10_000.0, symbols=1, seed=1)
    await gw.start()
    try:
        async def _fail_oco(*args, **kwargs):
            raise Exception("(-1013, 'Filter failure: PRICE_FILTER')")

        gw.place_oco_sell = _fail_oco
        ex = Executor(cfg, gw, db, Portfolio(cfg, gw), RiskManager(cfg))
        ex.filters = await gw.get_symbol_filters()

        sym = next(iter(gw.sims))
        price = gw.sims[sym].price
        sig = Signal(ts=now_ms(), symbol=sym, price=price, score=80,
                     breakdown={}, suggested_stop=price * 0.98,
                     reason="test OCO fail")
        pos = await ex.try_enter(sig)

        assert pos is not None
        assert pos.status == "CLOSED"
        assert pos.oco_failure_code == "PRICE_FILTER"
        assert "PRICE_FILTER" in pos.exit_reason

        events = db.get_recent_events(20)
        types = [e["type"] for e in events]
        assert "OCO_FAIL_PRICE_FILTER" in types
        assert "OCO_FAILED_POSITION_CLOSED" in types
    finally:
        await gw.stop()


def test_close_insufficient_balance_reconciles_external_fill():
    asyncio.run(_close_insufficient_balance_scenario())


async def _close_insufficient_balance_scenario():
    cfg = Config()
    cfg.execution.exit_mode = "manual"
    db = Database(os.path.join(tempfile.mkdtemp(), "external_close.db"))
    gw = SimulatedGateway(start_equity=10_000.0, symbols=1, seed=2)
    await gw.start()
    try:
        ex = Executor(cfg, gw, db, Portfolio(cfg, gw), RiskManager(cfg))
        ex.filters = await gw.get_symbol_filters()

        sym = next(iter(gw.sims))
        price = gw.sims[sym].price
        sig = Signal(ts=now_ms(), symbol=sym, price=price, score=80,
                     breakdown={}, suggested_stop=price * 0.98,
                     reason="test external close")
        pos = await ex.try_enter(sig)
        assert pos is not None and pos.status == "OPEN"

        # Simulasi kondisi exchange sudah menjual aset (mis. OCO/manual Binance),
        # tapi record lokal bot masih OPEN, lalu market sell ditolak -2010.
        gw.base_balances[sym] = 0.0

        async def _fail_sell(*args, **kwargs):
            raise Exception("(-2010, 'Account has insufficient balance for requested action.')")

        gw.market_sell = _fail_sell
        ok = await ex.close_position(pos, "SL (manual)")

        assert ok
        assert pos.status == "CLOSED"
        assert pos.trade_id not in ex.positions
        events = db.get_recent_events(20)
        assert any(e["type"] == "EXTERNAL_CLOSE_RECONCILED" for e in events)
    finally:
        await gw.stop()
