"""
Test END-TO-END jalur STOP LOSS (paper/simulated gateway).

Membuka posisi nyata lewat Executor.try_enter, memasang OCO, lalu
harga dipaksa turun di bawah stop -> OCO SL harus terisi di simulator,
direkonsiliasi executor ("SL (OCO)"), chunk sibling ter-cancel, posisi
ditutup dengan PnL negatif, dan akuntansi tetap konsisten
(equity == modal awal + realized_pnl).
"""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import load_config
from bot.database.db import Database
from bot.exchange.simulated_gateway import SimulatedGateway
from bot.execution.executor import Executor
from bot.models import Signal
from bot.portfolio import Portfolio
from bot.risk_management.manager import RiskManager
from bot.utils import now_ms

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CFG = os.path.join(_ROOT, "config", "config.yaml")


def test_sl_exit_end_to_end():
    asyncio.run(_sl_scenario())


async def _sl_scenario():
    cfg = load_config(_CFG)
    db = Database(os.path.join(tempfile.mkdtemp(), "sl.db"))
    gw = SimulatedGateway(start_equity=10_000.0, symbols=3,
                          time_scale=1.0, seed=23)
    await gw.start()
    try:
        ex = Executor(cfg, gw, db, Portfolio(cfg, gw), RiskManager(cfg))
        ex.filters = await gw.get_symbol_filters()

        sym = next(iter(gw.sims))
        price = gw.sims[sym].price
        sig = Signal(ts=now_ms(), symbol=sym, price=price, score=80,
                     breakdown={}, suggested_stop=price * 0.98,
                     reason="test SL")
        pos = await ex.try_enter(sig)
        assert pos is not None and pos.status == "OPEN", "entry gagal"
        assert pos.chunks, "tidak ada chunk TP"
        assert any(c.oco_list_id for c in pos.chunks), "OCO tidak terpasang"

        # paksa harga turun di bawah SL (simulasi dump);
        # set berulang agar watcher OCO (tick 200 ms) pasti melihatnya
        target = pos.stop_loss * 0.97
        for _ in range(30):
            gw.sims[sym].price = target
            await asyncio.sleep(0.1)
            # executor merekonsiliasi OCO tiap siklus position manager;
            # panggil langsung supaya test tidak bergantung timing loop
            await ex.reconcile_oco(pos)
            if pos.status == "CLOSED":
                break

        assert pos.status == "CLOSED", "posisi tidak tertutup oleh SL"
        assert pos.exit_reason == "SL (OCO)", f"alasan exit: {pos.exit_reason}"
        assert pos.realized_pnl < 0, "SL harus profit-negatif"
        # semua chunk tidak boleh PENDING lagi (sibling ter-cancel)
        assert all(c.status != "PENDING" for c in pos.chunks)
        # tidak ada OCO yatim
        live = [o for o in gw._oco_orders.values()
                if o["symbol"] == sym and o["status"] == "EXECUTING"]
        assert not live, f"OCO yatim: {live}"

        # akuntansi: equity == modal + realized (toleransi dust/fee kecil)
        px_now = gw.sims[sym].price
        equity = gw.balance_quote + gw.base_balances.get(sym, 0.0) * px_now
        expected = 10_000.0 + pos.realized_pnl
        tol = pos.quote_value * 0.02 + 2.0
        assert abs(equity - expected) <= tol, (
            f"equity {equity:.2f} != {expected:.2f} "
            f"(selisih {equity - expected:+.2f})")

        # kerugian tidak boleh melebihi risiko yang diizinkan
        # (risk_per_trade_pct dari config demo + toleransi slippage/fee)
        risk_budget = 10_000.0 * cfg.risk.risk_per_trade_pct / 100.0
        slippage_tol = risk_budget * 0.6 + pos.quote_value * 0.01
        assert pos.realized_pnl > -(risk_budget + slippage_tol), (
            f"kerugian {pos.realized_pnl:.2f} melebihi budget risiko "
            f"{risk_budget:.2f}")
    finally:
        await gw.stop()
