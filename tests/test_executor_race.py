"""
Test regresi RACE-CONDITION executor (paper/simulated gateway).

Skenario yang pernah jadi bug nyata: pada detik yang sama,
PositionManager (breakeven/trailing) me-render OCO ulang sementara
user menekan tombol "tutup posisi" di dashboard. Tanpa serialisasi:

  1. close_position membaca chunk.oco_list_id == None (OCO baru masih
     dalam proses pemasangan) -> tidak ada yang dibatalkan
  2. OCO baru jadi YATIM (orphaned) setelah record posisi ditutup
  3. OCO yatim terisi di exchange/simulator -> kredit quote hantu
     (paper equity membengkak; di live akan coba jual aset yang sudah
     tidak dimiliki -> ditolak insufficient balance)

Invarian yang dicek setelah close + re-place berjalan serentak:
  a. posisi berstatus CLOSED
  b. tidak ada OCO yatim yang masih EXECUTING untuk simbol tsb
  c. saldo base simulator tidak pernah negatif
  d. konservasi dana: equity == saldo awal + realized_pnl (toleransi
     kecil untuk fee beli 0.1% + dust pembulatan)
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


def test_race_close_vs_oco_replace():
    asyncio.run(_race_scenario())


async def _race_scenario():
    cfg = load_config(_CFG)
    db = Database(os.path.join(tempfile.mkdtemp(), "race.db"))
    gw = SimulatedGateway(start_equity=10_000.0, symbols=3,
                          time_scale=1.0, seed=11)

    # Latensi jaringan realistis (REST round-trip ~100 ms) saat memasang
    # OCO. Tanpa ini jendela race-nya terlalu sempit untuk direproduksi
    # deterministik; di exchange nyata latensi inilah yang membuka celah
    # antara "cancel OCO lama" dan "oco_list_id baru terisi".
    _orig_place = gw.place_oco_sell

    async def _slow_place(*args, **kwargs):
        await asyncio.sleep(0.1)
        return await _orig_place(*args, **kwargs)

    gw.place_oco_sell = _slow_place
    await gw.start()
    try:
        ex = Executor(cfg, gw, db, Portfolio(cfg, gw), RiskManager(cfg))
        ex.filters = await gw.get_symbol_filters()

        sym = next(iter(gw.sims))
        price = gw.sims[sym].price
        sig = Signal(ts=now_ms(), symbol=sym, price=price, score=80,
                     breakdown={}, suggested_stop=price * 0.97,
                     reason="test race")
        pos = await ex.try_enter(sig)
        assert pos is not None and pos.status == "OPEN", "entry gagal"

        # (a) posisi harus tertutup penuh walau re-place dan close
        #     ditembakkan SEREMBAGAI (asyncio.gather)
        pos.last_oco_sync = 0   # lewati throttle 5 detik agar sync jalan
        await asyncio.gather(
            ex.sync_exit_orders(pos),
            ex.close_position(pos, "MANUAL (test)", fraction=1.0),
        )
        await asyncio.sleep(1.0)   # beri waktu watcher OCO memproses tick

        assert pos.status == "CLOSED", "posisi tidak tertutup"

        # (b) tidak boleh ada OCO yatim yang masih hidup
        live = [o for o in gw._oco_orders.values()
                if o["symbol"] == sym and o["status"] == "EXECUTING"]
        assert not live, f"OCO yatim tertinggal: {live}"

        # (c) saldo base tidak boleh negatif (penjualan ganda)
        assert gw.base_balances.get(sym, 0.0) >= 0, \
            f"saldo base negatif: {gw.base_balances.get(sym)}"

        # (d) konservasi dana: tidak ada kredit hantu dari OCO yatim
        px_now = gw.sims[sym].price
        equity = gw.balance_quote + gw.base_balances.get(sym, 0.0) * px_now
        expected = 10_000.0 + pos.realized_pnl
        tol = pos.quote_value * 0.02 + 2.0   # fee beli + dust pembulatan
        assert abs(equity - expected) <= tol, (
            f"equity {equity:.2f} != {expected:.2f} "
            f"(selisih {equity - expected:+.2f} -> indikasi kredit hantu)")
    finally:
        await gw.stop()
