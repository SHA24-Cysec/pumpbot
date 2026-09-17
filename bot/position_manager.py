"""
PositionManager - loop pengawasan posisi terbuka (jalan tiap 1 detik).

Untuk SETIAP posisi terbuka:
  1. Update harga terakhir & harga tertinggi sejak entry.
  2. BREAKEVEN  : harga mencapai +1R dari SL awal
                  -> pindahkan SL ke entry + buffer (posisi jadi bebas risiko).
  3. TRAILING   : langsung setelah breakeven, SL terus naik mengikuti harga
                  (percent atau ATR) -> mengunci profit selama tren lanjut.
  4. MANUAL EXIT: kalau mode manual (tanpa OCO di exchange), bot sendiri
                  yang menjual saat TP/SL tersentuh.
  5. REKONSILIASI OCO (tiap beberapa detik): cek apakah TP/SL OCO sudah
                  tereksekusi di exchange.
  6. WATCHDOG   : harga sudah jauh MENEMBUS SL tapi OCO masih hidup
                  (stop-limit tidak terisi) -> emergency market sell.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from bot.config import Config
from bot.data_collector.collector import DataCollector
from bot.execution.executor import Executor
from bot.models import Position
from bot.risk_management.stops import (
    atr,
    breakeven_price,
    should_trigger_breakeven,
    should_update_exit_order,
    update_trailing,
)
from bot.utils import now_ms

logger = logging.getLogger("pumpbot.posmgr")


class PositionManager:
    def __init__(self, cfg: Config, executor: Executor, collector: DataCollector):
        self.cfg = cfg
        self.executor = executor
        self.collector = collector
        self.paused = False            # pause menghentikan ENTRY, bukan pengelolaan
        self._task: Optional[asyncio.Task] = None
        self._last_reconcile = 0

    async def run(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="position-manager")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    async def _loop(self) -> None:
        try:
            while True:
                positions = list(self.executor.positions.values())
                # rekonsiliasi OCO cukup sekali per siklus (hemat rate limit)
                reconcile_due = (now_ms() - self._last_reconcile
                                 >= self.cfg.execution.reconcile_sec * 1000)
                for pos in positions:
                    try:
                        await self._manage(pos, reconcile_due)
                    except Exception as exc:
                        logger.exception(f"Error mengelola posisi #{pos.trade_id}: {exc}")
                if reconcile_due:
                    self._last_reconcile = now_ms()
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    async def _manage(self, pos: Position, reconcile_due: bool = False) -> None:
        if pos.status != "OPEN":
            return
        price = self.collector.last_price(pos.symbol)
        if price <= 0:
            return
        pos._last_price = price  # cache utk perhitungan nilai posisi
        pos.highest_price = max(pos.highest_price, price)

        manual_mode = (pos.exit_mode == "manual") or pos.oco_fallback

        # ---------------- 4/6. exit manual (bot sebagai eksekutor) --------
        if manual_mode:
            await self._manual_exit_checks(pos, price)

        # ---------------- 2. breakeven ------------------------------------
        if (self.cfg.breakeven.enabled and not pos.be_triggered):
            be_cfg = self.cfg.breakeven
            # BE di +1R (atau rasio yang dikonfigurasi), bukan saat TP.
            # Jadi dengan TP 2R, trailing sudah mulai bekerja sebelum TP.
            if should_trigger_breakeven(
                    price=price,
                    entry=pos.entry_price,
                    initial_stop=pos.initial_stop,
                    trigger_rr=be_cfg.trigger_rr):
                new_sl = breakeven_price(pos.entry_price, be_cfg.buffer_pct,
                                         self.cfg.risk.fee_pct)
                if new_sl > pos.stop_loss:
                    old = pos.stop_loss
                    pos.stop_loss = new_sl
                    pos.be_triggered = True
                    self.executor.db.update_trade(pos.trade_id, stop_loss=new_sl,
                                                  be_triggered=1)
                    self.executor.db.add_trade_event(
                        pos.trade_id, "BREAKEVEN", price, pos.qty_remaining, 0.0,
                        f"SL {old:.6f} -> {new_sl:.6f} (posisi bebas risiko)")
                    logger.info(f"BREAKEVEN #{pos.trade_id} {pos.symbol}: "
                                f"SL {old:.6f} -> {new_sl:.6f}")
                    await self.executor.sync_exit_orders(pos)

        # ---------------- 3. trailing stop --------------------------------
        if (self.cfg.trailing.enabled and pos.be_triggered
                and not pos.trail_active):
            pos.trail_active = True   # aktif otomatis setelah BE; jalan terus
            self.executor.db.update_trade(pos.trade_id, trail_active=1)
        if pos.trail_active:
            atr_value = 0.0
            if self.cfg.trailing.mode == "atr":
                buf = self.collector.buffer(pos.symbol)
                if buf and buf.candles:
                    atr_value = atr(list(buf.candles)[-self.cfg.trailing.atr_period - 1:],
                                    self.cfg.trailing.atr_period)
            old_sl = pos.stop_loss
            new_sl = update_trailing(
                current_sl=old_sl,
                highest=pos.highest_price,
                entry=pos.entry_price,
                mode=self.cfg.trailing.mode,
                percent_pct=self.cfg.trailing.percent_pct,
                atr_value=atr_value,
                atr_multiplier=self.cfg.trailing.atr_multiplier,
            )
            # hanya republish order kalau SL naik cukup berarti (hemat rate limit)
            if should_update_exit_order(old_sl, new_sl, self.cfg.trailing.update_step_pct):
                pos.stop_loss = new_sl
                self.executor.db.update_trade(pos.trade_id, stop_loss=new_sl)
                self.executor.db.add_trade_event(
                    pos.trade_id, "TRAIL_UPDATE", price, pos.qty_remaining, 0.0,
                    f"SL {old_sl:.6f} -> {new_sl:.6f} (trail, high {pos.highest_price:.6f})")
                logger.debug(f"TRAIL #{pos.trade_id} {pos.symbol}: "
                             f"SL {old_sl:.6f} -> {new_sl:.6f}")
                await self.executor.sync_exit_orders(pos)

        # ---------------- 5. rekonsiliasi OCO berkala ---------------------
        if not manual_mode and reconcile_due:
            await self.executor.reconcile_oco(pos)

        # ---------------- 6. watchdog: SL tembus tapi OCO hidup ------------
        if not manual_mode and price < pos.stop_loss * 0.99:
            # harga 1% DI BAWAH SL tapi posisi masih terbuka -> kemungkinan
            # stop-limit OCO tidak terisi -> jual market darurat
            logger.error(
                f"WATCHDOG #{pos.trade_id} {pos.symbol}: harga {price:.6f} sudah "
                f"1%+ di bawah SL {pos.stop_loss:.6f} tapi posisi masih terbuka "
                f"-> emergency market sell")
            await self.executor.close_position(pos, "SL (watchdog emergency)")

    # ------------------------------------------------------------------
    async def _manual_exit_checks(self, pos: Position, price: float) -> None:
        """Eksekusi TP/SP langsung oleh bot (mode manual / fallback OCO)."""
        # SL kena? -> jual semua sisa
        if price <= pos.stop_loss:
            await self.executor.close_position(pos, "SL (manual)")
            return
        # TP berikutnya tersentuh? -> jual chunk tsb (partial take profit).
        # Status chunk baru ditandai FILLED setelah market sell sukses.
        # Jika sell gagal sementara chunk sudah ditandai FILLED, bot tidak akan
        # retry TP itu lagi dan posisi bisa menggantung.
        for chunk in pos.chunks:
            if chunk.status == "PENDING" and price >= chunk.tp_price:
                fraction = (chunk.qty / pos.qty_remaining
                            if pos.qty_remaining > 0 else 1.0)
                ok = await self.executor.close_position(pos, "TP (manual)",
                                                        fraction=fraction)
                if ok:
                    chunk.status = "FILLED"
                    self.executor.db.update_trade(
                        pos.trade_id, qty_remaining=pos.qty_remaining,
                        realized_pnl=round(pos.realized_pnl, 6),
                        fees_paid=round(pos.fees_paid, 6))
                    await self.executor._finalize_if_done(pos, price, "TP (manual)")
                return
