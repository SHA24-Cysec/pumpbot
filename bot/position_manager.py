"""
PositionManager - loop pengawasan posisi terbuka (jalan tiap 1 detik).

Untuk SETIAP posisi terbuka:
  1. Update harga terakhir & harga tertinggi sejak entry.
  2. BREAKEVEN  : harga mencapai pemicu (basis R: +trigger_rr x jarak SL awal,
                  atau basis ATR: +trigger_atr_mult x ATR entry)
                  -> pindahkan SL ke entry + buffer (posisi jadi bebas risiko).
  3. TRAILING   : langsung setelah breakeven, SL terus naik mengikuti harga,
                  jaraknya dihitung dari persen harga tertinggi ATAU dari
                  kelipatan ATR entry (trailing.mode percent|atr) -> mengunci
                  profit. ATR sengaja DIBEKUKAN saat entry (pos.atr_entry).
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
    be_trigger_mode,
    breakeven_price,
    should_trigger_be,
    should_update_exit_order,
    update_trailing,
    update_trailing_atr,
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
                # Simbol yang baru saja mengirim event akun lewat User Data
                # Stream dicek SEKARANG tanpa menunggu throttle, sehingga fill
                # TP/SL tercatat dalam hitungan milidetik, bukan detik.
                ambil_urgen = getattr(self.executor, "take_urgent_symbols", None)
                urgen = ambil_urgen() if callable(ambil_urgen) else set()
                for pos in positions:
                    try:
                        await self._manage(
                            pos, reconcile_due or pos.symbol in urgen)
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
        self.executor.last_prices[pos.symbol] = price  # fallback jalur lain
        pos.highest_price = max(pos.highest_price, price)

        # ATR yang DIBEKUKAN saat entry (0.0 = tidak tersedia saat entry).
        # Nilai ini tidak pernah dihitung ulang di sini: kalau dihitung ulang,
        # SL/TP sebuah posisi bisa bergerak hanya karena volatilitas berubah,
        # tanpa aksi harga yang berarti.
        atr_entry = float(getattr(pos, "atr_entry", 0.0) or 0.0)

        manual_mode = (pos.exit_mode == "manual") or pos.oco_fallback

        # Data basi: harga di buffer tidak bisa dipercaya. Pada mode OCO,
        # proteksi SL/TP sudah hidup di exchange sehingga lebih aman MENUNDA
        # penggeseran SL (breakeven/trailing) dan emergency sell berbasis
        # harga basi daripada bertindak dengan angka lama. Rekonsiliasi OCO
        # tetap jalan karena memakai data REST, bukan stream.
        cek_basi = getattr(self.collector, "data_is_stale", None)
        stale = bool(cek_basi(
            float(getattr(self.cfg.execution, "max_data_age_sec", 0.0) or 0.0)
        )) if callable(cek_basi) else False
        if stale and not manual_mode:
            if reconcile_due:
                await self.executor.reconcile_oco(pos)
            return

        # ---------------- 4/6. exit manual (bot sebagai eksekutor) --------
        if manual_mode:
            await self._manual_exit_checks(pos, price)

        # ---------------- 2. breakeven ------------------------------------
        if (self.cfg.breakeven.enabled and not pos.be_triggered):
            be_cfg = self.cfg.breakeven
            # Basis pemicu BE: 'auto' mengikuti basis SL (stops.mode), jadi
            # SL berbasis ATR otomatis memakai pemicu berbasis ATR juga.
            be_mode = be_trigger_mode(be_cfg.trigger_mode, self.cfg.stops.mode)
            if be_mode == "atr" and atr_entry <= 0:
                # Tidak ada ATR (entry lama/fallback) -> pakai basis R supaya
                # proteksi breakeven tidak hilang.
                be_mode = "rr"
            # BE di +1R (rasio config) atau +k x ATR, bukan saat TP. Jadi
            # dengan TP 2R, trailing sudah mulai bekerja sebelum TP.
            if should_trigger_be(
                    price=price,
                    entry=pos.entry_price,
                    initial_stop=pos.initial_stop,
                    mode=be_mode,
                    trigger_rr=be_cfg.trigger_rr,
                    atr=atr_entry,
                    trigger_atr_mult=be_cfg.trigger_atr_mult):
                new_sl = breakeven_price(pos.entry_price, be_cfg.buffer_pct,
                                         self.cfg.risk.fee_pct)
                if new_sl > pos.stop_loss:
                    old = pos.stop_loss
                    pos.stop_loss = new_sl
                    pos.be_triggered = True
                    basis_txt = (f"ATR {atr_entry:.6g} x {be_cfg.trigger_atr_mult}"
                                 if be_mode == "atr" else
                                 f"{be_cfg.trigger_rr}R")
                    self.executor.db.update_trade(pos.trade_id, stop_loss=new_sl,
                                                  be_triggered=1)
                    self.executor.db.add_trade_event(
                        pos.trade_id, "BREAKEVEN", price, pos.qty_remaining, 0.0,
                        f"SL {old:.6f} -> {new_sl:.6f} (posisi bebas risiko, "
                        f"pemicu {basis_txt})")
                    logger.info(f"BREAKEVEN #{pos.trade_id} {pos.symbol}: "
                                f"SL {old:.6f} -> {new_sl:.6f} ({basis_txt})")
                    await self.executor.sync_exit_orders(pos)

        # ---------------- 3. trailing stop --------------------------------
        if (self.cfg.trailing.enabled and pos.be_triggered
                and not pos.trail_active):
            pos.trail_active = True   # aktif otomatis setelah BE; jalan terus
            self.executor.db.update_trade(pos.trade_id, trail_active=1)
        if pos.trail_active:
            old_sl = pos.stop_loss
            tr_cfg = self.cfg.trailing
            # Basis ATR dipakai hanya bila ATR entry tersedia; kalau tidak
            # (posisi lama sebelum fitur ini, atau ATR gagal saat entry),
            # trailing persen tetap berjalan supaya posisi tidak tanpa proteksi.
            if tr_cfg.mode == "atr" and atr_entry > 0:
                new_sl = update_trailing_atr(
                    current_sl=old_sl,
                    highest=pos.highest_price,
                    atr=atr_entry,
                    multiplier=tr_cfg.atr_multiplier,
                )
                basis_txt = f"ATR {atr_entry:.6g} x {tr_cfg.atr_multiplier}"
            else:
                new_sl = update_trailing(
                    current_sl=old_sl,
                    highest=pos.highest_price,
                    percent_pct=tr_cfg.percent_pct,
                )
                basis_txt = f"persen {tr_cfg.percent_pct}%"
            # hanya republish order kalau SL naik cukup berarti (hemat rate limit)
            if should_update_exit_order(old_sl, new_sl, tr_cfg.update_step_pct):
                pos.stop_loss = new_sl
                self.executor.db.update_trade(pos.trade_id, stop_loss=new_sl)
                self.executor.db.add_trade_event(
                    pos.trade_id, "TRAIL_UPDATE", price, pos.qty_remaining, 0.0,
                    f"SL {old_sl:.6f} -> {new_sl:.6f} (trail {basis_txt}, "
                    f"high {pos.highest_price:.6f})")
                logger.debug(f"TRAIL #{pos.trade_id} {pos.symbol}: "
                             f"SL {old_sl:.6f} -> {new_sl:.6f} ({basis_txt})")
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
