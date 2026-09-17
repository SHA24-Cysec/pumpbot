"""
ExecutionEngine (Executor) - semua interaksi order ke exchange.

Tanggung jawab:
  * try_enter(signal)    : validasi risiko -> sizing -> order entry -> pasang exit
  * place_exit_orders()  : OCO per chunk TP (atau mode manual tanpa order)
  * reconcile_oco()      : cek status OCO di exchange, catat fill TP/SL
  * partial_exit()       : jual sebagian posisi (partial take profit)
  * close_position()     : tutup posisi (SL manual / tombol dashboard)

Semua kejadian dicatat ke database (tabel trades + trade_events) dengan
timestamp, harga, qty, dan ALASAN eksekusi.

Catatan penting OCO di Binance Spot:
  OCO = 2 leg (TP LIMIT_MAKER + SL STOP_LOSS_LIMIT). Untuk partial TP multi
  target, bot memasang SATU OCO PER CHUNK dengan stop yang sama. Saat SL
  digeser (breakeven/trailing), seluruh OCO posisi dibatalkan lalu dipasang
  ulang (throttle oleh trailing.update_step_pct).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from bot.config import Config
from bot.database.db import Database
from bot.exchange.gateway import ExchangeGateway
from bot.models import ExitChunk, Fill, Position, Signal, SymbolFilters
from bot.portfolio import Portfolio
from bot.risk_management.manager import RiskManager
from bot.risk_management.stops import (
    initial_stop,
    should_update_exit_order,
    take_profit_levels,
)
from bot.utils import now_ms

logger = logging.getLogger("pumpbot.exec")


class Executor:
    def __init__(self, cfg: Config, gateway: ExchangeGateway, db: Database,
                 portfolio: Portfolio, risk: RiskManager):
        self.cfg = cfg
        self.gateway = gateway
        self.db = db
        self.portfolio = portfolio
        self.risk = risk
        self.positions: dict[int, Position] = {}
        self.filters: dict[str, SymbolFilters] = {}
        # cache harga terakhir per simbol (di-update position manager)
        self.last_prices: dict[str, float] = {}
        self.entering: set[str] = set()   # anti entry ganda utk simbol sama
        # kunci serialisasi semua operasi yang memutasi OCO (re-place,
        # rekonsiliasi, tutup manual). Tanpa ini, breakeven yang sedang
        # memasang ulang OCO bisa saling serobot dengan tombol "tutup posisi"
        # di dashboard -> OCO lama tidak dibatalkan -> terisi dua kali.
        self._oco_lock = asyncio.Lock()

    # ==================================================================
    # ENTRY
    # ==================================================================
    async def try_enter(self, signal: Signal) -> Optional[Position]:
        """
        Alur entry lengkap. Return Position kalau berhasil, None kalau ditolak.
        Setiap penolakan dicatat dengan alasan yang jelas.
        """
        if signal.symbol in self.entering:
            return None
        self.entering.add(signal.symbol)
        try:
            return await self._try_enter_inner(signal)
        finally:
            self.entering.discard(signal.symbol)

    async def _try_enter_inner(self, signal: Signal) -> Optional[Position]:
        symbol = signal.symbol
        await self.portfolio.refresh()
        open_positions = list(self.positions.values())
        # Daily loss memakai equity berjalan, sedangkan sizing sengaja memakai
        # balance pada harga modal agar unrealized PnL tidak mengubah risk %.
        equity = self.portfolio.equity(open_positions)
        balance = self.portfolio.sizing_balance(open_positions)

        # ---- 1. pintu risiko ----
        ok, reason = self.risk.can_open_new(open_positions, equity, paused=False)
        if not ok:
            self.db.record_event("INFO", "ENTRY_REJECTED", f"{symbol}: {reason}", symbol)
            logger.info(f"Entry {symbol} DITOLAK: {reason}")
            return None

        # sudah punya posisi di simbol yang sama? tidak boleh double
        if any(p.symbol == symbol for p in open_positions):
            return None

        filters = self.filters.get(symbol)
        if filters is None:
            logger.warning(f"Filter exchangeInfo untuk {symbol} tidak ada - lewati")
            return None

        # ---- 2. tentukan entry & stop awal ----
        entry_ref = signal.price or self.last_prices.get(symbol, 0.0)
        if entry_ref <= 0:
            return None

        st = self.cfg.stops
        stop = initial_stop(
            entry=entry_ref,
            swing_low=signal.suggested_stop if signal.suggested_stop > 0 else None,
            mode=st.mode,
            percent_pct=st.percent_pct,
            min_stop_pct=st.min_stop_pct,
            max_stop_pct=st.max_stop_pct,
        )
        stop = filters.round_price(stop, "down")   # jangan lebih dekat karena pembulatan
        if stop <= 0 or stop >= entry_ref:
            self.db.record_event("INFO", "ENTRY_REJECTED",
                                 f"{symbol}: stop tidak valid", symbol)
            return None

        # ---- 3. sizing ----
        sizing = self.risk.size_position(
            balance=balance,
            available_quote=self.portfolio.quote_free,
            entry=entry_ref,
            stop=stop,
            filters=filters,
        )
        if sizing.qty <= 0:
            self.db.record_event("INFO", "ENTRY_REJECTED",
                                 f"{symbol}: sizing gagal - {sizing.reason}", symbol)
            logger.info(f"Entry {symbol} DITOLAK (sizing): {sizing.reason}")
            return None
        logger.info(
            f"Sizing {symbol}: qty={sizing.qty} notional={sizing.notional:.2f} "
            f"risiko={sizing.actual_risk_pct:.2f}% "
            f"(entry~{entry_ref:.6f} stop={stop:.6f})"
        )

        # ---- 4. kirim order entry ----
        fill: Optional[Fill] = None
        try:
            if self.cfg.execution.entry_order_type == "limit":
                fill = await self._limit_entry(symbol, sizing.qty, entry_ref, filters)
            else:
                fill = await self.gateway.market_buy(
                    symbol, round(sizing.notional, 2))
        except Exception as exc:
            self.db.record_event("ERROR", "ENTRY_FAILED",
                                 f"{symbol}: {exc}", symbol)
            logger.error(f"Order entry {symbol} GAGAL: {exc}")
            return None
        if fill is None or fill.qty <= 0:
            return None

        entry_price = fill.price
        # Qty yang BENAR-BENAR diterima = qty fill dikurangi fee beli
        # (di Binance spot tanpa BNB, fee beli dipotong dari aset yang
        # diterima). Kalau qty kotor yang dipakai, total qty OCO/TP akan
        # melebihi saldo base ~fee_pct dan kelebihannya tidak pernah
        # terjual -> equity terlihat "bocor" sebesar fee beli.
        fee_qty = (fill.fee_quote / fill.price) if fill.price > 0 else 0.0
        actual_qty = fill.qty - fee_qty
        # SL/TP dihitung ulang dari HARGA FILL sebenarnya
        stop = initial_stop(
            entry=entry_price,
            swing_low=(signal.suggested_stop if signal.suggested_stop > 0 and
                       signal.suggested_stop < entry_price else None),
            mode=st.mode, percent_pct=st.percent_pct,
            min_stop_pct=st.min_stop_pct, max_stop_pct=st.max_stop_pct,
        )
        tp_cfg = self.cfg.take_profit
        tps = take_profit_levels(entry_price, stop, tp_cfg.mode, tp_cfg.rr, tp_cfg.targets)

        # ---- 5. catat posisi ke DB ----
        trade_id = self.db.open_trade(
            symbol=symbol, entry_time=now_ms(), entry_price=entry_price,
            qty=actual_qty, quote_value=fill.quote_qty, stop_loss=stop,
            take_profits=[t["price"] for t in tps], score=signal.score,
            entry_reason=signal.reason, exit_mode=self.cfg.execution.exit_mode,
        )

        # bangun chunk partial TP: qty per chunk dibulatkan step,
        # chunk TERAKHIR mendapat sisa persis (anti dust)
        chunks = []
        remaining = actual_qty
        for i, t in enumerate(tps):
            if i == len(tps) - 1:
                cq = remaining
            else:
                cq = filters.round_qty(actual_qty * t["sell_pct"] / 100.0)
                cq = min(cq, remaining)
            if cq <= 0:
                continue
            chunks.append(ExitChunk(qty=cq, tp_price=filters.round_price(t["price"], "up")))
            remaining -= cq
        if not chunks:  # fallback: satu chunk penuh
            chunks = [ExitChunk(qty=actual_qty, tp_price=filters.round_price(
                entry_price * 1.02, "up"))]

        pos = Position(
            trade_id=trade_id, symbol=symbol, entry_time=now_ms(),
            entry_price=entry_price, qty_total=actual_qty,
            qty_remaining=actual_qty, quote_value=fill.quote_qty,
            stop_loss=stop, initial_stop=stop,
            take_profits=[c.tp_price for c in chunks], chunks=chunks,
            highest_price=entry_price, score=signal.score,
            entry_reason=signal.reason, exit_mode=self.cfg.execution.exit_mode,
        )
        pos.realized_pnl = -fill.fee_quote   # PnL ekonomis: mulai minus fee beli
        pos.fees_paid = fill.fee_quote
        self.positions[trade_id] = pos
        self.db.update_trade(trade_id,
                             realized_pnl=round(pos.realized_pnl, 6),
                             fees_paid=round(pos.fees_paid, 6))

        # ---- 6. pasang exit order (OCO) ----
        if pos.exit_mode == "oco":
            ok = await self.place_exit_orders(pos, force=True)
            if not ok:
                # fallback ke manajemen manual - posisi tetap terlindungi
                # selama bot hidup, tapi TANPA proteksi di sisi exchange.
                pos.oco_fallback = True
                logger.warning(
                    f"{symbol}: OCO gagal dipasang -> fallback ke mode manual "
                    f"(SL dipantau bot, bukan order di exchange)")
                self.db.add_trade_event(trade_id, "OCO_FALLBACK", entry_price,
                                        actual_qty, 0.0, "OCO gagal, mode manual")

        self.db.record_event(
            "INFO", "ENTRY",
            f"{symbol}: beli {actual_qty:.6f} @ {entry_price:.6f} "
            f"(notional {fill.quote_qty:.2f}, SL {stop:.6f}, "
            f"TP {[round(c.tp_price, 6) for c in chunks]})", symbol)
        logger.info(f"ENTRY #{trade_id} {symbol}: {actual_qty:.6f} @ {entry_price:.6f} "
                    f"SL={stop:.6f} TP={[round(c.tp_price, 6) for c in chunks]}")
        return pos

    async def _limit_entry(self, symbol: str, qty: float, ref_price: float,
                           filters: SymbolFilters) -> Optional[Fill]:
        """Entry limit di harga pasar saat itu; batal kalau tidak terisi dalam timeout."""
        price = filters.round_price(ref_price, "nearest")
        qty = filters.round_qty(qty)
        oid = await self.gateway.place_limit_buy(symbol, qty, price)
        deadline = now_ms() + self.cfg.execution.limit_entry_timeout_sec * 1000
        while now_ms() < deadline:
            await asyncio.sleep(1.0)
            st = await self.gateway.get_order_status(symbol, oid)
            if st["status"] == "FILLED":
                return Fill(symbol=symbol, price=st["avg_price"] or price,
                            qty=st["executed_qty"], quote_qty=st["quote_qty"],
                            fee_quote=st["quote_qty"] * self.cfg.risk.fee_pct / 100,
                            order_id=oid)
            if st["status"] in ("CANCELED", "REJECTED", "EXPIRED"):
                break
        # timeout: cancel, kalau terisi sebagian tetap dipakai
        await self.gateway.cancel_order(symbol, oid)
        st = await self.gateway.get_order_status(symbol, oid)
        if st["executed_qty"] > 0:
            logger.info(f"Limit entry {symbol} terisi sebagian: {st['executed_qty']}")
            return Fill(symbol=symbol, price=st["avg_price"] or price,
                        qty=st["executed_qty"], quote_qty=st["quote_qty"],
                        fee_quote=st["quote_qty"] * self.cfg.risk.fee_pct / 100,
                        order_id=oid)
        return None

    # ==================================================================
    # EXIT ORDERS (OCO)
    # ==================================================================
    async def _reconcile_chunks_locked(self, pos: Position) -> None:
        """
        Rekonsiliasi cepat: chunk PENDING yang OCO-nya ternyata SUDAH
        selesai di exchange. Fill-nya dicatat (partial_exit) dan chunk
        ditandai FILLED - TIDAK dipasang ulang. Ini mencegah menjual
        qty yang sudah tidak dimiliki lagi (double-fill).

        HARUS dipanggil dalam keadaan memegang self._oco_lock.
        """
        for chunk in pos.chunks:
            if chunk.status == "PENDING" and chunk.oco_list_id:
                try:
                    st = await self.gateway.get_oco_status(pos.symbol, chunk.oco_list_id)
                except Exception as exc:
                    logger.warning(f"Gagal cek OCO lama {pos.symbol}#{chunk.oco_list_id}: {exc}")
                    continue
                if not st.get("done"):
                    continue
                chunk.oco_list_id = None
                if st.get("any_filled") and st.get("filled_qty", 0) > 0:
                    chunk.status = "FILLED"
                    reason = "TP (OCO)" if st.get("which") == "tp" else "SL (OCO)"
                    await self.partial_exit(
                        pos, qty=st["filled_qty"], price=st["avg_price"],
                        reason=reason,
                        fee_quote=st["filled_quote"] * self.cfg.risk.fee_pct / 100)
                    if st.get("which") == "sl":
                        # SL kena -> sibling OCO otomatis ter-cancel di exchange
                        for c in pos.chunks:
                            if c.status == "PENDING":
                                c.status = "CANCELED"
                        break

    async def place_exit_orders(self, pos: Position, force: bool = False) -> bool:
        """Versi publik (dengan lock) dari _place_exit_orders_locked."""
        async with self._oco_lock:
            return await self._place_exit_orders_locked(pos, force)

    async def _place_exit_orders_locked(self, pos: Position,
                                        force: bool = False) -> bool:
        """
        (Re)pasang OCO untuk semua chunk yang belum selesai.
        Return False kalau SEMUA upaya gagal (pemanggil harus fallback manual).

        Dipanggil dengan self._oco_lock DIPEGANG (lewat place_exit_orders atau
        dari reconcile_oco / close_position yang sudah memegang lock).
        """
        filters = self.filters.get(pos.symbol)
        if not filters:
            return False

        # --- 1. rekonsiliasi cepat dulu (anti double-fill) ---
        await self._reconcile_chunks_locked(pos)
        if pos.status != "OPEN":
            return True   # ternyata posisi sudah selesai saat rekonsiliasi

        # --- 2. batalkan OCO aktif yang tersisa (akan diganti) ---
        for chunk in pos.chunks:
            if chunk.oco_list_id and chunk.status == "PENDING":
                await self.gateway.cancel_oco(pos.symbol, chunk.oco_list_id)
                chunk.oco_list_id = None

        # --- 3. pasang OCO baru hanya untuk chunk yang benar-benar pending,
        #        dengan qty dijepit ke sisa posisi (anti oversell) ---
        placed_any = False
        for chunk in pos.chunks:
            if chunk.status != "PENDING":
                continue
            qty = filters.round_qty(min(chunk.qty, pos.qty_remaining), market=True)
            if qty <= 0:
                chunk.status = "CANCELED"
                continue
            try:
                oco_id = await self.gateway.place_oco_sell(
                    symbol=pos.symbol, qty=qty,
                    tp_price=filters.round_price(chunk.tp_price, "up"),
                    stop_price=filters.round_price(pos.stop_loss, "down"),
                )
                chunk.oco_list_id = oco_id
                placed_any = True
            except Exception as exc:
                logger.error(f"Gagal pasang OCO {pos.symbol} chunk "
                             f"(qty {qty}): {exc}")
        pos.last_oco_sync = now_ms()
        return placed_any or all(c.status != "PENDING" for c in pos.chunks)

    async def sync_exit_orders(self, pos: Position) -> None:
        """
        Pasang ulang OCO setelah SL berpindah (breakeven / trailing).
        Keputusan 'apakah SL sudah berpindah cukup jauh' diambil PositionManager
        (memakai should_update_exit_order); di sini hanya ada jarak minimum
        antar replace untuk menghormati rate limit.
        """
        if pos.exit_mode != "oco" or pos.oco_fallback:
            return
        if pos.status != "OPEN":
            return
        if now_ms() - pos.last_oco_sync < 5_000:
            return
        ok = await self.place_exit_orders(pos)
        if not ok and any(c.status == "PENDING" for c in pos.chunks):
            pos.oco_fallback = True
            self.db.add_trade_event(pos.trade_id, "OCO_FALLBACK",
                                    pos.stop_loss, pos.qty_remaining, 0.0,
                                    "re-place OCO gagal -> manual")

    # ==================================================================
    # REKONSILIASI OCO (deteksi fill dari sisi exchange)
    # ==================================================================
    async def reconcile_oco(self, pos: Position) -> None:
        """Cek status OCO tiap chunk; kalau terisi -> catat partial/exit."""
        if pos.exit_mode != "oco" or pos.oco_fallback:
            return
        if pos.status != "OPEN":
            return
        async with self._oco_lock:
            for chunk in pos.chunks:
                if not chunk.oco_list_id or chunk.status != "PENDING":
                    continue
                try:
                    st = await self.gateway.get_oco_status(pos.symbol, chunk.oco_list_id)
                except Exception as exc:
                    logger.warning(f"Gagal cek OCO {pos.symbol}#{chunk.oco_list_id}: {exc}")
                    continue
                if not st.get("done"):
                    continue
                chunk.oco_list_id = None
                if st.get("any_filled") and st.get("filled_qty", 0) > 0:
                    chunk.status = "FILLED"     # WAJIB: cegah re-place OCO ganda
                    reason = "TP (OCO)" if st.get("which") == "tp" else "SL (OCO)"
                    await self.partial_exit(
                        pos, qty=st["filled_qty"], price=st["avg_price"],
                        reason=reason, fee_quote=st["filled_quote"] * self.cfg.risk.fee_pct / 100)
                    if st.get("which") == "sl":
                        # SL kena -> sisa chunk otomatis ter-cancel oleh OCO induk
                        for c in pos.chunks:
                            if c.status == "PENDING":
                                c.status = "CANCELED"
                        await self._finalize_if_done(pos, exit_price=st["avg_price"],
                                                     reason=reason)
                        break
                else:
                    # dibatalkan pihak lain / expired tanpa fill -> pasang lagi
                    await self._place_exit_orders_locked(pos)

    # ==================================================================
    # EXIT
    # ==================================================================
    async def partial_exit(self, pos: Position, qty: float, price: float,
                           reason: str, fee_quote: float = 0.0) -> None:
        """Catat penjualan SEBAGIAN posisi (partial TP)."""
        qty = min(qty, pos.qty_remaining)
        if qty <= 0:
            return
        pnl = (price - pos.entry_price) * qty - fee_quote
        pos.qty_remaining -= qty
        pos.realized_pnl += pnl
        pos.fees_paid += fee_quote
        self.db.update_trade(
            pos.trade_id, qty_remaining=pos.qty_remaining,
            realized_pnl=round(pos.realized_pnl, 6),
            fees_paid=round(pos.fees_paid, 6))
        self.db.add_trade_event(pos.trade_id, reason.split(" ")[0], price, qty,
                                round(pnl, 6), reason)
        logger.info(f"PARTIAL EXIT #{pos.trade_id} {pos.symbol}: jual {qty:.6f} @ "
                    f"{price:.6f} ({reason}) pnl={pnl:+.2f} "
                    f"sisa={pos.qty_remaining:.6f}")
        await self._finalize_if_done(pos, exit_price=price, reason=reason)

    async def close_position(self, pos: Position, reason: str,
                             fraction: float = 1.0) -> bool:
        """
        Tutup posisi lewat market sell (dipakai SL manual, TP manual,
        tombol dashboard, dan emergency). Return True kalau berhasil.
        """
        if pos.status != "OPEN":
            return True

        async with self._oco_lock:
            # 1. rekonsiliasi dulu: OCO yang barusan terisi harus tercatat
            #    (kalau tidak, fill itu jadi PnL hantu / aset tak terlacak)
            if pos.exit_mode == "oco" and not pos.oco_fallback:
                await self._reconcile_chunks_locked(pos)
            if pos.status != "OPEN":
                return True   # posisi ternyata sudah selesai via OCO

            qty = pos.qty_remaining * min(1.0, max(0.0, fraction))
            filters = self.filters.get(pos.symbol)
            if filters:
                qty = filters.round_qty(qty, market=True)
            if qty <= 0:
                # tidak ada yang bisa dijual -> langsung finalisasi
                await self._finalize_if_done(pos, pos.entry_price, reason, force=True)
                return True

            # 2. batalkan SEMUA OCO posisi ini supaya tidak ikut tereksekusi
            #    (dilakukan dalam lock agar tidak berpacu dengan re-place
            #    dari breakeven/trailing yang berjalan bersamaan)
            if pos.exit_mode == "oco" and not pos.oco_fallback:
                for chunk in pos.chunks:
                    if chunk.oco_list_id:
                        await self.gateway.cancel_oco(pos.symbol, chunk.oco_list_id)
                        chunk.oco_list_id = None
                    chunk.status = "CANCELED" if chunk.status == "PENDING" else chunk.status

            # 3. market sell (qty sudah dihitung dari qty_remaining TERBARU
            #    setelah rekonsiliasi, jadi tidak menjual dua kali)
            try:
                fill = await self.gateway.market_sell(pos.symbol, qty)
            except Exception as exc:
                self.db.record_event("ERROR", "CLOSE_FAILED",
                                     f"{pos.symbol}: {exc}", pos.symbol)
                logger.error(f"Gagal tutup posisi #{pos.trade_id} {pos.symbol}: {exc}")
                return False

            await self.partial_exit(pos, fill.qty, fill.price, reason, fill.fee_quote)
            if fraction >= 1.0:
                await self._finalize_if_done(pos, fill.price, reason, force=True)
            return True

    async def _finalize_if_done(self, pos: Position, exit_price: float,
                                reason: str, force: bool = False) -> None:
        """Tutup record posisi kalau semua chunk sudah selesai / qty habis."""
        all_done = all(c.status != "PENDING" for c in pos.chunks)
        if force or all_done or pos.qty_remaining <= 0:
            if pos.status == "OPEN":
                # jual sisa dust (hasil pembulatan chunk) bila masih layak jual
                if pos.qty_remaining > 0:
                    filters = self.filters.get(pos.symbol)
                    if filters and pos.qty_remaining * exit_price >= filters.min_notional:
                        try:
                            fill = await self.gateway.market_sell(
                                pos.symbol, filters.round_qty(pos.qty_remaining, market=True))
                            if fill.qty > 0:
                                pnl = ((fill.price - pos.entry_price) * fill.qty
                                       - fill.fee_quote)
                                pos.qty_remaining -= fill.qty
                                pos.realized_pnl += pnl
                                pos.fees_paid += fill.fee_quote
                                self.db.update_trade(
                                    pos.trade_id, qty_remaining=pos.qty_remaining,
                                    realized_pnl=round(pos.realized_pnl, 6),
                                    fees_paid=round(pos.fees_paid, 6))
                        except Exception as exc:
                            logger.warning(f"Gagal jual dust {pos.symbol}: {exc}")
                pos.status = "CLOSED"
                pos.exit_time = now_ms()
                pos.exit_price = exit_price
                pos.exit_reason = reason
                self.db.close_trade(
                    pos.trade_id, pos.exit_time, exit_price,
                    pos.realized_pnl, reason, pos.qty_remaining)
                self.db.record_event(
                    "INFO", "CLOSE",
                    f"{pos.symbol}: posisi #{pos.trade_id} ditutup ({reason}) "
                    f"pnl={pos.realized_pnl:+.2f}", pos.symbol)
                logger.info(
                    f"CLOSE #{pos.trade_id} {pos.symbol} @ {exit_price:.6f} "
                    f"({reason}) total_pnl={pos.realized_pnl:+.2f}")
                self.positions.pop(pos.trade_id, None)
                await self.portfolio.refresh(force=True)
