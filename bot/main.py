"""
BotApp - orkestrator seluruh modul (entry: run.py).

Menghubungkan:
  DataCollector -> SignalEngine -> (RiskManager) -> Executor -> PositionManager
  semua dicatat ke Database & ditampilkan lewat Dashboard.

Siklus hidup:
  1. Validasi konfigurasi (dilakukan load_config)
  2. Init database + gateway (Binance / simulator)
  3. Pulihkan posisi terbuka dari database (kalau bot restart)
  4. Watchlist + seed historis + langganan stream
  5. Jalankan: loop signal, loop position manager, loop equity snapshot,
     loop refresh watchlist, dashboard uvicorn
  6. Shutdown rapi (SIGINT/SIGTERM): posisi & OCO di exchange TETAP HIDUP
     dan dipulihkan saat bot dinyalakan lagi.

Error fatal -> notifikasi (Telegram bila dikonfigurasi) + log.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal as os_signal
from bot.config import Config
from bot.data_collector.collector import DataCollector
from bot.database.db import Database
from bot.dashboard import server as dashboard_server
from bot.execution.executor import Executor
from bot.models import ExitChunk, Position, Signal
from bot.portfolio import Portfolio
from bot.position_manager import PositionManager
from bot.risk_management.manager import RiskManager
from bot.signal_engine.engine import SignalEngine
from bot.utils import now_ms, notify

logger = logging.getLogger("pumpbot.main")


class BotApp:
    """Objek aplikasi utama; juga dipakai dashboard sebagai sumber data (ctx)."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.mode = cfg.mode
        self.paused = False
        self.started_at = now_ms()
        self.uvicorn_server = None
        self.idr_rate: float = 0.0
        self.idr_rate_symbol: str = ""
        self.idr_rate_source: str = ""
        self.idr_rate_updated_at: int = 0

        # --- komponen inti ---
        self.db = Database(cfg.database.path)
        logger.info("Database: %s (mode %s)", cfg.database.path, cfg.mode)
        self.gateway = self._build_gateway()
        self.portfolio = Portfolio(cfg, self.gateway)
        self.collector = DataCollector(cfg, self.gateway)
        self.engine = SignalEngine(cfg, self.collector)
        self.risk = RiskManager(cfg)
        self.executor = Executor(cfg, self.gateway, self.db, self.portfolio, self.risk)
        self.posmgr = PositionManager(cfg, self.executor, self.collector)

        # cooldown sinyal dibaca dari database
        self.engine.cooldown_provider = self.db.seconds_since_last_activity

        self._tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

    # ------------------------------------------------------------------
    def _build_dust_sweeper(self):
        """
        Bangun DustSweeper (konversi dust -> BNB) kalau diaktifkan di config.

        Hanya berjalan di mode LIVE: endpoint SAPI (/sapi/v1/asset/dust)
        tidak tersedia di testnet, dan simulator paper tidak punya BNB.
        """
        if not self.cfg.dust_sweep.enabled:
            return None
        if self.mode != "live":
            logger.warning(
                "dust_sweep aktif di config tetapi HANYA berjalan di mode "
                "live (endpoint SAPI tidak tersedia di testnet/paper) "
                "-> fitur dilewati")
            return None
        try:
            # binance-sdk-wallet = SDK modular resmi Binance untuk endpoint
            # wallet/SAPI (dipisah dari binance-sdk-spot). Dipasang lewat
            # requirements.txt; impor lazy supaya mode paper/testnet tetap
            # jalan walau paket tidak terpasang.
            from binance_sdk_wallet import Wallet
            from binance_common.configuration import ConfigurationRestAPI
        except ImportError:
            logger.error("dust_sweep aktif tapi paket binance-sdk-wallet "
                         "tidak terpasang: pip install binance-sdk-wallet")
            return None

        wallet = Wallet(config_rest_api=ConfigurationRestAPI(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
        ))

        async def btc_price() -> float:
            """Harga BTC terhadap quote (dipakai menilai dust dalam USD)."""
            try:
                for t in await self.gateway.get_universe():
                    if t.symbol == "BTC" + self.cfg.quote_asset:
                        return float(t.last_price)
            except Exception:
                pass
            return 0.0

        from bot.dust import DustSweeper
        logger.info("Dust sweep siap (konversi dust ke BNB, interval "
                    f"{self.cfg.dust_sweep.interval_minutes} menit)")
        return DustSweeper(self.cfg, self.db, wallet.rest_api, btc_price)

    # ------------------------------------------------------------------
    def _build_gateway(self):
        if self.cfg.mode == "paper":
            from bot.exchange.simulated_gateway import SimulatedGateway
            return SimulatedGateway(
                start_equity=self.cfg.paper.start_equity,
                symbols=self.cfg.paper.symbols,
                time_scale=self.cfg.paper.time_scale,
                seed=self.cfg.paper.seed,
                quote_asset=self.cfg.quote_asset,
                fee_pct=self.cfg.risk.fee_pct,
                sl_limit_buffer_pct=self.cfg.execution.oco_sl_limit_buffer_pct,
            )
        from bot.exchange.binance_gateway import BinanceGateway
        return BinanceGateway(
            mode=self.cfg.mode,
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            quote_asset=self.cfg.quote_asset,
            sl_limit_buffer_pct=self.cfg.execution.oco_sl_limit_buffer_pct,
            # kedalaman stream orderbook (5/10/20 level) dari config
            depth_levels=self.cfg.data.depth_levels,
        )

    # ------------------------------------------------------------------
    # Kontrol dari dashboard
    # ------------------------------------------------------------------
    def set_paused(self, paused: bool) -> None:
        self.paused = paused
        self.engine.paused = paused
        # position manager TETAP jalan saat pause (posisi harus dikelola)
        self.db.record_event("INFO", "CONTROL",
                              f"bot {'di-PAUSE' if paused else 'di-RESUME'}")
        notify(f"⏸ Bot {'di-pause' if paused else 'resume'}.")

    def apply_runtime_flags(self) -> None:
        """Terapkan parameter live (threshold, trailing, breakeven) ke modul."""
        self.cfg.trailing.enabled = self.risk.params.trailing_enabled
        self.cfg.breakeven.enabled = self.risk.params.breakeven_enabled
        self.engine._override_threshold = self.risk.params.score_threshold

    # ------------------------------------------------------------------
    # Pemulihan posisi setelah restart
    # ------------------------------------------------------------------
    async def _restore_positions(self) -> None:
        rows = self.db.get_open_trades()
        if not rows:
            return
        logger.info(f"Memulihkan {len(rows)} posisi terbuka dari database...")
        for r in rows:
            try:
                tps = json.loads(r["take_profits"] or "[]")
                qty_rem = r["qty_remaining"]
                # bangun ulang chunk dari daftar TP: dibagi rata,
                # chunk TERAKHIR mendapat sisa persis (anti dust)
                chunks: list[ExitChunk] = []
                if tps:
                    per = qty_rem / len(tps)
                    for i, tp in enumerate(tps):
                        if i < len(tps) - 1:
                            chunks.append(ExitChunk(qty=per, tp_price=tp))
                        else:
                            chunks.append(ExitChunk(
                                qty=qty_rem - per * (len(tps) - 1), tp_price=tp))
                pos = Position(
                    trade_id=r["id"], symbol=r["symbol"],
                    entry_time=r["entry_time"], entry_price=r["entry_price"],
                    qty_total=r["qty_total"], qty_remaining=qty_rem,
                    quote_value=r["quote_value"], stop_loss=r["stop_loss"],
                    initial_stop=r["initial_stop"] or r["stop_loss"],
                    take_profits=tps, chunks=chunks,
                    be_triggered=bool(r["be_triggered"]),
                    trail_active=bool(r["trail_active"]),
                    highest_price=r["highest_price"] or r["entry_price"],
                    score=r["score"] or 0, entry_reason=r["entry_reason"] or "",
                    realized_pnl=r["realized_pnl"], fees_paid=r["fees_paid"],
                    exit_mode=r["exit_mode"] or "oco",
                )
                self.executor.positions[pos.trade_id] = pos
                if self.mode != "paper" and pos.exit_mode == "oco":
                    # Order lama di exchange tidak punya ID tersimpan ->
                    # batalkan SEMUA order terbuka simbol ini lalu pasang ulang.
                    # Jika pembatalan gagal, posisi tidak aman untuk dibiarkan
                    # tanpa kepastian OCO, jadi tutup market otomatis.
                    if not await self.gateway.cancel_all_orders(pos.symbol):
                        pos.oco_failure_code = "UNKNOWN"
                        pos.oco_failure_detail = (
                            f"{pos.symbol}: gagal cancel order lama saat restore "
                            "sebelum memasang OCO ulang"
                        )
                        self.db.record_event("ERROR", "OCO_FAIL_UNKNOWN",
                                             pos.oco_failure_detail, pos.symbol)
                        await self.executor._close_after_oco_failure(
                            pos, "restore cancel orders")
            except Exception as exc:
                logger.exception(f"Gagal pulihkan trade #{r['id']}: {exc}")
        # pasang ulang exit order utk semua posisi OCO. Jika gagal, tutup
        # otomatis sesuai kebijakan keamanan (tidak fallback manual).
        for pos in list(self.executor.positions.values()):
            if pos.status == "OPEN" and pos.exit_mode == "oco" and not pos.oco_fallback:
                ok = await self.executor.place_exit_orders(pos, force=True)
                if not ok:
                    await self.executor._close_after_oco_failure(pos, "restore OCO")
        logger.info(f"Pemulihan selesai: {len(self.executor.positions)} posisi aktif")

    # ------------------------------------------------------------------
    # Handler sinyal (dipanggil SignalEngine)
    # ------------------------------------------------------------------
    async def on_signal(self, sig: Signal) -> None:
        self.db.record_signal(sig.symbol, sig.price, sig.score, sig.breakdown)
        if self.paused:
            return
        halted, _ = self.risk.check_daily_limit(
            self.portfolio.equity(list(self.executor.positions.values())))
        if halted:
            return
        await self.executor.try_enter(sig)

    # ==================================================================
    # RUN
    # ==================================================================
    async def run(self) -> None:
        logger.info("=" * 60)
        logger.info(f"PumpBot mulai — mode={self.mode.upper()} "
                    f"quote={self.cfg.quote_asset}")
        logger.info("=" * 60)
        notify(f"🚀 PumpBot mulai (mode {self.mode.upper()})")

        try:
            # seluruh startup + loop utama dibungkus try/finally supaya
            # kegagalan APAPUN (mis. error SDK saat subscribe stream) tetap
            # menjalankan shutdown -> koneksi WS/session ditutup rapi dan
            # notifikasi terkirim, bukan meninggalkan "Unclosed client session"
            await self.gateway.start()
            # Ambil filter lebih awal agar posisi yang dipulihkan bisa langsung
            # dipasangi OCO ulang sebelum universe/watchlist selesai start.
            self.executor.filters = await self.gateway.get_symbol_filters()
            await self._restore_positions()
            await self.collector.start()
            self.executor.filters = getattr(self.collector, "filters", self.executor.filters)

            # equity awal (sekali) + roll hari
            await self.portfolio.refresh(force=True)
            equity = self.portfolio.equity(list(self.executor.positions.values()))
            if not self.portfolio.start_equity:
                self.portfolio.start_equity = equity
                self.db.kv_set("start_equity", str(equity))
                logger.info(f"Equity awal tercatat: {equity:,.2f}")
            self.risk.roll_day_if_needed(equity)

            # ---- loops latar ----
            self._tasks.append(asyncio.create_task(self.engine.run(self.on_signal),
                                                   name="signal-engine"))
            self._tasks.append(asyncio.create_task(self.posmgr.run(),
                                                   name="position-manager"))
            self._tasks.append(asyncio.create_task(self._equity_loop(),
                                                   name="equity-loop"))
            self._tasks.append(asyncio.create_task(self._watchlist_loop(),
                                                   name="watchlist-loop"))
            self._tasks.append(asyncio.create_task(self._daily_roll_loop(),
                                                   name="daily-roll"))
            self._tasks.append(asyncio.create_task(self._idr_rate_loop(),
                                                   name="idr-rate"))

            # ---- dust sweep (opsional, hanya mode live) ----
            sweeper = self._build_dust_sweeper()
            if sweeper:
                self._tasks.append(asyncio.create_task(sweeper.run(),
                                                       name="dust-sweep"))

            # ---- dashboard (blokir sampai shutdown) ----
            dash_task = asyncio.create_task(dashboard_server.run_dashboard(self),
                                            name="dashboard")
            self._tasks.append(dash_task)

            # ---- sinyal OS untuk shutdown rapi ----
            loop = asyncio.get_running_loop()
            for sig in (os_signal.SIGINT, os_signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self._shutdown_event.set)
                except NotImplementedError:
                    pass  # Windows

            stop_monitor = asyncio.create_task(self._shutdown_event.wait(),
                                               name="stop-monitor")
            done, _ = await asyncio.wait(
                {dash_task, stop_monitor},
                return_when=asyncio.FIRST_COMPLETED)
            # kalau dashboard mati sendiri (port bentrok dsb.) -> berhenti juga
            if dash_task in done and not self._shutdown_event.is_set():
                logger.error("Dashboard berhenti tidak terduga (cek port?). "
                             "Mematikan bot...")
                notify("❌ PumpBot berhenti: dashboard gagal jalan (port dipakai?)",
                       level="ERROR")
        except Exception as exc:
            logger.exception("PumpBot berhenti karena ERROR tidak terduga")
            notify(f"❌ PumpBot mati karena error: {exc}", level="ERROR")
            raise
        finally:
            await self.shutdown()

    # ------------------------------------------------------------------
    async def _update_idr_rate(self) -> None:
        """Update kurs quote asset -> IDR untuk tampilan dashboard."""
        try:
            data = await self.gateway.get_quote_idr_rate()
            rate = float(data.get("rate") or 0.0)
            if rate > 0:
                self.idr_rate = rate
                self.idr_rate_symbol = str(data.get("symbol") or "")
                self.idr_rate_source = str(data.get("source") or "Binance market")
                self.idr_rate_updated_at = now_ms()
                logger.info(
                    "Kurs dashboard: 1 %s ≈ %.2f IDR (%s %s)",
                    self.cfg.quote_asset, rate, self.idr_rate_source,
                    self.idr_rate_symbol)
        except Exception as exc:
            logger.debug(f"Update kurs IDR gagal: {exc}")

    async def _idr_rate_loop(self) -> None:
        """Refresh kurs IDR berkala; dipakai dashboard saja."""
        try:
            while True:
                await self._update_idr_rate()
                await asyncio.sleep(5 * 60)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    async def _equity_loop(self) -> None:
        """Snapshot equity tiap 30 detik (dasar equity curve & drawdown)."""
        try:
            while True:
                await self.portfolio.refresh()
                positions = list(self.executor.positions.values())
                equity = self.portfolio.equity(positions)
                self.db.snapshot_equity(
                    equity=equity,
                    available=self.portfolio.quote_free,
                    in_position=self.portfolio.positions_value(positions))
                # catat hal harian ke log
                self.risk.check_daily_limit(equity)
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            pass

    async def _watchlist_loop(self) -> None:
        """Refresh watchlist berkala (tambah simbol baru yang lolos filter)."""
        try:
            while True:
                await asyncio.sleep(self.cfg.universe.refresh_minutes * 60)
                try:
                    if self.mode == "paper":
                        continue  # universe simulator tetap
                    new_symbols = await self.collector.refresh_watchlist()
                    if new_symbols:
                        logger.info(f"Watchlist bertambah: {new_symbols}")
                        self.executor.filters = getattr(self.collector, "filters", {})
                except Exception as exc:
                    logger.warning(f"Refresh watchlist gagal: {exc}")
        except asyncio.CancelledError:
            pass

    async def _daily_roll_loop(self) -> None:
        """Cek pergantian hari (reset halt harian) tiap menit."""
        try:
            while True:
                await self.portfolio.refresh()
                equity = self.portfolio.equity(list(self.executor.positions.values()))
                if self.risk.roll_day_if_needed(equity):
                    logger.info(f"Hari baru: equity awal hari = {equity:,.2f}")
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        """Shutdown rapi. Posisi & OCO di exchange TETAP hidup (dipulihkan
        saat bot dinyalakan lagi)."""
        logger.info("Mematikan bot...")
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await self.engine.stop()
            await self.posmgr.stop()
        except Exception:
            pass
        if self.uvicorn_server:
            self.uvicorn_server.should_exit = True
        try:
            await self.collector.stop()
        except Exception as exc:
            logger.debug(f"stop collector: {exc}")
        self.db.record_event("INFO", "SHUTDOWN", "bot dimatikan")
        self.db.close()
        logger.info("Bot berhenti. (Posisi/OCO yang masih terbuka di exchange "
                    "akan dipulihkan saat bot dinyalakan lagi.)")
        notify("🛑 PumpBot berhenti.")
