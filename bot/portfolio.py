"""
Portfolio - pelacak saldo & equity real-time.

Equity = saldo quote bebas + saldo quote terkunci + nilai semua posisi terbuka
         (qty tersisa x harga pasar terkini).

Saldo quote di-refresh dari exchange secara berkala (cache 5 detik) supaya
tidak membebani REST API; nilai posisi dihitung dari harga stream terakhir.
"""

from __future__ import annotations

import logging
import time
from bot.config import Config
from bot.exchange.gateway import ExchangeGateway

logger = logging.getLogger("pumpbot.portfolio")


class Portfolio:
    def __init__(self, cfg: Config, gateway: ExchangeGateway):
        self.cfg = cfg
        self.gateway = gateway
        self.quote_free: float = 0.0
        self.quote_locked: float = 0.0
        self.quote_asset = cfg.quote_asset
        self.start_equity: float = 0.0
        self._last_refresh: float = 0.0
        self._refreshing = False

    async def refresh(self, force: bool = False) -> None:
        """Ambil saldo quote terbaru dari exchange (cache 5 detik)."""
        if self._refreshing:
            return
        if not force and time.time() - self._last_refresh < 5.0:
            return
        self._refreshing = True
        try:
            self.quote_free, self.quote_locked = await self.gateway.get_quote_balance()
            self._last_refresh = time.time()
        except Exception as exc:
            logger.warning(f"Gagal refresh saldo: {exc}")
        finally:
            self._refreshing = False

    def positions_value(self, positions: list) -> float:
        """Nilai semua posisi terbuka berdasar qty tersisa x harga."""
        total = 0.0
        for pos in positions:
            price = getattr(pos, "_last_price", 0.0) or pos.entry_price
            total += pos.qty_remaining * price
        return total

    def equity(self, positions: list) -> float:
        """Nilai akun mark-to-market; dipakai khusus untuk daily loss/equity curve."""
        return self.quote_free + self.quote_locked + self.positions_value(positions)

    def sizing_balance(self, positions: list) -> float:
        """Balance acuan risiko tanpa unrealized PnL.

        Quote yang masih tersedia dijumlahkan dengan biaya masuk sisa setiap
        posisi, bukan nilainya pada harga berjalan. Dengan begitu pump/dump
        pada posisi yang sedang terbuka tidak memperbesar atau memperkecil
        risk % entry berikutnya.
        """
        invested_at_cost = sum(
            max(0.0, pos.qty_remaining) * max(0.0, pos.entry_price)
            for pos in positions
        )
        return self.quote_free + self.quote_locked + invested_at_cost

    def ensure_start_equity(self, db) -> float:
        """Catat equity awal sekali (untuk hitung total return & drawdown)."""
        if self.start_equity <= 0:
            stored = db.kv_get("start_equity")
            if stored:
                self.start_equity = float(stored)
        return self.start_equity
