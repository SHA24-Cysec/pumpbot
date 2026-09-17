"""
DataCollector - jembatan antara gateway (Binance WS / simulator) dan buffer.

Tugas:
  1. Menentukan watchlist: ambil daftar simbol dari exchangeInfo + filter
     volume 24 jam (otomatis, bisa dioverride via config include_symbols).
  2. Seed candle historis via REST supaya detector punya baseline segera.
  3. Berlangganan stream (kline, aggTrade, depth, ticker) dan mengisi buffer.
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Optional

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.exchange.gateway import ExchangeGateway
from bot.models import Ticker24h

logger = logging.getLogger("pumpbot.collector")

# base asset yang menandakan pair stabilcoin/forex (tidak relevan utk pump)
_STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "AEUR", "EUR", "GBP",
    "AUD", "TRY", "BRL", "ARS", "JPY", "RUB", "UAH", "PLN", "RON", "ZAR", "XUSD",
}
_LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


class DataCollector:
    def __init__(self, cfg: Config, gateway: ExchangeGateway):
        self.cfg = cfg
        self.gateway = gateway
        self.buffers: dict[str, SymbolBuffer] = {}
        self.watchlist: list[str] = []
        self.ws_alive = False

    # ------------------------------------------------------------------
    # Pemilihan watchlist
    # ------------------------------------------------------------------
    def _is_excluded(self, t: Ticker24h) -> bool:
        u = self.cfg.universe
        symbol = t.symbol
        if symbol in u.exclude_symbols:
            return True
        # simbol harus berakhiran quote asset (mis. "...USDT")
        if not symbol.endswith(self.cfg.quote_asset):
            return True
        base = symbol[: -len(self.cfg.quote_asset)]
        if not base:
            return True
        if u.exclude_stable_pairs and base in _STABLE_BASES:
            return True
        if u.exclude_leveraged_tokens and base.endswith(_LEVERAGED_SUFFIXES):
            return True
        if t.quote_volume < u.min_quote_volume_24h:
            return True
        # pair yang mati total dalam 24 jam (count=0) tidak berguna
        if t.trade_count <= 0:
            return True
        return False

    async def build_watchlist(self, filters: dict) -> list[str]:
        """Pilih simbol yang dipantau berdasarkan volume & filter config."""
        u = self.cfg.universe
        if u.include_symbols:
            candidates = [s.upper() for s in u.include_symbols]
            logger.info(f"Watchlist manual dari config: {candidates}")
            return candidates

        tickers = await self.gateway.get_universe()
        eligible = [t for t in tickers if not self._is_excluded(t)]
        # urutkan berdasarkan volume 24 jam terbesar
        eligible.sort(key=lambda t: t.quote_volume, reverse=True)
        chosen = [t.symbol for t in eligible[: u.max_symbols]]
        logger.info(
            f"Watchlist otomatis: {len(chosen)} simbol "
            f"(dari {len(tickers)} total, {len(eligible)} lolos filter volume "
            f">= {u.min_quote_volume_24h:,.0f})"
        )
        return chosen

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------
    async def start(self) -> None:
        """Tentukan watchlist, buat buffer, seed historis, langganan stream."""
        filters = await self.gateway.get_symbol_filters()
        self.filters = filters
        self.watchlist = await self.build_watchlist(filters)

        d = self.cfg.data
        for sym in self.watchlist:
            self.buffers[sym] = SymbolBuffer(
                sym,
                max_candles=max(720, d.history_candles),
                max_trades=d.trade_buffer_max,
                trade_window_sec=d.trade_window_sec,
                book_history_sec=d.book_history_sec,
            )

        # Seed candle historis (REST) paralel supaya cepat.
        logger.info(f"Seed {d.history_candles} candle historis untuk {len(self.watchlist)} simbol...")
        sem = asyncio.Semaphore(5)   # batasi konkurensi, hormati rate limit

        async def seed(sym: str):
            async with sem:
                try:
                    candles = await self.gateway.get_klines(
                        sym, d.kline_interval, d.history_candles)
                    buf = self.buffers[sym]
                    for c in candles:
                        buf.on_candle(c)
                except Exception as exc:
                    logger.warning(f"Gagal seed kline {sym}: {exc}")

        await asyncio.gather(*(seed(s) for s in self.watchlist))
        seeded = sum(1 for b in self.buffers.values() if len(b.candles) >= self.cfg.signal.min_candles)
        logger.info(f"Seed selesai: {seeded}/{len(self.watchlist)} simbol punya >= "
                    f"{self.cfg.signal.min_candles} candle")

        # Langganan stream real-time.
        await self.gateway.subscribe(
            symbols=self.watchlist,
            on_candle=self._on_candle,
            on_trade=self._on_trade,
            on_book=self._on_book,
            on_ticker=self._on_ticker,
        )
        self.ws_alive = True
        logger.info("DataCollector aktif - stream real-time berjalan.")

    async def stop(self) -> None:
        await self.gateway.stop()
        self.ws_alive = False

    # ------------------------------------------------------------------
    # Refresh berkala (dipanggil BotApp tiap universe.refresh_minutes)
    # ------------------------------------------------------------------
    async def refresh_watchlist(self) -> list[str]:
        """
        Perbarui watchlist: berlangganan simbol BARU yang lolos filter.
        (Add-only: simbol lama tetap dipantau sampai bot restart supaya
        buffer analisisnya tidak terbuang mendadak.)
        """
        filters = await self.gateway.get_symbol_filters()
        self.filters = filters
        new_list = await self.build_watchlist(filters)
        added = [s for s in new_list if s not in self.buffers]
        if not added:
            return []

        d = self.cfg.data
        for sym in added:
            self.buffers[sym] = SymbolBuffer(
                sym, max_candles=max(720, d.history_candles),
                max_trades=d.trade_buffer_max,
                trade_window_sec=d.trade_window_sec,
                book_history_sec=d.book_history_sec)
            self.watchlist.append(sym)

        # seed historis simbol baru
        sem = asyncio.Semaphore(3)

        async def seed(sym: str):
            async with sem:
                try:
                    candles = await self.gateway.get_klines(
                        sym, d.kline_interval, d.history_candles)
                    for c in candles:
                        self.buffers[sym].on_candle(c)
                except Exception as exc:
                    logger.warning(f"Gagal seed kline {sym}: {exc}")

        await asyncio.gather(*(seed(s) for s in added))
        # SDK otomatis melewati stream yang sudah dilanggan sebelumnya
        await self.gateway.subscribe(
            symbols=added,
            on_candle=self._on_candle, on_trade=self._on_trade,
            on_book=self._on_book, on_ticker=self._on_ticker)
        logger.info(f"Watchlist refresh: +{len(added)} simbol baru: {added}")
        return added

    # ------------------------------------------------------------------
    # Callback stream (SYNC & ringan: hanya tulis buffer)
    # ------------------------------------------------------------------
    def _on_candle(self, symbol: str, candle) -> None:
        buf = self.buffers.get(symbol)
        if buf:
            buf.on_candle(candle)

    def _on_trade(self, symbol: str, trade) -> None:
        buf = self.buffers.get(symbol)
        if buf:
            buf.on_trade(trade)

    def _on_book(self, symbol: str, book) -> None:
        buf = self.buffers.get(symbol)
        if buf:
            buf.on_book(book)

    def _on_ticker(self, symbol: str, ticker) -> None:
        buf = self.buffers.get(symbol)
        if buf:
            buf.on_ticker(ticker)

    # ------------------------------------------------------------------
    # Helper pembacaan untuk modul lain
    # ------------------------------------------------------------------
    def last_price(self, symbol: str) -> float:
        buf = self.buffers.get(symbol)
        return buf.last_price if buf else 0.0

    def buffer(self, symbol: str) -> Optional[SymbolBuffer]:
        return self.buffers.get(symbol)
