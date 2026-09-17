"""
Abstraksi akses exchange (ExchangeGateway).

Ada dua implementasi:
  - BinanceGateway    : pakai SDK resmi `binance-sdk-spot` (REST + WebSocket Streams)
  - SimulatedGateway  : simulator pasar + paper trading (mode `paper`)

Dengan interface yang sama, seluruh bot (collector, executor, position manager)
tidak perlu tahu sedang jalan di mode apa - memudahkan pengujian tanpa risiko.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Awaitable, Callable, Optional

from bot.models import BookSnapshot, Candle, Fill, SymbolFilters, Ticker24h, Trade

# Tipe callback yang dipasang collector ke stream.
# Semua callback bersifat SYNC dan HARUS cepat (micro-detik): hanya menulis
# ke buffer memori. Pemrosesan berat dilakukan oleh task asyncio terpisah.
OnCandle = Callable[[str, Candle], None]
OnTrade = Callable[[str, Trade], None]
OnBook = Callable[[str, BookSnapshot], None]
OnTicker = Callable[[str, Ticker24h], None]


class ExchangeGateway(ABC):
    """Kontrak yang harus dipenuhi setiap implementasi exchange."""

    mode: str = ""   # "testnet" | "live" | "paper"

    # -------------------------------------------------------------- lifecycle
    @abstractmethod
    async def start(self) -> None:
        """Inisialisasi koneksi/sesi."""

    @abstractmethod
    async def stop(self) -> None:
        """Tutup semua koneksi dengan rapi."""

    # -------------------------------------------------------------- info pasar
    @abstractmethod
    async def get_symbol_filters(self) -> dict[str, SymbolFilters]:
        """Ambil aturan filter (LOT_SIZE, PRICE_FILTER, NOTIONAL) semua simbol."""

    @abstractmethod
    async def get_universe(self) -> list[Ticker24h]:
        """
        Statistik 24 jam semua simbol quote (untuk pemilihan watchlist).
        Diimplementasikan via endpoint ticker 24hr (dengan cache menit-menitan
        agar hemat weight API).
        """

    @abstractmethod
    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        """Ambil candle historis (untuk baseline detector saat startup)."""

    # -------------------------------------------------------------- streaming
    @abstractmethod
    async def subscribe(
        self,
        symbols: list[str],
        on_candle: OnCandle,
        on_trade: OnTrade,
        on_book: OnBook,
        on_ticker: OnTicker,
    ) -> None:
        """Berlangganan stream: kline, aggTrade, partial depth, ticker 24h."""

    # -------------------------------------------------------------- akun
    @abstractmethod
    async def get_quote_balance(self) -> tuple[float, float]:
        """Saldo quote asset -> (free, locked) dalam satuan quote."""

    # -------------------------------------------------------------- trading
    @abstractmethod
    async def market_buy(self, symbol: str, quote_qty: float) -> Fill:
        """Beli market sebesar `quote_qty` (mis. 150 USDT)."""

    @abstractmethod
    async def market_sell(self, symbol: str, qty: float) -> Fill:
        """Jual market sebanyak `qty` base asset."""

    @abstractmethod
    async def place_limit_buy(self, symbol: str, qty: float, price: float) -> int:
        """Pasang limit buy. Return order_id."""

    @abstractmethod
    async def get_order_status(self, symbol: str, order_id: int) -> dict:
        """
        Status order -> {"status", "executed_qty", "avg_price", "quote_qty"}.
        status mengikuti konvensi Binance: NEW/FILLED/CANCELED/PARTIALLY_FILLED...
        """

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        """Batalkan order. Return True kalau berhasil (atau memang sudah hilang)."""

    @abstractmethod
    async def cancel_all_orders(self, symbol: str) -> bool:
        """
        Batalkan SEMUA order terbuka (limit + OCO) untuk satu simbol.
        Dipakai saat pemulihan posisi setelah restart, saat ID order lama
        tidak tersimpan dan lebih aman memasang ulang dari nol.
        """

    # -------------------------------------------------------------- OCO
    @abstractmethod
    async def place_oco_sell(
        self, symbol: str, qty: float, tp_price: float, stop_price: float
    ) -> int:
        """
        Pasang OCO sell: TP limit @tp_price + SL stop-limit @stop_price.
        Return order_list_id. Stop-limit price dihitung implementasi
        (sedikit di bawah stop_price, buffer dari config).
        """

    @abstractmethod
    async def cancel_oco(self, symbol: str, order_list_id: int) -> bool:
        """Batalkan satu order list (OCO)."""

    @abstractmethod
    async def get_oco_status(self, symbol: str, order_list_id: int) -> dict:
        """
        Status OCO -> {
            "list_status": EXECUTED|ALL_DONE|EXECUTING|CANCELLED...,
            "done": bool,              # true jika semua leg selesai (terisi/batal)
            "filled_qty": float,       # total qty base yang terisi
            "avg_price": float,        # harga rata-rata fill
            "filled_quote": float,
            "which": str,              # "tp" | "sl" | "" leg mana yang terisi
        }
        """
