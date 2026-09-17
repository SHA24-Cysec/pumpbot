"""
Rolling buffer per simbol di memori.

Menyimpan data real-time beberapa menit terakhir untuk dianalisis SignalEngine:
  * candles   : candle 1m yang sudah CLOSE (maxlen besar, di-seed dari REST)
  * trades    : trade mentah (rolling window / maxlen)
  * book      : snapshot order book terakhir + riwayat singkat (deteksi spoof)
  * ticker    : statistik 24 jam terakhir

Semua akses terjadi di satu event loop asyncio sehingga tidak perlu lock.
"""

from __future__ import annotations

from collections import deque
from typing import Optional

from bot.models import BookSnapshot, Candle, Ticker24h, Trade
from bot.utils import now_ms


class SymbolBuffer:
    """Buffer data satu simbol."""

    def __init__(self, symbol: str, max_candles: int = 720,
                 max_trades: int = 5000, trade_window_sec: int = 600,
                 book_history_sec: int = 60):
        self.symbol = symbol
        self.candles: deque[Candle] = deque(maxlen=max_candles)
        self.trades: deque[Trade] = deque(maxlen=max_trades)
        self.book: Optional[BookSnapshot] = None
        self.book_history: deque[BookSnapshot] = deque(maxlen=book_history_sec * 2)
        self.ticker: Optional[Ticker24h] = None
        self._trade_window_ms = trade_window_sec * 1000
        self._last_price: float = 0.0
        self.last_update: int = 0

    # ------------------------------------------------------------ writers
    def on_candle(self, candle: Candle) -> None:
        """Dipanggil untuk event kline. Simpan yang sudah close, update
        candle berjalan sebagai referensi volume berjalan."""
        if candle.closed:
            self.candles.append(candle)
        if candle.close > 0:
            self._last_price = candle.close
        self.last_update = now_ms()

    def on_trade(self, trade: Trade) -> None:
        self.trades.append(trade)
        self._last_price = trade.price
        self.last_update = now_ms()
        # buang trade di luar window (hemat memori, selain maxlen)
        cutoff = trade.ts - self._trade_window_ms
        while self.trades and self.trades[0].ts < cutoff:
            self.popleft_trade()

    def on_book(self, book: BookSnapshot) -> None:
        self.book = book
        # simpan max 1 snapshot per detik untuk riwayat spoofing
        if self.book_history and self.book_history[-1].ts == book.ts:
            self.book_history[-1] = book
        else:
            self.book_history.append(book)
        self.last_update = now_ms()

    def on_ticker(self, ticker: Ticker24h) -> None:
        self.ticker = ticker
        if ticker.last_price > 0:
            self._last_price = ticker.last_price
        self.last_update = now_ms()

    def popleft_trade(self) -> None:
        try:
            self.trades.popleft()
        except IndexError:
            pass

    # ------------------------------------------------------------ readers
    @property
    def last_price(self) -> float:
        return self._last_price

    def trades_since(self, ts_ms: int) -> list[Trade]:
        """Trade dengan timestamp >= ts_ms (pencarian linier dari belakang)."""
        out = []
        for t in reversed(self.trades):
            if t.ts < ts_ms:
                break
            out.append(t)
        out.reverse()
        return out

    def ready(self, min_candles: int) -> bool:
        """Cukup data untuk dianalisis?"""
        return len(self.candles) >= min_candles and self.book is not None
