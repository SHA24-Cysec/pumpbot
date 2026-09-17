"""
Model data inti yang dipakai lintas modul.

Semua timestamp memakai epoch MILLISECONDS (konvensi Binance).
Semua harga/kuantitas bertipe float, tapi sebelum dikirim ke exchange
selalu dibulatkan sesuai filter (tick_size / step_size) lewat SymbolFilters.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


# ============================================================================
# DATA PASAR
# ============================================================================

@dataclass
class Candle:
    """Satu candle/kline (interval dari config, default 1m)."""
    open_time: int          # epoch ms
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float           # volume base asset
    quote_volume: float     # volume quote (USDT)
    trades: int             # jumlah trade dalam candle ini
    taker_buy_volume: float  # volume yang di-INITIATE pembeli (agresor beli)
    closed: bool = True     # False = candle yang sedang berjalan


@dataclass
class Trade:
    """Satu eksekusi trade (dari aggTrade stream)."""
    ts: int                 # epoch ms
    price: float
    qty: float
    buyer_is_maker: bool    # True = agresornya PENJUAL (seller-initiated / market sell)


@dataclass
class BookSnapshot:
    """Snapshot order book (partial depth, N level)."""
    ts: int
    bids: list[tuple[float, float]]   # [(harga, qty), ...] terurut TURUN
    asks: list[tuple[float, float]]   # [(harga, qty), ...] terurut NAIK

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) / 2.0
        return None

    @property
    def spread_bps(self) -> Optional[float]:
        """Spread bid-ask dalam basis point (1 bps = 0.01%)."""
        if self.bids and self.asks:
            mid = self.mid
            if mid and mid > 0:
                return (self.asks[0][0] - self.bids[0][0]) / mid * 10000.0
        return None


@dataclass
class Ticker24h:
    """Ringkasan statistik 24 jam (dari ticker stream / REST)."""
    ts: int
    symbol: str
    last_price: float
    price_change_pct: float   # perubahan 24 jam dalam persen
    high: float
    low: float
    volume: float
    quote_volume: float
    trade_count: int
    bid: float
    ask: float


# ============================================================================
# FILTER EXCHANGE (aturan LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL)
# ============================================================================

@dataclass
class SymbolFilters:
    """
    Aturan harga/qty simbol yang diambil dari exchangeInfo.

    Dipakai Executor supaya order tidak ditolak Binance:
      - harga harus kelipatan tick_size dan dalam [min_price, max_price]
      - qty harus kelipatan step_size dan dalam [min_qty, max_qty]
      - nilai order (qty x harga) harus >= min_notional
    """
    symbol: str
    tick_size: float = 0.0001
    min_price: float = 0.0
    max_price: float = math.inf
    step_size: float = 0.0001
    min_qty: float = 0.0
    max_qty: float = math.inf
    # Filter khusus order MARKET (kadang lebih ketat dari LOT_SIZE biasa)
    market_step_size: Optional[float] = None
    market_min_qty: Optional[float] = None
    market_max_qty: Optional[float] = None
    min_notional: float = 5.0

    def round_price(self, price: float, mode: str = "nearest") -> float:
        """Bulatkan harga ke kelipatan tick_size. mode: down|up|nearest."""
        if self.tick_size <= 0:
            return price
        steps = price / self.tick_size
        if mode == "down":
            steps = math.floor(steps)
        elif mode == "up":
            steps = math.ceil(steps)
        else:
            steps = round(steps)
        return round(steps * self.tick_size, 12)

    def round_qty(self, qty: float, market: bool = False) -> float:
        """Bulatkan qty KE BAWAH ke kelipatan step_size (floor agar tidak
        melebihi saldo/limit)."""
        step = self.step_size
        if market and self.market_step_size:
            step = max(step, self.market_step_size)
        if step <= 0:
            return qty
        return math.floor(qty / step) * step

    @property
    def price_decimals(self) -> int:
        """Jumlah desimal harga untuk ditampilkan di dashboard."""
        d = round(-math.log10(self.tick_size)) if self.tick_size > 0 else 2
        return max(0, min(10, int(d)))

    @property
    def qty_decimals(self) -> int:
        d = round(-math.log10(self.step_size)) if self.step_size > 0 else 2
        return max(0, min(10, int(d)))


# ============================================================================
# HASIL DETECTOR & SINYAL
# ============================================================================

@dataclass
class DetectorResult:
    """Output satu detector. score 0-100 (semakin tinggi semakin bullis)."""
    name: str
    score: float = 0.0
    eligible: bool = True    # False = gate keras (mis. spread kelewat lebar)
    veto: bool = False       # True = larang entry total (mis. manipulasi jelas)
    details: dict = field(default_factory=dict)


@dataclass
class Signal:
    """Sinyal final dari SignalEngine yang layak eksekusi."""
    ts: int
    symbol: str
    price: float                 # harga saat sinyal
    score: float                 # skor gabungan 0-100
    breakdown: dict              # skor per detector (untuk dashboard & log)
    suggested_stop: float        # usulan SL awal (dari struktur/percent)
    entry_type: str = "breakout"  # breakout | pullback
    reason: str = ""


# ============================================================================
# EKSEKUSI
# ============================================================================

@dataclass
class Fill:
    """Hasil eksekusi order."""
    symbol: str
    price: float          # harga rata-rata eksekusi
    qty: float            # qty base yang terisi
    quote_qty: float      # nilai quote
    fee_quote: float      # fee dikonversi ke quote (estimasi)
    order_id: int = 0


@dataclass
class ExitChunk:
    """Satu 'potongan' posisi untuk partial take profit.

    Dalam mode OCO, tiap chunk punya order OCO sendiri
    (TP sesuai target chunk, SL sama untuk semua chunk).
    """
    qty: float
    tp_price: float
    oco_list_id: Optional[int] = None
    status: str = "PENDING"     # PENDING | FILLED | CANCELED


@dataclass
class Position:
    """Posisi terbuka (long, spot)."""
    trade_id: int
    symbol: str
    entry_time: int
    entry_price: float
    qty_total: float          # qty awal
    qty_remaining: float      # sisa qty yang belum dijual
    quote_value: float        # nilai beli awal (quote)
    stop_loss: float          # SL aktif saat ini (bisa berpindah: BE/trailing)
    initial_stop: float       # SL awal (untuk hitung risiko terpakai)
    take_profits: list[float] # daftar harga TP (urut naik)
    chunks: list[ExitChunk] = field(default_factory=list)
    be_triggered: bool = False
    trail_active: bool = False
    highest_price: float = 0.0
    score: float = 0.0
    entry_reason: str = ""
    realized_pnl: float = 0.0
    fees_paid: float = 0.0
    exit_mode: str = "oco"    # oco | manual
    oco_fallback: bool = False  # True jika OCO gagal & posisi dikelola manual
    status: str = "OPEN"      # OPEN | CLOSED
    exit_time: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    last_oco_sync: int = 0    # epoch ms terakhir OCO di-replace

    @property
    def current_risk_quote(self) -> float:
        """Rugi (quote) jika SL kena sekarang, berdasar sisa qty."""
        if self.stop_loss >= self.entry_price:  # sudah breakeven / profit
            return 0.0
        return max(0.0, self.qty_remaining * (self.entry_price - self.stop_loss))

    def unrealized_pnl(self, price: float) -> float:
        """Profit mengambang dari sisa posisi (belum termasuk realized)."""
        return (price - self.entry_price) * self.qty_remaining

    def unrealized_pnl_pct(self, price: float) -> float:
        if self.entry_price <= 0:
            return 0.0
        return (price - self.entry_price) / self.entry_price * 100.0

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "entry_time": self.entry_time,
            "entry_price": self.entry_price,
            "qty_total": self.qty_total,
            "qty_remaining": self.qty_remaining,
            "quote_value": self.quote_value,
            "stop_loss": self.stop_loss,
            "initial_stop": self.initial_stop,
            "take_profits": self.take_profits,
            "be_triggered": self.be_triggered,
            "trail_active": self.trail_active,
            "highest_price": self.highest_price,
            "score": self.score,
            "entry_reason": self.entry_reason,
            "realized_pnl": self.realized_pnl,
            "status": self.status,
            "exit_mode": self.exit_mode + ("(fallback)" if self.oco_fallback else ""),
        }
