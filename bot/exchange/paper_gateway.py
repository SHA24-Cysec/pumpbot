"""
PaperGateway - AKUN DEMO PumpBot (mode `paper`).

Konsep: data pasar NYATA, uang PALSU.

  * HARGA, VOLUME, ORDER BOOK, dan TRADE diambil langsung dari Binance Spot
    produksi lewat endpoint market data PUBLIK yang resmi:
      REST : https://data-api.binance.vision
      WS   : wss://data-stream.binance.vision
    Kedua domain ini TIDAK memerlukan API key sama sekali dan hanya melayani
    data pasar publik (user data stream tidak tersedia di sana).
    Rujukan resmi: Binance Spot API docs, halaman "Market Data Only".

  * SALDO, ORDER, dan POSISI sepenuhnya disimulasikan di dalam proses ini.
    Tidak ada satu pun request yang menyentuh endpoint bertanda tangan
    (SIGNED/TRADE), jadi secara teknis MUSTAHIL bot ini mengirim order
    sungguhan saat mode paper - tidak ada API key, tidak ada signature.

Perbedaan dengan Binance Testnet yang digantikan:
  * Testnet punya buku order sendiri yang sepi dan harga yang menyimpang jauh
    dari pasar nyata, sehingga hasil uji strategi pump TIDAK bisa dipercaya.
  * Testnet mewajibkan API key dan datanya direset berkala.
  * Paper mode memakai pergerakan harga asli, jadi sinyal pump yang terdeteksi
    adalah pump yang benar-benar terjadi di pasar.

Realisme eksekusi yang ditiru:
  * Beli memakai best ASK, jual memakai best BID (bukan harga tengah).
  * Slippage taker (`paper.slippage_bps`) ditambahkan ke arah yang merugikan.
  * Fee taker (`risk.fee_pct`): beli dipotong dari base asset, jual dipotong
    dari quote - sama seperti perilaku Binance Spot tanpa diskon BNB.
  * Aset base DIKUNCI saat OCO terpasang dan dilepas saat OCO dibatalkan,
    meniru locked balance Binance.
  * Order OCO/limit hanya terisi bila harga pasar SUNGGUHAN menyentuhnya.

Saldo demo bertahan lintas restart: state ditulis ke tabel `kv` database mode
paper lewat `state_store`. Tanpa ini, akun demo akan lupa hasil tradingnya
setiap bot dinyalakan ulang sementara histori trade di database tetap ada -
equity curve dan statistik jadi tidak konsisten.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Protocol

from bot.exchange.binance_gateway import BinanceGateway
from bot.exchange.gateway import ExchangeGateway
from bot.models import Fill, SymbolFilters, Ticker24h

logger = logging.getLogger("pumpbot.paper")

# Versi skema state; dinaikkan bila struktur dict state berubah agar state
# lama dari versi bot sebelumnya bisa diabaikan dengan aman.
STATE_VERSION = 1


class PaperStateStore(Protocol):
    """Penyimpan state akun demo (diimplementasikan main.py di atas tabel kv)."""

    def load(self) -> Optional[dict]:
        """Kembalikan state tersimpan, atau None bila belum ada."""

    def save(self, state: dict) -> None:
        """Simpan state akun demo."""


class _Quote:
    """Harga pasar terakhir satu simbol (diisi dari stream publik)."""

    __slots__ = ("last", "bid", "ask", "ts")

    def __init__(self) -> None:
        self.last = 0.0
        self.bid = 0.0
        self.ask = 0.0
        self.ts = 0


class PaperGateway(ExchangeGateway):
    """Akun demo: market data Binance asli + buku order dan saldo virtual."""

    mode = "paper"

    # Jeda loop pemantau order simulasi. 0.2 dtk cukup rapat untuk stream
    # aggTrade Binance (puluhan event per detik pada pair likuid) tanpa
    # membebani CPU.
    ORDER_WATCH_INTERVAL_S = 0.2

    def __init__(
        self,
        quote_asset: str = "USDT",
        start_balance: float = 10_000.0,
        fee_pct: float = 0.1,
        slippage_bps: float = 2.0,
        sl_limit_buffer_pct: float = 0.3,
        depth_levels: int = 10,
        state_store: Optional[PaperStateStore] = None,
    ):
        if start_balance <= 0:
            raise ValueError("paper.start_balance harus > 0")

        self.quote_asset = quote_asset.upper()
        self.start_balance = float(start_balance)
        self.fee_rate = float(fee_pct) / 100.0
        self.slippage_rate = float(slippage_bps) / 10_000.0
        self._sl_limit_buffer_pct = float(sl_limit_buffer_pct)
        self._state_store = state_store

        # Sumber data pasar: BinanceGateway dalam mode paper -> otomatis
        # memakai URL market data publik dan TANPA API key.
        self._market = BinanceGateway(
            mode="paper",
            api_key="",
            api_secret="",
            quote_asset=self.quote_asset,
            sl_limit_buffer_pct=sl_limit_buffer_pct,
            depth_levels=depth_levels,
        )

        # --- dompet virtual ---
        self.balance_quote = self.start_balance
        self.base_free: dict[str, float] = {}      # per SIMBOL (mis. BTCUSDT)
        self.base_locked: dict[str, float] = {}    # dikunci oleh OCO terpasang

        # --- buku order virtual ---
        self._next_id = 1_000_000
        self._limit_orders: dict[int, dict] = {}
        self._oco_orders: dict[int, dict] = {}

        # --- harga pasar terakhir dari stream publik ---
        self._quotes: dict[str, _Quote] = {}

        self._watcher: Optional[asyncio.Task] = None
        self._running = False
        self._restored = False

        self._load_state()

        logger.info(
            "PaperGateway (AKUN DEMO) siap: saldo %s %.2f | fee %.3f%% | "
            "slippage %.1f bps | data pasar REAL dari Binance publik",
            self.quote_asset, self.balance_quote, fee_pct, slippage_bps,
        )

    # ==================================================================
    # STATE (bertahan lintas restart)
    # ==================================================================
    def state(self) -> dict:
        """Snapshot dompet + order terbuka untuk disimpan ke database."""
        return {
            "version": STATE_VERSION,
            "quote_asset": self.quote_asset,
            "start_balance": self.start_balance,
            "balance_quote": self.balance_quote,
            "base_free": dict(self.base_free),
            "base_locked": dict(self.base_locked),
            "next_id": self._next_id,
        }

    def _load_state(self) -> None:
        if not self._state_store:
            return
        try:
            raw = self._state_store.load()
        except Exception as exc:
            logger.warning("Gagal membaca state akun demo: %s", exc)
            return
        if not isinstance(raw, dict) or raw.get("version") != STATE_VERSION:
            return
        if raw.get("quote_asset") != self.quote_asset:
            logger.warning(
                "State akun demo memakai quote %s, config sekarang %s -> "
                "state lama diabaikan dan saldo dimulai ulang.",
                raw.get("quote_asset"), self.quote_asset)
            return
        try:
            self.balance_quote = float(raw["balance_quote"])
            self.base_free = {str(k): float(v)
                              for k, v in (raw.get("base_free") or {}).items()}
            # Aset yang tadinya terkunci OCO dibebaskan: order OCO virtual
            # tidak ikut disimpan, jadi tidak ada lagi yang menguncinya.
            recovered = {str(k): float(v)
                         for k, v in (raw.get("base_locked") or {}).items()}
            for sym, qty in recovered.items():
                self.base_free[sym] = self.base_free.get(sym, 0.0) + qty
            self.base_locked = {}
            self._next_id = max(int(raw.get("next_id", self._next_id)),
                                self._next_id)
            self.start_balance = float(raw.get("start_balance",
                                               self.start_balance))
            self._restored = True
            logger.info("State akun demo dipulihkan: saldo %s %.2f, %d aset base",
                        self.quote_asset, self.balance_quote,
                        len([v for v in self.base_free.values() if v > 0]))
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("State akun demo rusak (%s) -> mulai dari saldo awal.",
                           exc)
            self.balance_quote = self.start_balance
            self.base_free, self.base_locked = {}, {}

    def _persist(self) -> None:
        if not self._state_store:
            return
        try:
            self._state_store.save(self.state())
        except Exception as exc:
            logger.warning("Gagal menyimpan state akun demo: %s", exc)

    @property
    def restored_from_state(self) -> bool:
        """True bila saldo dibaca dari state tersimpan, bukan saldo awal baru."""
        return self._restored

    # ==================================================================
    # LIFECYCLE
    # ==================================================================
    async def start(self) -> None:
        await self._market.start()
        self._running = True
        self._watcher = asyncio.create_task(self._run_order_watcher(),
                                            name="paper-orders")
        logger.info("Akun demo aktif - order dieksekusi terhadap harga pasar "
                    "Binance sungguhan, tetapi seluruh dana bersifat virtual.")

    async def stop(self) -> None:
        self._running = False
        if self._watcher:
            self._watcher.cancel()
            try:
                await self._watcher
            except (asyncio.CancelledError, Exception):
                pass
            self._watcher = None
        self._persist()
        await self._market.stop()

    # ==================================================================
    # INFO PASAR (diteruskan ke Binance publik)
    # ==================================================================
    async def get_symbol_filters(self) -> dict[str, SymbolFilters]:
        return await self._market.get_symbol_filters()

    async def get_universe(self) -> list[Ticker24h]:
        tickers = await self._market.get_universe()
        # Pakai sekalian untuk menyegarkan harga acuan simbol yang belum
        # punya stream (mis. tepat setelah restart, sebelum WS mengalir).
        for t in tickers:
            q = self._quotes.setdefault(t.symbol, _Quote())
            if t.last_price > 0:
                q.last = t.last_price
                q.ts = t.ts
            if t.bid > 0:
                q.bid = t.bid
            if t.ask > 0:
                q.ask = t.ask
        return tickers

    async def get_klines(self, symbol: str, interval: str, limit: int):
        return await self._market.get_klines(symbol, interval, limit)

    async def get_quote_idr_rate(self) -> dict:
        return await self._market.get_quote_idr_rate()

    # ==================================================================
    # STREAMING (callback dibungkus agar harga pasar ikut terekam)
    # ==================================================================
    async def subscribe(self, symbols, on_candle, on_trade, on_book,
                        on_ticker) -> None:
        """
        Langganan stream publik Binance.

        Callback asli bot tetap dipanggil apa adanya; pembungkus di sini hanya
        mencatat harga terakhir/bid/ask per simbol supaya order virtual bisa
        diisi memakai harga pasar yang sama dengan yang dilihat detector.
        Pembungkus sengaja dibuat sangat ringan (hanya tulis atribut) karena
        callback SDK bersifat sync dan tidak boleh memblokir.
        """

        def wrap_candle(sym, candle):
            q = self._quotes.setdefault(sym, _Quote())
            if candle.close > 0:
                q.last = candle.close
                q.ts = candle.close_time
            on_candle(sym, candle)

        def wrap_trade(sym, trade):
            q = self._quotes.setdefault(sym, _Quote())
            if trade.price > 0:
                q.last = trade.price
                q.ts = trade.ts
            on_trade(sym, trade)

        def wrap_book(sym, book):
            q = self._quotes.setdefault(sym, _Quote())
            if book.bids:
                q.bid = book.bids[0][0]
            if book.asks:
                q.ask = book.asks[0][0]
            on_book(sym, book)

        def wrap_ticker(sym, ticker):
            q = self._quotes.setdefault(sym, _Quote())
            if ticker.last_price > 0:
                q.last = ticker.last_price
                q.ts = ticker.ts
            if ticker.bid > 0:
                q.bid = ticker.bid
            if ticker.ask > 0:
                q.ask = ticker.ask
            on_ticker(sym, ticker)

        await self._market.subscribe(symbols, wrap_candle, wrap_trade,
                                     wrap_book, wrap_ticker)

    # ==================================================================
    # HARGA ACUAN
    # ==================================================================
    def last_price(self, symbol: str) -> float:
        """Harga pasar terakhir; 0 bila simbol belum pernah mengirim data."""
        q = self._quotes.get(symbol)
        if not q:
            return 0.0
        if q.last > 0:
            return q.last
        if q.bid > 0 and q.ask > 0:
            return (q.bid + q.ask) / 2
        return q.bid or q.ask or 0.0

    def _buy_price(self, symbol: str) -> float:
        """Harga beli taker: best ask + slippage (memburuk untuk pembeli)."""
        q = self._quotes.get(symbol)
        ref = (q.ask if q and q.ask > 0 else 0.0) or self.last_price(symbol)
        return ref * (1 + self.slippage_rate) if ref > 0 else 0.0

    def _sell_price(self, symbol: str) -> float:
        """Harga jual taker: best bid - slippage (memburuk untuk penjual)."""
        q = self._quotes.get(symbol)
        ref = (q.bid if q and q.bid > 0 else 0.0) or self.last_price(symbol)
        return ref * (1 - self.slippage_rate) if ref > 0 else 0.0

    # ==================================================================
    # AKUN
    # ==================================================================
    async def get_quote_balance(self) -> tuple[float, float]:
        return self.balance_quote, 0.0

    async def get_base_balance(self, symbol: str) -> tuple[float, float]:
        return (self.base_free.get(symbol, 0.0),
                self.base_locked.get(symbol, 0.0))

    def _next_order_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _credit_base(self, symbol: str, qty: float) -> None:
        self.base_free[symbol] = self.base_free.get(symbol, 0.0) + qty

    def _debit_base(self, symbol: str, qty: float) -> float:
        """Kurangi base free; kembalikan qty yang benar-benar terpakai."""
        avail = self.base_free.get(symbol, 0.0)
        used = min(qty, avail)
        self.base_free[symbol] = max(0.0, avail - used)
        return used

    def _lock_base(self, symbol: str, qty: float) -> float:
        """Kunci base untuk order jual (meniru locked balance Binance)."""
        locked = self._debit_base(symbol, qty)
        if locked > 0:
            self.base_locked[symbol] = self.base_locked.get(symbol, 0.0) + locked
        return locked

    def _unlock_base(self, symbol: str, qty: float) -> float:
        """Lepas kunci base; kembalikan qty yang benar-benar dilepas."""
        held = self.base_locked.get(symbol, 0.0)
        released = min(qty, held)
        self.base_locked[symbol] = max(0.0, held - released)
        if released > 0:
            self._credit_base(symbol, released)
        return released

    # ==================================================================
    # TRADING (virtual)
    # ==================================================================
    async def market_buy(self, symbol: str, quote_qty: float) -> Fill:
        price = self._buy_price(symbol)
        if price <= 0:
            raise ValueError(
                f"Harga pasar {symbol} belum tersedia di stream (paper)")
        if quote_qty <= 0:
            raise ValueError("quote_qty harus > 0 (paper)")
        if quote_qty > self.balance_quote + 1e-9:
            # Pesan sengaja memuat frasa yang dikenali classify_oco_failure
            # sehingga executor menanganinya seperti penolakan Binance asli.
            raise ValueError(
                f"Account has insufficient balance for requested action "
                f"(paper): butuh {quote_qty:.8f} {self.quote_asset}, "
                f"tersedia {self.balance_quote:.8f}")

        qty_gross = quote_qty / price
        fee_base = qty_gross * self.fee_rate          # fee beli dipotong di base
        self.balance_quote -= quote_qty
        self._credit_base(symbol, qty_gross - fee_base)
        self._persist()
        logger.info("[DEMO] BUY %s qty=%.8f @ %.8f (nilai %.2f %s)",
                    symbol, qty_gross, price, quote_qty, self.quote_asset)
        return Fill(symbol=symbol, price=price, qty=qty_gross,
                    quote_qty=quote_qty, fee_quote=fee_base * price,
                    order_id=self._next_order_id())

    async def market_sell(self, symbol: str, qty: float) -> Fill:
        price = self._sell_price(symbol)
        if price <= 0:
            raise ValueError(
                f"Harga pasar {symbol} belum tersedia di stream (paper)")
        avail = self.base_free.get(symbol, 0.0)
        if qty > avail + 1e-12:
            if avail <= 0:
                raise ValueError(
                    f"Account has insufficient balance for requested action "
                    f"(paper): {symbol} free=0, diminta {qty:.8f}")
            qty = avail

        sold = self._debit_base(symbol, qty)
        quote_gross = sold * price
        fee_quote = quote_gross * self.fee_rate       # fee jual dipotong di quote
        self.balance_quote += quote_gross - fee_quote
        self._persist()
        logger.info("[DEMO] SELL %s qty=%.8f @ %.8f (terima %.2f %s)",
                    symbol, sold, price, quote_gross - fee_quote,
                    self.quote_asset)
        return Fill(symbol=symbol, price=price, qty=sold,
                    quote_qty=quote_gross, fee_quote=fee_quote,
                    order_id=self._next_order_id())

    async def place_limit_buy(self, symbol: str, qty: float, price: float) -> int:
        if qty <= 0 or price <= 0:
            raise ValueError("qty dan price limit buy harus > 0 (paper)")
        need = qty * price
        if need > self.balance_quote + 1e-9:
            raise ValueError(
                f"Account has insufficient balance for requested action "
                f"(paper): limit buy butuh {need:.8f} {self.quote_asset}")
        oid = self._next_order_id()
        self._limit_orders[oid] = {
            "symbol": symbol, "side": "BUY", "qty": qty, "price": price,
            "status": "NEW", "executed_qty": 0.0, "quote_qty": 0.0,
            "avg_price": 0.0,
        }
        return oid

    async def get_order_status(self, symbol: str, order_id: int) -> dict:
        o = self._limit_orders.get(order_id)
        if not o:
            return {"status": "CANCELED", "executed_qty": 0.0,
                    "avg_price": 0.0, "quote_qty": 0.0}
        return {
            "status": o["status"],
            "executed_qty": o["executed_qty"],
            "avg_price": o["avg_price"] or o["price"],
            "quote_qty": o["quote_qty"],
        }

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        o = self._limit_orders.get(order_id)
        if o and o["status"] == "NEW":
            o["status"] = "CANCELED"
        return True

    async def cancel_all_orders(self, symbol: str) -> bool:
        for o in self._limit_orders.values():
            if o["symbol"] == symbol and o["status"] == "NEW":
                o["status"] = "CANCELED"
        for o in self._oco_orders.values():
            if o["symbol"] == symbol and o["status"] == "EXECUTING":
                o["status"] = "CANCELLED"
                self._unlock_base(symbol, o["qty"])
        self._persist()
        return True

    # ==================================================================
    # OCO (virtual)
    # ==================================================================
    async def place_oco_sell(self, symbol: str, qty: float, tp_price: float,
                             stop_price: float) -> int:
        if qty <= 0:
            raise ValueError("qty OCO harus > 0 (paper)")
        if not (tp_price > stop_price > 0):
            raise ValueError(
                f"OCO tidak valid (paper): tp={tp_price} harus di atas "
                f"stop={stop_price} dan keduanya > 0")

        mark = self.last_price(symbol)
        if mark > 0 and stop_price >= mark:
            # Binance menolak stop yang sudah tersentuh saat order dibuat.
            raise ValueError(
                f"Order would trigger immediately (paper): stop {stop_price} "
                f">= harga pasar {mark}")

        locked = self._lock_base(symbol, qty)
        if locked + 1e-12 < qty:
            self._unlock_base(symbol, locked)
            raise ValueError(
                f"Account has insufficient balance for requested action "
                f"(paper): OCO {symbol} butuh {qty:.8f}, "
                f"free {self.base_free.get(symbol, 0.0):.8f}")

        oid = self._next_order_id()
        self._oco_orders[oid] = {
            "symbol": symbol, "qty": locked, "tp": tp_price,
            "stop": stop_price,
            "sl_limit": stop_price * (1 - self._sl_limit_buffer_pct / 100),
            "status": "EXECUTING", "filled_qty": 0.0, "filled_quote": 0.0,
            "which": "",
        }
        self._persist()
        return oid

    async def cancel_oco(self, symbol: str, order_list_id: int) -> bool:
        o = self._oco_orders.get(order_list_id)
        if o and o["status"] == "EXECUTING":
            o["status"] = "CANCELLED"
            self._unlock_base(o["symbol"], o["qty"])
            self._persist()
        return True

    async def get_oco_status(self, symbol: str, order_list_id: int) -> dict:
        o = self._oco_orders.get(order_list_id, {})
        filled_qty = o.get("filled_qty", 0.0)
        filled_quote = o.get("filled_quote", 0.0)
        status = o.get("status", "CANCELLED")
        return {
            "list_status": status,
            "done": status != "EXECUTING",
            "any_filled": filled_qty > 0,
            "filled_qty": filled_qty,
            "filled_quote": filled_quote,
            "avg_price": (filled_quote / filled_qty) if filled_qty > 0 else 0.0,
            "which": o.get("which", ""),
        }

    # ==================================================================
    # PEMANTAU ORDER: isi order virtual dari harga pasar SUNGGUHAN
    # ==================================================================
    def poll_orders(self) -> None:
        """
        Satu putaran pemeriksaan order (dipisah agar bisa diuji tanpa loop).

        Limit buy terisi bila harga pasar turun menyentuh limit; OCO terisi
        bila harga menyentuh stop (isi di harga stop-limit, meniru slippage
        stop-limit Binance) atau menyentuh TP.
        """
        changed = False

        for o in self._limit_orders.values():
            if o["status"] != "NEW":
                continue
            px = self.last_price(o["symbol"])
            if px <= 0 or px > o["price"]:
                continue
            cost = o["qty"] * o["price"]
            if cost > self.balance_quote + 1e-9:
                o["status"] = "EXPIRED"      # dana sudah terpakai order lain
                changed = True
                continue
            fee_base = o["qty"] * self.fee_rate
            self.balance_quote -= cost
            self._credit_base(o["symbol"], o["qty"] - fee_base)
            o.update(status="FILLED", executed_qty=o["qty"],
                     quote_qty=cost, avg_price=o["price"])
            changed = True

        for o in self._oco_orders.values():
            if o["status"] != "EXECUTING":
                continue
            px = self.last_price(o["symbol"])
            if px <= 0:
                continue
            hit_sl = px <= o["stop"]
            hit_tp = px >= o["tp"]
            if not (hit_sl or hit_tp):
                continue

            # Prioritaskan SL bila keduanya tersentuh dalam satu tick:
            # asumsi paling konservatif untuk pengujian strategi.
            fill_px = o["sl_limit"] if hit_sl else o["tp"]
            sellable = self._unlock_base(o["symbol"], o["qty"])
            sellable = min(sellable, self.base_free.get(o["symbol"], 0.0))
            if sellable <= 0:
                o["status"] = "REJECTED"
                o["which"] = "rejected"
                changed = True
                continue
            self._debit_base(o["symbol"], sellable)
            gross = sellable * fill_px
            self.balance_quote += gross - gross * self.fee_rate
            o.update(status="EXECUTED", filled_qty=sellable,
                     filled_quote=gross, which="sl" if hit_sl else "tp")
            logger.info("[DEMO] OCO %s %s terisi: qty=%.8f @ %.8f",
                        o["symbol"], o["which"].upper(), sellable, fill_px)
            changed = True

        if changed:
            self._persist()

    async def _run_order_watcher(self) -> None:
        try:
            while self._running:
                try:
                    self.poll_orders()
                except Exception as exc:       # jangan biarkan loop mati
                    logger.exception("Pemantau order demo error: %s", exc)
                await asyncio.sleep(self.ORDER_WATCH_INTERVAL_S)
        except asyncio.CancelledError:
            pass
