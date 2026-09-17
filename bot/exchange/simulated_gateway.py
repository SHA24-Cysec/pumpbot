"""
SimulatedGateway - simulator pasar + paper trading untuk mode `paper`.

Tujuan:
  * Menguji SELURUH pipeline (collector -> signal -> risk -> execution ->
    dashboard) tanpa API key dan tanpa risiko uang.
  * Demo cepat: parameter `time_scale` > 1 mempercepat waktu simulasi.

Simulasi per simbol:
  * Harga random-walk dengan beberapa rezim: NORMAL, PUMP, DUMP.
  * Trade dibangkitkan proses Poisson; ukuran lognormal; sesekali WHALE.
  * Order book disintesis di sekitar harga tengah (plus wall saat pump,
    plus wall palsu yang cepat hilang supaya detektor spoofing teruji).
  * Candle 1 menit diagregasi dari trade (waktu simulasi).

Semua event diberi timestamp WAKTU SIMULASI (bukan waktu dinding) supaya
window deteksi (per menit, per jam) tetap konsisten meski time_scale diubah.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from typing import Optional

from bot.models import BookSnapshot, Candle, Fill, SymbolFilters, Ticker24h, Trade
from bot.exchange.gateway import ExchangeGateway

logger = logging.getLogger("pumpbot.sim")

_SIM_SYMBOLS = [
    "ALPHAUSDT", "BETAUSDT", "GAMMAUSDT", "DELTAUSDT", "EPSILONUSDT",
    "ZETAUSDT", "ETHAUSDT", "THETAUSDT", "IOTAUSDT", "KAPPAUSDT",
    "LAMBDAUSDT", "OMICRONUSDT", "SIGMAUSDT", "TAUUSDT", "PHIUSDT",
    "PSIUSDT", "RHOUSDT", "CHIUSDT", "NUUSDT", "XIUSDT",
]

_REGIME_NORMAL, _REGIME_PUMP, _REGIME_DUMP = "NORMAL", "PUMP", "DUMP"


class _SymbolSim:
    """State simulator satu simbol."""

    def __init__(self, symbol: str, rng: random.Random):
        self.symbol = symbol
        self.rng = rng
        self.price = round(rng.uniform(0.05, 20.0), 6)
        self.regime = _REGIME_NORMAL
        self.regime_until = 0.0          # detik simulasi
        self.next_event_at = None        # dijadwalkan saat loop pertama berjalan
        # statistik "24 jam" sintetis
        self.day_open = self.price * rng.uniform(0.90, 1.05)
        self.day_high = self.price * rng.uniform(1.01, 1.10)
        self.day_low = self.price * rng.uniform(0.90, 0.99)
        self.day_volume = rng.uniform(8_000_000, 400_000_000)  # quote
        self.trade_count = int(rng.uniform(50_000, 2_000_000))
        self.last_trade_id = 0
        # candle yang sedang berjalan (waktu simulasi)
        self.cur_candle: Optional[Candle] = None
        # wall palsu (spoof) aktif: (sisi, harga, qty, berakhir_pada_sim_s)
        self.spoof: Optional[tuple] = None
        # wall asli saat pump
        self.pump_wall: Optional[tuple] = None


class SimulatedGateway(ExchangeGateway):
    mode = "paper"

    def __init__(self, start_equity: float = 10_000.0, symbols: int = 12,
                 time_scale: float = 1.0, seed: int = 42,
                 quote_asset: str = "USDT", fee_pct: float = 0.1,
                 sl_limit_buffer_pct: float = 0.3):
        self.quote_asset = quote_asset
        self.fee_pct = fee_pct / 100.0
        self._sl_limit_buffer_pct = sl_limit_buffer_pct
        self.time_scale = time_scale
        self.rng = random.Random(seed)

        n = min(symbols, len(_SIM_SYMBOLS))
        self.sims: dict[str, _SymbolSim] = {
            s: _SymbolSim(s, random.Random(seed + i + 1))
            for i, s in enumerate(_SIM_SYMBOLS[:n])
        }

        # saldo paper trading
        self.balance_quote = float(start_equity)
        self.base_balances: dict[str, float] = {}

        # jam simulasi
        self._t0 = time.time()
        self._sim_t0 = time.time()

        # order simulasi
        self._next_id = 1000
        self._oco_orders: dict[int, dict] = {}
        self._limit_orders: dict[int, dict] = {}

        self._tasks: list[asyncio.Task] = []
        self._cbs: Optional[dict] = None
        self._running = False

        logger.info(
            f"SimulatedGateway: {len(self.sims)} simbol, modal awal {start_equity:,.0f} "
            f"{quote_asset}, time_scale={time_scale}x"
        )

    # ------------------------------------------------------------- sim clock
    def sim_now_ms(self) -> int:
        """Waktu simulasi dalam epoch ms (berjalan time_scale x lebih cepat)."""
        sim_sec = self._sim_t0 + (time.time() - self._t0) * self.time_scale
        return int(sim_sec * 1000)

    def _sim_sec(self) -> float:
        return self.sim_now_ms() / 1000.0

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        self._running = True
        for sym in self.sims:
            self._tasks.append(asyncio.create_task(
                self._run_symbol(sym), name=f"sim-{sym.lower()}"))
        self._tasks.append(asyncio.create_task(
            self._run_order_watcher(), name="sim-orders"))
        logger.info("Simulator pasar berjalan.")

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ------------------------------------------------------------- info pasar
    async def get_symbol_filters(self) -> dict[str, SymbolFilters]:
        out = {}
        for sym, sim in self.sims.items():
            # tick/step realistis: ~4 desimal di bawah harga
            mag = 10 ** (-max(1, int(math.log10(sim.price)) + 4))
            out[sym] = SymbolFilters(
                symbol=sym, tick_size=mag, step_size=mag * 10,
                min_qty=mag * 10, max_qty=1e12, min_notional=5.0,
            )
        return out

    async def get_universe(self) -> list[Ticker24h]:
        ts = self.sim_now_ms()
        out = []
        for sym, sim in self.sims.items():
            out.append(Ticker24h(
                ts=ts, symbol=sym, last_price=sim.price,
                price_change_pct=(sim.price - sim.day_open) / sim.day_open * 100,
                high=sim.day_high, low=sim.day_low,
                volume=sim.day_volume / max(sim.price, 1e-9),
                quote_volume=sim.day_volume, trade_count=sim.trade_count,
                bid=sim.price * 0.9999, ask=sim.price * 1.0001,
            ))
        return out

    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        """Bangkitkan candle historis (random walk mundur dari harga sekarang)."""
        sim = self.sims.get(symbol)
        if not sim:
            return []
        rng = random.Random(hash(symbol) + limit)
        now = self.sim_now_ms()
        minute = 60_000
        end = now // minute * minute
        candles: list[Candle] = []
        price = sim.price
        for i in range(limit):
            close_t = end - i * minute
            # volatilitas harian ~0.15%/menit
            o = price / rng.uniform(0.9985, 1.0015)
            c = price
            h = max(o, c) * rng.uniform(1.0000, 1.0022)
            low = min(o, c) * rng.uniform(0.9978, 1.0000)
            vol = rng.uniform(0.3, 3.0)  # volume dasar per menit
            if rng.random() < 0.06:      # sesekali ada spike volume historis
                vol *= rng.uniform(3, 8)
            candles.append(Candle(
                open_time=close_t - minute, close_time=close_t,
                open=o, high=h, low=low, close=c,
                volume=vol, quote_volume=vol * (o + c) / 2,
                trades=int(vol * rng.uniform(8, 20)),
                taker_buy_volume=vol * rng.uniform(0.35, 0.65),
                closed=True,
            ))
            price = o
        candles.reverse()
        # pastikan kontinuitas dengan harga sekarang
        return candles

    # ------------------------------------------------------------- streaming
    async def subscribe(self, symbols, on_candle, on_trade, on_book, on_ticker) -> None:
        self._cbs = {"candle": on_candle, "trade": on_trade,
                     "book": on_book, "ticker": on_ticker}

    # ------------------------------------------------------------- akun
    async def get_quote_balance(self) -> tuple[float, float]:
        return self.balance_quote, 0.0

    # ------------------------------------------------------------- trading
    def _next_order_id(self) -> int:
        self._next_id += 1
        return self._next_id

    async def market_buy(self, symbol: str, quote_qty: float) -> Fill:
        sim = self.sims.get(symbol)
        if not sim:
            raise ValueError(f"Simbol {symbol} tidak ada di simulator")
        if quote_qty > self.balance_quote:
            raise ValueError("Saldo quote tidak cukup (paper)")
        price = sim.price * (1 + self.rng.uniform(0.0001, 0.0006))  # slippage naik
        qty_gross = quote_qty / price
        fee_base = qty_gross * self.fee_pct
        self.balance_quote -= quote_qty
        self.base_balances[symbol] = self.base_balances.get(symbol, 0.0) + (qty_gross - fee_base)
        self._emit_trade(symbol, price, qty_gross, buyer_is_maker=False, whale=True)
        logger.debug(f"[PAPER] BUY {symbol}: {qty_gross:.6f} @ {price:.6f}")
        return Fill(symbol=symbol, price=price, qty=qty_gross,
                    quote_qty=quote_qty, fee_quote=fee_base * price,
                    order_id=self._next_order_id())

    async def market_sell(self, symbol: str, qty: float) -> Fill:
        sim = self.sims.get(symbol)
        if not sim:
            raise ValueError(f"Simbol {symbol} tidak ada di simulator")
        avail = self.base_balances.get(symbol, 0.0)
        if qty > avail + 1e-12:
            qty = avail
        price = sim.price * (1 - self.rng.uniform(0.0001, 0.0006))  # slippage turun
        quote_gross = qty * price
        fee_quote = quote_gross * self.fee_pct
        self.base_balances[symbol] = avail - qty
        self.balance_quote += quote_gross - fee_quote
        self._emit_trade(symbol, price, qty, buyer_is_maker=True, whale=False)
        logger.debug(f"[PAPER] SELL {symbol}: {qty:.6f} @ {price:.6f}")
        return Fill(symbol=symbol, price=price, qty=qty,
                    quote_qty=quote_gross, fee_quote=fee_quote,
                    order_id=self._next_order_id())

    async def place_limit_buy(self, symbol: str, qty: float, price: float) -> int:
        oid = self._next_order_id()
        self._limit_orders[oid] = {
            "symbol": symbol, "side": "BUY", "qty": qty, "price": price,
            "status": "NEW", "executed_qty": 0.0, "quote_qty": 0.0,
        }
        return oid

    async def get_order_status(self, symbol: str, order_id: int) -> dict:
        o = self._limit_orders.get(order_id, {})
        return {
            "status": o.get("status", "CANCELED"),
            "executed_qty": o.get("executed_qty", 0.0),
            "avg_price": o.get("price", 0.0),
            "quote_qty": o.get("quote_qty", 0.0),
        }

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        o = self._limit_orders.get(order_id)
        if o and o["status"] == "NEW":
            o["status"] = "CANCELED"
        return True

    async def cancel_all_orders(self, symbol: str) -> bool:
        """Batalkan semua order/OCO terbuka simbol ini (pemulihan restart)."""
        for o in self._limit_orders.values():
            if o["symbol"] == symbol and o["status"] == "NEW":
                o["status"] = "CANCELED"
        for o in self._oco_orders.values():
            if o["symbol"] == symbol and o["status"] == "EXECUTING":
                o["status"] = "CANCELLED"
        return True

    # ------------------------------------------------------------- OCO
    async def place_oco_sell(self, symbol: str, qty: float, tp_price: float,
                             stop_price: float) -> int:
        oid = self._next_order_id()
        self._oco_orders[oid] = {
            "symbol": symbol, "qty": qty, "tp": tp_price, "stop": stop_price,
            "sl_limit": stop_price * (1 - self._sl_limit_buffer_pct / 100),
            "status": "EXECUTING", "filled_qty": 0.0, "filled_quote": 0.0,
            "which": "",
        }
        return oid

    async def cancel_oco(self, symbol: str, order_list_id: int) -> bool:
        o = self._oco_orders.get(order_list_id)
        if o and o["status"] == "EXECUTING":
            o["status"] = "CANCELLED"
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

    # =====================================================================
    # LOOP SIMULASI
    # =====================================================================
    async def _run_symbol(self, symbol: str) -> None:
        """Loop utama satu simbol: langkah harga tiap ~100 ms waktu dinding."""
        sim = self.sims[symbol]
        step_wall = 0.1
        book_counter = 0.0
        try:
            while self._running:
                dt_sim = step_wall * self.time_scale  # detik simulasi per langkah
                self._step_price(sim, dt_sim)
                self._emit_trades(sim, dt_sim)
                self._update_candle(sim)

                # book & ticker tiap ~1 detik simulasi
                book_counter += dt_sim
                if book_counter >= 1.0:
                    book_counter = 0.0
                    self._emit_book(sim)
                    self._emit_ticker(sim)

                await asyncio.sleep(step_wall)
        except asyncio.CancelledError:
            pass

    def _step_price(self, sim: _SymbolSim, dt: float) -> None:
        """Satu langkah harga: drift + volatilitas tergantung rezim."""
        now = self._sim_sec()
        self._update_regime(sim, now)

        if sim.regime == _REGIME_PUMP:
            drift, vol = 0.0006, 0.0016     # +0.06%/detik, vol tinggi
        elif sim.regime == _REGIME_DUMP:
            drift, vol = -0.0005, 0.0020    # dump setelah pump palsu
        else:
            drift, vol = 0.000002, 0.00035  # kangaroo random walk

        shock = sim.rng.gauss(drift * dt, vol * math.sqrt(dt))
        sim.price = max(1e-9, sim.price * math.exp(shock))
        sim.day_high = max(sim.day_high, sim.price)
        sim.day_low = min(sim.day_low, sim.price)

    def _update_regime(self, sim: _SymbolSim, now: float) -> None:
        """Jadwal pergantian rezim NORMAL -> PUMP -> (mungkin) DUMP -> NORMAL."""
        if now >= sim.regime_until:
            if sim.regime == _REGIME_PUMP:
                # 55% pump diikuti dump (menguji detektor manipulasi),
                # sisanya transisi ke tren sehat
                if sim.rng.random() < 0.55:
                    sim.regime = _REGIME_DUMP
                    sim.regime_until = now + sim.rng.uniform(20, 90)
                else:
                    sim.regime = _REGIME_NORMAL
                    sim.regime_until = now + sim.rng.uniform(30, 120)
            else:
                sim.regime = _REGIME_NORMAL
                sim.regime_until = now + sim.rng.uniform(10, 60)
            sim.pump_wall = None

        if sim.next_event_at is None:
            # jadwalkan episode pump pertama (relatif terhadap jam simulasi)
            sim.next_event_at = now + sim.rng.uniform(20, 300)

        if sim.regime != _REGIME_PUMP and now >= sim.next_event_at:
            # mulai episode PUMP
            sim.regime = _REGIME_PUMP
            sim.regime_until = now + sim.rng.uniform(60, 180)
            sim.next_event_at = now + sim.rng.uniform(120, 900)
            # kadang ada wall beli besar (support) saat pump
            if sim.rng.random() < 0.35:
                lvl = sim.rng.randint(2, 5)
                sim.pump_wall = ("bid", sim.price * (1 - lvl * 0.0008),
                                 sim.rng.uniform(20, 60))
            # kadang muncul wall jual PALSU (spoof) yang akan hilang cepat
            if sim.rng.random() < 0.30:
                sim.spoof = ("ask", sim.price * 1.002,
                             sim.rng.uniform(25, 50), now + sim.rng.uniform(4, 9))
            logger.info(f"[SIM] {sim.symbol} mulai rezim PUMP @ {sim.price:.6f}")

    def _emit_trades(self, sim: _SymbolSim, dt: float) -> None:
        """Bangkitkan trade (proses Poisson) untuk dt detik simulasi."""
        if sim.regime == _REGIME_PUMP:
            rate = sim.rng.uniform(6, 14)       # trade/detik saat pump
        elif sim.regime == _REGIME_DUMP:
            rate = sim.rng.uniform(4, 10)
        else:
            rate = sim.rng.uniform(0.2, 1.5)

        n = self._poisson(self.rng, rate * dt)
        for _ in range(n):
            # saat pump tekanan beli dominan (agresor beli = m False)
            buy_prob = 0.72 if sim.regime == _REGIME_PUMP else (
                       0.30 if sim.regime == _REGIME_DUMP else 0.50)
            buyer_is_maker = self.rng.random() > buy_prob  # True = agresor jual
            # ukuran trade lognormal
            base_size = sim.rng.lognormvariate(-4.2, 1.1) * (3 if sim.regime == _REGIME_PUMP else 1)
            whale = self.rng.random() < (0.012 if sim.regime == _REGIME_PUMP else 0.0015)
            if whale:
                base_size *= sim.rng.uniform(10, 30)
            self._emit_trade(sim.symbol, sim.price, base_size, buyer_is_maker, whale)

    @staticmethod
    def _poisson(rng: random.Random, lam: float) -> int:
        """
        Sampling distribusi Poisson (algoritma Knuth untuk lambda kecil,
        aproksimasi normal untuk lambda besar) - random.Random stdlib
        tidak punya metode poisson.
        """
        if lam <= 0:
            return 0
        if lam > 30:
            return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
        L = math.exp(-lam)
        k, p = 0, 1.0
        while True:
            p *= rng.random()
            if p <= L:
                return k
            k += 1

    def _emit_trade(self, symbol: str, price: float, qty: float,
                    buyer_is_maker: bool, whale: bool = False) -> None:
        sim = self.sims.get(symbol)
        if sim is not None:
            self._acc_candle(sim, price, qty, buyer_is_maker)
        if not self._cbs:
            return
        self._cbs["trade"](symbol, Trade(
            ts=self.sim_now_ms(), price=price, qty=qty, buyer_is_maker=buyer_is_maker))

    def _acc_candle(self, sim: _SymbolSim, price: float, qty: float,
                    buyer_is_maker: bool) -> None:
        """Akumulasi trade ke candle berjalan (bucket 1 menit waktu simulasi)."""
        now = self.sim_now_ms()
        bucket = now // 60_000 * 60_000
        c = sim.cur_candle
        if c is None or c.open_time != bucket:
            c = Candle(
                open_time=bucket, close_time=bucket + 60_000 - 1,
                open=price, high=price, low=price, close=price,
                volume=0.0, quote_volume=0.0, trades=0, taker_buy_volume=0.0,
                closed=False,
            )
            sim.cur_candle = c
        c.high = max(c.high, price)
        c.low = min(c.low, price)
        c.close = price
        c.volume += qty
        c.quote_volume += qty * price
        c.trades += 1
        if not buyer_is_maker:          # agresor beli
            c.taker_buy_volume += qty

    def _update_candle(self, sim: _SymbolSim) -> None:
        """Update OHLC dari harga terakhir & emit candle yang sudah close."""
        if not self._cbs:
            return
        now = self.sim_now_ms()
        bucket = now // 60_000 * 60_000
        c = sim.cur_candle
        if c is not None and c.open_time != bucket:
            # waktu simulasi sudah masuk menit baru -> tutup candle lama
            c.closed = True
            self._cbs["candle"](sim.symbol, c)
            sim.cur_candle = None
        elif c is not None:
            c.high = max(c.high, sim.price)
            c.low = min(c.low, sim.price)
            c.close = sim.price

    def _emit_book(self, sim: _SymbolSim) -> None:
        """Sintesis order book 20 level di sekitar harga."""
        if not self._cbs:
            return
        now = self._sim_sec()
        # hapus spoof yang sudah kedaluwarsa
        if sim.spoof and now >= sim.spoof[3]:
            sim.spoof = None

        bids, asks = [], []
        tick = max(sim.price * 0.0002, 1e-9)
        base_liq = 500.0 / max(sim.price, 1e-9)  # "likuiditas" dasar per level
        pump_bias = 2.8 if sim.regime == _REGIME_PUMP else 1.0

        for i in range(20):
            bid_px = sim.price - tick * (i + 1) * sim.rng.uniform(0.8, 1.3)
            ask_px = sim.price + tick * (i + 1) * sim.rng.uniform(0.8, 1.3)
            bid_qty = abs(sim.rng.lognormvariate(0, 0.8)) * base_liq * pump_bias
            ask_qty = abs(sim.rng.lognormvariate(0, 0.8)) * base_liq / pump_bias
            bids.append((round(bid_px, 10), round(bid_qty, 6)))
            asks.append((round(ask_px, 10), round(ask_qty, 6)))

        # wall beli asli saat pump (support)
        if sim.pump_wall:
            side, price, qty = sim.pump_wall
            bids.append((round(price, 10), round(qty * base_liq * 20, 6)))
            bids.sort(key=lambda x: -x[0])
            bids = bids[:20]
        # wall jual palsu (spoofing) -> muncul lalu hilang
        if sim.spoof:
            side, price, qty, _ = sim.spoof
            asks.append((round(price, 10), round(qty * base_liq * 20, 6)))
            asks.sort(key=lambda x: x[0])
            asks = asks[:20]

        self._cbs["book"](sim.symbol, BookSnapshot(
            ts=self.sim_now_ms(), bids=bids, asks=asks))

    def _emit_ticker(self, sim: _SymbolSim) -> None:
        if not self._cbs:
            return
        self._cbs["ticker"](sim.symbol, Ticker24h(
            ts=self.sim_now_ms(), symbol=sim.symbol, last_price=sim.price,
            price_change_pct=(sim.price - sim.day_open) / sim.day_open * 100,
            high=sim.day_high, low=sim.day_low,
            volume=sim.day_volume / max(sim.price, 1e-9),
            quote_volume=sim.day_volume, trade_count=sim.trade_count,
            bid=sim.price * 0.9999, ask=sim.price * 1.0001,
        ))

    # =====================================================================
    # PENONTON ORDER SIMULASI
    # =====================================================================
    async def _run_order_watcher(self) -> None:
        """Isi limit order & OCO berdasarkan pergerakan harga simulasi."""
        try:
            while self._running:
                now = self.sim_now_ms()
                for oid, o in list(self._limit_orders.items()):
                    if o["status"] != "NEW":
                        continue
                    sim = self.sims.get(o["symbol"])
                    if not sim:
                        continue
                    if sim.price <= o["price"]:  # limit buy tersentuh
                        o["status"] = "FILLED"
                        o["executed_qty"] = o["qty"]
                        o["quote_qty"] = o["qty"] * o["price"]
                        self.balance_quote -= o["quote_qty"]
                        fee = o["qty"] * self.fee_pct
                        self.base_balances[o["symbol"]] = \
                            self.base_balances.get(o["symbol"], 0.0) + o["qty"] - fee

                for oid, o in list(self._oco_orders.items()):
                    if o["status"] != "EXECUTING":
                        continue
                    sim = self.sims.get(o["symbol"])
                    if not sim:
                        continue
                    if sim.price <= o["stop"] or sim.price >= o["tp"]:
                        # SL kena (isi di harga stop-limit) atau TP kena.
                        # PENTING: jual hanya sebatas base yang benar-benar
                        # dimiliki — di exchange nyata, menjual aset yang tidak
                        # dimiliki ditolak (insufficient balance). Tanpa clamp
                        # ini, bug executor mana pun akan tampat sebagai kredit
                        # quote hantu (base balance jadi negatif).
                        px = o["sl_limit"] if sim.price <= o["stop"] else o["tp"]
                        sellable = min(o["qty"],
                                       self.base_balances.get(o["symbol"], 0.0))
                        if sellable <= 0:
                            # tidak punya aset -> order ditolak (bukan terisi)
                            o["status"] = "REJECTED"
                            o["which"] = "rejected"
                            continue
                        o["status"] = "EXECUTED"
                        o["filled_qty"] = sellable
                        o["filled_quote"] = sellable * px
                        o["which"] = "sl" if sim.price <= o["stop"] else "tp"
                        self.base_balances[o["symbol"]] = \
                            self.base_balances.get(o["symbol"], 0.0) - sellable
                        self.balance_quote += sellable * px * (1 - self.fee_pct)

                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            pass
