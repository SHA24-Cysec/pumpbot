"""
Implementasi ExchangeGateway untuk Binance Spot memakai SDK RESMI
`binance-sdk-spot` (penerus resmi binance-connector yang sudah deprecated).

Hal penting desain:
  * REST SDK ini sinkron (pakai `requests`) -> semua panggilan REST dibungkus
    asyncio.to_thread() supaya tidak memblokir event loop bot.
  * WebSocket Streams SDK ini async & callback-nya SYNC -> callback kami hanya
    menulis buffer (micro-detik), tidak pernah menunggu I/O.
  * Semua respons model dikonversi jadi dict mentah (by_alias) / dataclass
    internal supaya bot tidak bergantung pada bentuk model SDK.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from typing import Callable, Optional

from bot.models import BookSnapshot, Candle, Fill, SymbolFilters, Ticker24h, Trade
from bot.exchange.gateway import ExchangeGateway

logger = logging.getLogger("pumpbot.binance")


try:
    from binance_common.configuration import (
        ConfigurationRestAPI,
        ConfigurationWebSocketStreams,
    )
    from binance_common.constants import (
        SPOT_REST_API_PROD_URL,
        SPOT_REST_API_TESTNET_URL,
        SPOT_WS_STREAMS_PROD_URL,
        SPOT_WS_STREAMS_TESTNET_URL,
    )
    from binance_common.errors import (
        BadRequestError,
        ClientError,
        NetworkError,
        ServerError,
        TooManyRequestsError,
    )
    from binance_common.models import ApiResponse
    from binance_sdk_spot.spot import Spot

    from binance_sdk_spot.rest_api.models.enums import (
        KlinesIntervalEnum,
        NewOrderNewOrderRespTypeEnum,
        NewOrderSideEnum,
        NewOrderTimeInForceEnum,
        NewOrderTypeEnum,
        OrderOcoSideEnum,
        OrderOcoStopLimitTimeInForceEnum,
    )
    from binance_sdk_spot.websocket_streams.models.enums import (
        KlineIntervalEnum as WSKlineIntervalEnum,
        PartialBookDepthLevelsEnum,
        PartialBookDepthUpdateSpeedEnum,
    )
    _SDK_AVAILABLE = True

    # Peta interval REST & WS (dibangun di sini supaya modul tetap bisa
    # di-import walau SDK belum terpasang, mis. utk keperluan testing).
    _INTERVAL_MAP = {
        "1m": (KlinesIntervalEnum.INTERVAL_1m, "1m"),
        "3m": (KlinesIntervalEnum.INTERVAL_3m, "3m"),
        "5m": (KlinesIntervalEnum.INTERVAL_5m, "5m"),
        "15m": (KlinesIntervalEnum.INTERVAL_15m, "15m"),
    }
except ImportError as exc:  # pragma: no cover
    _SDK_AVAILABLE = False
    _import_error = exc
    _INTERVAL_MAP = {}

# Error koneksi aiohttp (dipakai SDK utk WebSocket). Dipakai untuk
# membedakan "koneksi direset server" (layak diulang) dari error program.
try:  # pragma: no cover - aiohttp selalu terpasang bersama SDK
    from aiohttp import ClientError as _AiohttpClientError
    _RETRYABLE_SUBSCRIBE_ERRORS = (ConnectionError, _AiohttpClientError)
except ImportError:  # pragma: no cover
    _RETRYABLE_SUBSCRIBE_ERRORS = (ConnectionError,)


# Interval REST & WS punya enum berbeda tapi value string-nya sama ("1m", dst)
# Basis asset yang BUKAN target pump-hunting:
# - stablecoin & fiat (pergerakannya bukan spekulasi)
# - token leveraged UP/DOWN/BULL/BEAR (nilainya meluruh oleh desain)
_STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EUR", "GBP", "AUD",
    "TRY", "BRL", "ARS", "JPY", "RUB", "UAH", "PLN", "RON", "ZAR",
}
_LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

# Karakter yang diizinkan regex parameter `symbols` endpoint market-data
# Binance: ^[A-Z0-9_.-]{1,50}$ (tanpa lowercase/karakter non-ASCII).
# PENTING: ada listing dengan simbol non-ASCII (mis. meme coin CJK
# "币安人生USDT", "牛来USDT") — sah di exchangeInfo, tetapi SATU simbol
# seperti itu membuat SELURUH request ticker24hr(symbols=[...]) ditolak
# HTTP 400 (-1100 "Illegal characters"). Maka simbol harus disaring
# sebelum dikirim via parameter symbols.
_SYMBOL_RE = re.compile(r"^[A-Z0-9_.\-]{1,50}$")


def _is_requestable_symbol(symbol: str) -> bool:
    """True kalau simbol aman dikirim sebagai parameter `symbols`/`symbol`."""
    return bool(_SYMBOL_RE.match(symbol or ""))


def _is_pump_candidate_symbol(symbol: str, quote: str) -> bool:
    """True kalau simbol layak diawasi untuk pump (bukan stable/fiat/leveraged)."""
    if not symbol.endswith(quote) or len(symbol) <= len(quote):
        return False
    base = symbol[: -len(quote)]
    if base in _STABLE_BASES:
        return False
    return not any(base.endswith(sfx) and len(base) > len(sfx)
                   for sfx in _LEVERAGED_SUFFIXES)


def _to_plain(data):
    """
    Normalisasi output SDK -> struktur Python murni.
    Menangani: ApiResponse, model union (actual_instance), model biasa, list.
    """
    if hasattr(data, "data"):          # ApiResponse
        data = data.data()
    if hasattr(data, "actual_instance"):  # model union (oneOf)
        data = data.actual_instance
    if isinstance(data, list):
        return [_to_plain(item) for item in data]
    if hasattr(data, "to_dict"):        # model pydantic hasil generate
        try:
            return data.to_dict()
        except Exception:
            return data
    return data


def _f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class BinanceGateway(ExchangeGateway):
    """Gateway ke Binance Spot (testnet atau live) via binance-sdk-spot."""

    def __init__(self, mode: str, api_key: str, api_secret: str,
                 quote_asset: str = "USDT", sl_limit_buffer_pct: float = 0.3,
                 depth_levels: int = 20):
        if not _SDK_AVAILABLE:
            raise RuntimeError(
                f"SDK resmi binance-sdk-spot belum terpasang: {_import_error}. "
                f"Jalankan: pip install -r requirements.txt"
            )
        self.mode = mode                      # "testnet" | "live"
        self.quote_asset = quote_asset
        self._sl_limit_buffer_pct = sl_limit_buffer_pct
        # Kedalaman stream orderbook: dari config data.depth_levels
        # (5/10/20; makin dalam makin boros bandwidth, deteksi spoofing
        # makin luas cakupannya). Detektor imbalance membaca
        # [:imbalance_levels] sendiri (divalidasi <= depth_levels).
        self._depth_levels = depth_levels

        if mode == "live":
            rest_url, ws_url = SPOT_REST_API_PROD_URL, SPOT_WS_STREAMS_PROD_URL
        else:
            rest_url, ws_url = SPOT_REST_API_TESTNET_URL, SPOT_WS_STREAMS_TESTNET_URL

        self._client = Spot(
            config_rest_api=ConfigurationRestAPI(
                api_key=api_key,
                api_secret=api_secret,
                base_path=rest_url,
                timeout=15000,       # ms
                retries=2,           # retry internal utk 5xx
                backoff=500,
            ),
            config_ws_streams=ConfigurationWebSocketStreams(
                stream_url=ws_url,
                reconnect_delay=5000,     # ms antar percobaan reconnect
                reconnect_attempts=10,    # maksimum percobaan bawaan SDK
            ),
        )
        self._ws_started = False
        self._stream_handles = []
        self._watchdog_task: Optional[asyncio.Task] = None
        self._subscribed_symbols: list[str] = []
        self._cbs: dict = {}
        logger.info(f"BinanceGateway aktif: mode={mode.upper()} rest={rest_url} ws={ws_url} "
                    f"depth={self._depth_levels} level")

    # ------------------------------------------------------------------ util
    async def _rest(self, fn, *args, **kwargs):
        """
        Panggil REST SDK dengan retry cerdas:
          - 429/5xx/network -> exponential backoff (karena order trading
            tidak boleh asal diulang, retry HANYA untuk operasi idempoten
            yang aman: query data. Order tetap diteruskan error-nya ke pemanggil).
        """
        delay = 1.0
        for attempt in range(1, 6):
            try:
                return _to_plain(await asyncio.to_thread(fn, *args, **kwargs))
            except TooManyRequestsError as exc:
                wait = float(getattr(exc, "retry_after", None) or delay)
                logger.warning(f"Rate limit (429), tunggu {wait:.0f}s: {exc.error_message}")
                await asyncio.sleep(min(wait, 60))
                delay *= 2
            except (NetworkError, ServerError) as exc:
                logger.warning(f"REST error jaringan/server (percobaan {attempt}/5): {exc}")
                await asyncio.sleep(delay)
                delay *= 2
        raise NetworkError("REST gagal setelah 5 percobaan")

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        # ping sekali untuk memvalidasi koneksi & kredensial lebih awal
        await self._rest(self._client.rest_api.ping)

    async def stop(self) -> None:
        if self._watchdog_task:
            self._watchdog_task.cancel()
        try:
            for handle in self._stream_handles:
                try:
                    await handle.unsubscribe()
                except Exception:
                    pass
            await self._client.websocket_streams.close_connection(close_session=True)
        except Exception as exc:
            logger.debug(f"Error saat menutup WS: {exc}")
        self._ws_started = False

    # -------------------------------------------------------------- info pasar
    async def get_symbol_filters(self) -> dict[str, SymbolFilters]:
        info = await self._rest(self._client.rest_api.exchange_info)
        out: dict[str, SymbolFilters] = {}
        for sym in info.get("symbols", []):
            try:
                if sym.get("status") != "TRADING":
                    continue
                sf = SymbolFilters(symbol=sym["symbol"])
                for flt in sym.get("filters", []):
                    ftype = flt.get("filterType", "")
                    if ftype == "PRICE_FILTER":
                        sf.tick_size = _f(flt.get("tickSize"), sf.tick_size)
                        sf.min_price = _f(flt.get("minPrice"), sf.min_price)
                        sf.max_price = _f(flt.get("maxPrice"), sf.max_price)
                    elif ftype == "LOT_SIZE":
                        sf.step_size = _f(flt.get("stepSize"), sf.step_size)
                        sf.min_qty = _f(flt.get("minQty"), sf.min_qty)
                        sf.max_qty = _f(flt.get("maxQty"), sf.max_qty)
                    elif ftype == "MARKET_LOT_SIZE":
                        sf.market_step_size = _f(flt.get("stepSize")) or None
                        sf.market_min_qty = _f(flt.get("minQty")) or None
                        sf.market_max_qty = _f(flt.get("maxQty")) or None
                    elif ftype in ("NOTIONAL", "MIN_NOTIONAL"):
                        sf.min_notional = _f(flt.get("minNotional"), sf.min_notional)
                out[sym["symbol"]] = sf
            except (KeyError, ValueError) as exc:
                logger.debug(f"Lewati simbol rusak {sym.get('symbol')}: {exc}")
        logger.info(f"exchangeInfo: {len(out)} simbol TRADING")
        return out

    async def get_universe(self) -> list[Ticker24h]:
        """
        Statistik 24h seluruh simbol pump-candidate.

        PENTING: endpoint /api/v3/ticker/24hr MEWAJIBKAN parameter symbol
        atau symbols (maks 100 simbol per permintaan) sejak perubahan API
        Binance — memanggil tanpa parameter adalah jalur lama yang di
        production mengembalikan format mentah (array of arrays) dan
        membuat SDK gagal mem-parsing. Maka: ambil daftar simbol dari
        exchangeInfo dulu, lalu query ticker per kelompok 100 simbol.

        Kalau jalur terdokumentasi itu ditolak server (mis. WAF/regional
        mengembalikan HTTP 400 seperti yang dialami pada api.binance.com
        dari sebagian jaringan), jatuh ke panggilan tanpa-parameter yang
        masih ditoleransi testnet.
        """
        info = await self._rest(self._client.rest_api.exchange_info)
        symbols = [
            s.get("symbol", "")
            for s in (info.get("symbols") or [])
            if s.get("status") == "TRADING"
            and _is_pump_candidate_symbol(s.get("symbol", ""),
                                          self.quote_asset)
            and _is_requestable_symbol(s.get("symbol", ""))
        ]

        def _parse(rows) -> list[Ticker24h]:
            out: list[Ticker24h] = []
            for t in (rows or []):
                if not isinstance(t, dict):
                    continue      # format tak terduga -> lewati
                sym = t.get("symbol", "")
                if not _is_pump_candidate_symbol(sym, self.quote_asset):
                    continue      # buang pair stable/fiat/leveraged
                if not _is_requestable_symbol(sym):
                    continue      # simbol non-ASCII -> tak bisa ditradingkan aman
                try:
                    out.append(Ticker24h(
                        ts=int(_f(t.get("closeTime"))),
                        symbol=t["symbol"],
                        last_price=_f(t.get("lastPrice")),
                        price_change_pct=_f(t.get("priceChangePercent")),
                        high=_f(t.get("highPrice")),
                        low=_f(t.get("lowPrice")),
                        volume=_f(t.get("volume")),
                        quote_volume=_f(t.get("quoteVolume")),
                        trade_count=int(_f(t.get("count"))),
                        bid=_f(t.get("bidPrice")),
                        ask=_f(t.get("askPrice")),
                    ))
                except (KeyError, TypeError, ValueError):
                    continue
            return out

        # Jalur utama: per kelompok 100 simbol (terdokumentasi).
        try:
            out: list[Ticker24h] = []
            for i in range(0, len(symbols), 100):
                resp = await self._rest(self._client.rest_api.ticker24hr,
                                        symbols=symbols[i:i + 100])
                out.extend(_parse(resp))
            if out:
                return out
        except Exception as exc:         # noqa: BLE001 — fallback di bawah
            logger.warning(
                "ticker24hr(symbols=) ditolak (%s) — coba jalur lama "
                "tanpa parameter", exc)

        # Fallback: tanpa parameter (masih diterima testnet).
        resp = await self._rest(self._client.rest_api.ticker24hr)
        return _parse(resp)

    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        enum_interval = _INTERVAL_MAP[interval][0]
        rows = await self._rest(
            self._client.rest_api.klines, symbol=symbol,
            interval=enum_interval, limit=limit,
        )
        candles = []
        for k in rows:
            # format array: [openTime, o, h, l, c, vol, closeTime, quoteVol,
            #                count, takerBuyBase, takerBuyQuote, ignore]
            candles.append(Candle(
                open_time=int(k[0]), close_time=int(k[6]),
                open=_f(k[1]), high=_f(k[2]), low=_f(k[3]), close=_f(k[4]),
                volume=_f(k[5]), quote_volume=_f(k[7]), trades=int(_f(k[8])),
                taker_buy_volume=_f(k[9]), closed=True,
            ))
        return candles

    # -------------------------------------------------------------- streaming
    # Batas Binance: maksimum 5 PESAN MASUK per detik per koneksi WebSocket
    # (SUBSCRIBE/UNSUBSCRIBE/PING). Melebihi itu, server MEMUTUS koneksi.
    # Kenyataan (diverifikasi empiris): subscribe 69 simbol x 4 stream
    # satu-per-satu tanpa jeda di jaringan cepat > 5 pesan/dtk ->
    # "ClientConnectionResetError: Cannot write to closing transport".
    # Solusi: PACING (jeda antar langganan) + RETRY tahan reset koneksi.
    SUBSCRIBE_PACE_S = 0.26         # ~3.8 pesan/dtk, aman di bawah batas 5
    SUBSCRIBE_MAX_ATTEMPTS = 5      # retry per stream bila koneksi direset
    SUBSCRIBE_RETRY_WAIT_S = 6.0    # >= reconnect_delay SDK (5 dtk) + margin

    async def _subscribe_with_pace(self, factory, label: str):
        """
        Satu langganan stream dengan pacing + retry tahan reset koneksi.

        - Jeda SUBSCRIBE_PACE_S sebelum tiap kirim -> tetap di bawah batas
          5 pesan masuk/dtk Binance (0.26s x N stream; 276 stream = +-72
          detik, sekali saja saat startup/refresh).
        - Koneksi direset (ClientConnectionResetError dkk.): tunggu SDK
          auto-reconnect lalu ulangi langganan yang sama (aman - duplikat
          SUBSCRIBE diabaikan server).
        - Pool koneksi KOSONG (semua reconnect SDK gagal setelah outage
          panjang -> SDK melepas semua langganan & menghapus koneksi):
          bangun ulang koneksi (create_connection) lalu ulangi. Tanpa ini,
          bot tidak pernah pulih sendiri setelah internet/listrik mati
          lama (watchdog berulang kali gagal dengan
          "ValueError: No WebSocket connections available").
        - SDK menolak DIAm-diam (mengembalikan None saat koneksi sedang
          sibuk reconnect) -> diulang, bukan crash di handle.on().
        - Error non-koneksi (bug program) langsung diteruskan, tidak diulang.
        """
        for attempt in range(1, self.SUBSCRIBE_MAX_ATTEMPTS + 1):
            await asyncio.sleep(self.SUBSCRIBE_PACE_S)
            try:
                result = await factory()
            except Exception as exc:
                # pesan SDK saat pool koneksi kosong (lihat docstring)
                pool_kosong = "no websocket connections" in str(exc).lower()
                if (not pool_kosong
                        and not isinstance(exc, _RETRYABLE_SUBSCRIBE_ERRORS)):
                    raise                    # bug program: jangan diulang
                if attempt == self.SUBSCRIBE_MAX_ATTEMPTS:
                    raise                    # teruskan error asli
                if pool_kosong:
                    try:
                        await self._client.websocket_streams.create_connection()
                        logger.info("Koneksi WS dibangun ulang "
                                    "(pool sempat kosong setelah outage)")
                    except Exception as cexc:
                        logger.warning(f"Bangun ulang koneksi WS gagal "
                                       f"(coba lagi nanti): {cexc}")
                wait_s = self.SUBSCRIBE_RETRY_WAIT_S * attempt
                logger.warning(
                    f"Subscribe {label} gagal ({type(exc).__name__}: {exc}) - "
                    f"tunggu {wait_s:.0f}s lalu ulangi "
                    f"({attempt + 1}/{self.SUBSCRIBE_MAX_ATTEMPTS})")
                await asyncio.sleep(wait_s)
                continue
            if result is not None:
                return result
            # SDK menolak tanpa exception (koneksi sedang reconnect)
            if attempt == self.SUBSCRIBE_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"subscribe {label} ditolak SDK "
                    f"{self.SUBSCRIBE_MAX_ATTEMPTS}x berturut-turut "
                    f"(koneksi tidak stabil)")
            logger.warning(f"Subscribe {label} ditunda SDK (koneksi sedang "
                           f"reconnect) - ulangi "
                           f"({attempt + 1}/{self.SUBSCRIBE_MAX_ATTEMPTS})")
            await asyncio.sleep(self.SUBSCRIBE_RETRY_WAIT_S * attempt)
        raise RuntimeError(f"subscribe {label}: tidak terjangkau")

    async def subscribe(self, symbols, on_candle, on_trade, on_book, on_ticker) -> None:
        """Berlangganan 4 stream per simbol lewat satu koneksi WebSocket gabungan."""
        self._subscribed_symbols = list(symbols)
        self._cbs = {"candle": on_candle, "trade": on_trade,
                     "book": on_book, "ticker": on_ticker}
        # CATATAN PENTING: interval & levels HARUS string/angka murni,
        # BUKAN enum. Versi SDK tertentu membangun nama stream rusak bila
        # diberi enum: "btcusdt@kline_KlineIntervalEnum.INTERVAL_1m"
        # (server menerima SUBSCRIBE tanpa error tapi stream tidak pernah
        # ada -> orderbook/kline tidak pernah masuk -> GATE abadi).
        # Diverifikasi: string "1m" / angka levels -> nama stream benar.
        interval_ws = "1m"
        levels = self._depth_levels   # tangga 5/10/20 dari imbalance_levels


        streams = self._client.websocket_streams
        if not self._ws_started:
            await streams.create_connection()
            # Catatan: nama event koneksi HARUS dari daftar yang didukung SDK
            # (binance_common.SUPPORTED_CONNECTION_EVENTS):
            # {'ping', 'open', 'reconnect', 'pong', 'close', 'error'}
            # ('reconnected'/'closed' -> ValueError dan bot mati saat startup).
            streams.on_connection("open", lambda *a, **kw: logger.info("WebSocket terhubung"))
            streams.on_connection("reconnect", lambda *a, **kw: logger.warning("WebSocket RECONNECTED"))
            streams.on_connection("error", lambda *a, **kw: logger.error(f"WebSocket error: {a}"))
            streams.on_connection("close", lambda *a, **kw: logger.warning("WebSocket connection closed"))
            self._ws_started = True

        t_start = time.time()
        for idx, sym in enumerate(symbols, start=1):
            low = sym.lower()

            # --- kline 1m (push tiap detik; flag x=True saat candle close) ---
            h = await self._subscribe_with_pace(
                lambda low=low: streams.kline(symbol=low, interval=interval_ws),
                f"{sym}:kline")
            h.on("message", self._make_kline_cb(sym))
            self._stream_handles.append(h)

            # --- aggTrade: setiap trade yang tereksekusi ---
            h = await self._subscribe_with_pace(
                lambda low=low: streams.agg_trade(symbol=low),
                f"{sym}:aggTrade")
            h.on("message", self._make_trade_cb(sym))
            self._stream_handles.append(h)

            # --- partial depth: top-N level order book ---
            h = await self._subscribe_with_pace(
                lambda low=low: streams.partial_book_depth(symbol=low,
                                                           levels=levels),
                f"{sym}:depth")
            h.on("message", self._make_book_cb(sym))
            self._stream_handles.append(h)

            # --- ticker 24h: update statistik harian tiap detik ---
            h = await self._subscribe_with_pace(
                lambda low=low: streams.ticker(symbol=low),
                f"{sym}:ticker")
            h.on("message", self._make_ticker_cb(sym))
            self._stream_handles.append(h)

            if idx % 25 == 0:
                logger.info(f"Subscribe berjalan: {idx}/{len(symbols)} simbol "
                            f"({idx * 4}/{len(symbols) * 4} stream)...")

        logger.info(f"Berlangganan {len(symbols)} x 4 stream = "
                    f"{len(symbols) * 4} stream selesai dalam "
                    f"{time.time() - t_start:.0f}s")

        # Validasi nama stream: tangkap dini bug "nama stream rusak" yang
        # gagalnya SUNYI (server terima SUBSCRIBE tapi tak ada data).
        try:
            subs = await streams.list_subscribe()
            names = (subs or {}).get("result") if isinstance(subs, dict) else None
            if names is not None:
                bad = [n for n in names if "Enum" in str(n)]
                if bad:
                    logger.error(f"!! {len(bad)} nama stream RUSAK terdeteksi "
                                 f"(contoh: {bad[:2]}) - data tidak akan "
                                 f"mengalir utk stream itu!")
                else:
                    logger.info(f"Validasi stream OK: {len(names)} langganan "
                                f"aktif, nama valid")
        except Exception as exc:
            logger.debug(f"validasi list_subscribe dilewati: {exc}")

        self._watchdog_task = asyncio.create_task(self._watchdog(), name="ws-watchdog")

    # Callback sync: konversi model SDK -> dataclass internal, tulis ke buffer.
    def _make_kline_cb(self, sym: str) -> Callable:
        def cb(k):
            try:
                kk = k.k
                if kk is None:
                    return
                self._cbs["candle"](sym, Candle(
                    open_time=kk.t, close_time=kk.T,
                    open=_f(kk.o), high=_f(kk.h), low=_f(kk.l), close=_f(kk.c),
                    volume=_f(kk.v), quote_volume=_f(kk.q), trades=int(_f(kk.n)),
                    taker_buy_volume=_f(kk.V), closed=bool(kk.x),
                ))
            except Exception as exc:
                logger.debug(f"callback kline {sym} error: {exc}")
        return cb

    def _make_trade_cb(self, sym: str) -> Callable:
        def cb(t):
            try:
                self._cbs["trade"](sym, Trade(
                    ts=t.T, price=_f(t.p), qty=_f(t.q), buyer_is_maker=bool(t.m),
                ))
            except Exception as exc:
                logger.debug(f"callback trade {sym} error: {exc}")
        return cb

    def _make_book_cb(self, sym: str) -> Callable:
        def cb(b):
            try:
                # partial depth stream TIDAK menyertakan simbol -> ditangkap via closure
                bids = [(_f(lvl[0]), _f(lvl[1])) for lvl in (b.bids or [])]
                asks = [(_f(lvl[0]), _f(lvl[1])) for lvl in (b.asks or [])]
                self._cbs["book"](sym, BookSnapshot(ts=int(time.time() * 1000),
                                                    bids=bids, asks=asks))
            except Exception as exc:
                logger.debug(f"callback book {sym} error: {exc}")
        return cb

    def _make_ticker_cb(self, sym: str) -> Callable:
        def cb(t):
            try:
                self._cbs["ticker"](sym, Ticker24h(
                    ts=t.E, symbol=sym, last_price=_f(t.c),
                    price_change_pct=_f(t.P), high=_f(t.h), low=_f(t.l),
                    volume=_f(t.v), quote_volume=_f(t.q), trade_count=int(_f(t.n)),
                    bid=_f(t.b), ask=_f(t.a),
                ))
            except Exception as exc:
                logger.debug(f"callback ticker {sym} error: {exc}")
        return cb

    async def _watchdog(self) -> None:
        """
        Pengawas langganan: kalau semua stream mati (koneksi putus lebih
        lama dari kemampuan reconnect SDK, dsb.), coba langgan ulang dari
        awal. Jalur pemulihannya self-healing: bila pool koneksi pun kosong,
        _subscribe_with_pace akan membangun ulang koneksinya.
        """
        while True:
            await asyncio.sleep(60)
            await self._watchdog_once()

    async def _watchdog_once(self) -> None:
        """Satu siklus watchdog: cek langganan, pulihkan bila mati."""
        try:
            subs = await self._client.websocket_streams.list_subscribe()
            if isinstance(subs, dict) and "result" in subs:
                # bentuk balasan sebenarnya: {"result": [nama, ...], "id": ..}
                total = len(subs.get("result") or [])
            elif isinstance(subs, dict):
                total = sum(len(v) for v in subs.values()
                            if isinstance(v, (list, dict)))
            else:
                # pool koneksi kosong (list_subscribe tak punya koneksi
                # untuk ditanya) -> anggap semua mati, pulihkan.
                total = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"Watchdog: list_subscribe gagal ({exc}) - "
                           f"anggap semua stream mati, coba pulihkan")
            total = 0

        if total == 0 and self._subscribed_symbols:
            logger.error("Semua stream mati! Mencoba berlangganan ulang...")
            try:
                await self.subscribe(
                    self._subscribed_symbols, **{
                        "on_candle": self._cbs["candle"],
                        "on_trade": self._cbs["trade"],
                        "on_book": self._cbs["book"],
                        "on_ticker": self._cbs["ticker"],
                    })
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"Watchdog: pemulihan langganan gagal "
                               f"({exc}) - coba lagi dalam 60s")

    # -------------------------------------------------------------- akun
    async def get_quote_balance(self) -> tuple[float, float]:
        acct = await self._rest(self._client.rest_api.get_account)
        free = locked = 0.0
        for bal in acct.get("balances", []):
            if bal.get("asset") == self.quote_asset:
                free, locked = _f(bal.get("free")), _f(bal.get("locked"))
                break
        return free, locked

    # -------------------------------------------------------------- trading
    async def market_buy(self, symbol: str, quote_qty: float) -> Fill:
        resp = await self._rest(self._client.rest_api.new_order,
                                symbol=symbol,
                                side=NewOrderSideEnum("BUY"),
                                type=NewOrderTypeEnum("MARKET"),
                                quote_order_qty=math.floor(quote_qty * 100) / 100,  # 2 desimal
                                new_order_resp_type=NewOrderNewOrderRespTypeEnum("RESULT"))
        return self._fill_from_order(resp, symbol)

    async def market_sell(self, symbol: str, qty: float) -> Fill:
        resp = await self._rest(self._client.rest_api.new_order,
                                symbol=symbol,
                                side=NewOrderSideEnum("SELL"),
                                type=NewOrderTypeEnum("MARKET"),
                                quantity=qty,
                                new_order_resp_type=NewOrderNewOrderRespTypeEnum("RESULT"))
        return self._fill_from_order(resp, symbol)

    async def place_limit_buy(self, symbol: str, qty: float, price: float) -> int:
        resp = await self._rest(self._client.rest_api.new_order,
                                symbol=symbol,
                                side=NewOrderSideEnum("BUY"),
                                type=NewOrderTypeEnum("LIMIT"),
                                time_in_force=NewOrderNewOrderRespTypeEnum("GTC") and "GTC",
                                quantity=qty, price=price)
        return int(_f(resp.get("orderId")))

    async def get_order_status(self, symbol: str, order_id: int) -> dict:
        resp = await self._rest(self._client.rest_api.get_order,
                                symbol=symbol, order_id=order_id)
        return {
            "status": resp.get("status", ""),
            "executed_qty": _f(resp.get("executedQty")),
            "avg_price": _f(resp.get("price")) or _f(resp.get("cummulativeQuoteQty")),
            "quote_qty": _f(resp.get("cummulativeQuoteQty")),
        }

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        try:
            await self._rest(self._client.rest_api.delete_order,
                             symbol=symbol, order_id=order_id)
            return True
        except (BadRequestError, ClientError) as exc:
            # -2011 "Unknown order sent" = order memang sudah tidak ada
            logger.debug(f"cancel_order {symbol}#{order_id}: {getattr(exc, 'error_message', exc)}")
            return True
        except Exception as exc:
            logger.error(f"Gagal cancel order {symbol}#{order_id}: {exc}")
            return False

    async def cancel_all_orders(self, symbol: str) -> bool:
        """Batalkan semua order terbuka simbol ini (dipakai pemulihan restart)."""
        try:
            await self._rest(self._client.rest_api.delete_open_orders,
                             symbol=symbol)
            return True
        except Exception as exc:
            logger.error(f"Gagal cancel semua order {symbol}: {exc}")
            return False

    def _fill_from_order(self, resp: dict, symbol: str) -> Fill:
        """Ekstrak info fill dari respons order (new_order_resp_type=RESULT)."""
        executed_qty = _f(resp.get("executedQty"))
        quote_qty = _f(resp.get("cummulativeQuoteQty"))
        avg_price = (quote_qty / executed_qty) if executed_qty > 0 else _f(resp.get("price"))

        # Fee dari daftar fills; komisi beli dibayar dalam BASE asset,
        # komisi jual dalam QUOTE asset -> konversikan semuanya ke quote.
        fee_quote = 0.0
        for f in resp.get("fills", []) or []:
            comm = _f(f.get("commission"))
            asset = f.get("commissionAsset", "")
            price = _f(f.get("price")) or avg_price
            if asset == self.quote_asset:
                fee_quote += comm
            else:
                fee_quote += comm * price  # estimasi
        return Fill(symbol=symbol, price=avg_price, qty=executed_qty,
                    quote_qty=quote_qty, fee_quote=fee_quote,
                    order_id=int(_f(resp.get("orderId"))))

    # -------------------------------------------------------------- OCO
    async def place_oco_sell(self, symbol: str, qty: float, tp_price: float,
                             stop_price: float) -> int:
        """
        OCO sell klasik (POST /api/v3/order/oco):
          - leg TP  : LIMIT_MAKER @ tp_price
          - leg SL  : STOP_LOSS_LIMIT, trigger @ stop_price,
                      limit @ stop_price*(1-buffer) supaya tetap terisi
                      saat pasar jatuh cepat.
        """
        sl_limit = stop_price * (1.0 - self._sl_limit_buffer_pct / 100.0)
        resp = await self._rest(self._client.rest_api.order_oco,
                                symbol=symbol,
                                side=OrderOcoSideEnum("SELL"),
                                quantity=qty,
                                price=tp_price,
                                stop_price=stop_price,
                                stop_limit_price=sl_limit,
                                stop_limit_time_in_force=OrderOcoStopLimitTimeInForceEnum("GTC"))
        return int(_f(resp.get("orderListId")))

    async def cancel_oco(self, symbol: str, order_list_id: int) -> bool:
        try:
            await self._rest(self._client.rest_api.delete_order_list,
                             symbol=symbol, order_list_id=order_list_id)
            return True
        except (BadRequestError, ClientError) as exc:
            logger.debug(f"cancel_oco {symbol}#{order_list_id}: {getattr(exc, 'error_message', exc)}")
            return True
        except Exception as exc:
            logger.error(f"Gagal cancel OCO {symbol}#{order_list_id}: {exc}")
            return False

    async def get_oco_status(self, symbol: str, order_list_id: int) -> dict:
        resp = await self._rest(self._client.rest_api.get_order_list,
                                order_list_id=order_list_id)
        return self._parse_oco_status(resp)

    @staticmethod
    def _parse_oco_status(resp: dict) -> dict:
        """Normalisasi respons get_order_list -> ringkasan status OCO."""
        list_status = resp.get("listStatusType", "")
        filled_qty = 0.0
        filled_quote = 0.0
        which = ""
        any_filled = False
        for o in resp.get("orders", []) or []:
            qty = _f(o.get("executedQty"))
            if o.get("status") == "FILLED" and qty > 0:
                any_filled = True
                filled_qty += qty
                filled_quote += _f(o.get("cummulativeQuoteQty"))
                # LIMIT_MAKER = leg TP; STOP_LOSS_LIMIT = leg SL
                if o.get("type") in ("STOP_LOSS_LIMIT", "STOP_LOSS"):
                    which = "sl"
                elif o.get("type") in ("LIMIT_MAKER", "LIMIT"):
                    which = which or "tp"
        done = list_status in ("EXECUTED", "ALL_DONE", "CANCELLED", "REJECTED", "EXPIRED")
        return {
            "list_status": list_status,
            "done": done,
            "any_filled": any_filled,
            "filled_qty": filled_qty,
            "filled_quote": filled_quote,
            "avg_price": (filled_quote / filled_qty) if filled_qty > 0 else 0.0,
            "which": which,
        }
