"""
Test AKUN DEMO (PaperGateway) - mode `paper` pengganti testnet.

Semua test di sini OFFLINE dan deterministik: sumber data pasar
(BinanceGateway) di-patch dengan stub, lalu harga pasar disuntik manual.
Yang diuji adalah bagian yang menentukan benar/tidaknya hasil akun demo:

  1. Tidak ada API key yang dipakai dan URL yang dituju adalah domain
     market-data-only Binance (mustahil mengirim order sungguhan).
  2. Akuntansi dompet: beli, jual, fee, slippage, dan kekekalan dana.
  3. Locked balance saat OCO terpasang (meniru perilaku Binance Spot).
  4. Pengisian OCO hanya terjadi bila harga pasar sungguhan menyentuhnya,
     termasuk prioritas SL saat TP dan SL tersentuh bersamaan.
  5. Saldo demo bertahan lintas restart lewat state store.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.exchange import paper_gateway as pg_mod          # noqa: E402
from bot.exchange.paper_gateway import PaperGateway       # noqa: E402
from bot.models import BookSnapshot, Candle, Ticker24h, Trade  # noqa: E402


# ---------------------------------------------------------------------------
# Stub sumber data pasar: menggantikan BinanceGateway agar test tidak online
# ---------------------------------------------------------------------------
class _StubMarket:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.subscribed = None
        self.cbs = {}

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def get_symbol_filters(self):
        return {"BTCUSDT": object()}

    async def get_universe(self):
        return [Ticker24h(ts=1, symbol="BTCUSDT", last_price=100.0,
                          price_change_pct=1.0, high=110.0, low=90.0,
                          volume=10.0, quote_volume=1000.0, trade_count=10,
                          bid=99.9, ask=100.1)]

    async def get_klines(self, symbol, interval, limit):
        return [Candle(open_time=0, close_time=59_999, open=1, high=2, low=0.5,
                       close=1.5, volume=1, quote_volume=1, trades=1,
                       taker_buy_volume=0.5, closed=True)]

    async def get_quote_idr_rate(self):
        return {"rate": 16_000.0, "symbol": "USDTIDR", "source": "stub"}

    async def subscribe(self, symbols, on_candle, on_trade, on_book, on_ticker,
                        kline_interval="1m"):
        self.subscribed = list(symbols)
        self.cbs = {"candle": on_candle, "trade": on_trade,
                    "book": on_book, "ticker": on_ticker}


@pytest.fixture
def gw(monkeypatch):
    """PaperGateway dengan sumber data pasar yang di-stub."""
    monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
    g = PaperGateway(quote_asset="USDT", start_balance=10_000.0, fee_pct=0.1,
                     slippage_bps=0.0, sl_limit_buffer_pct=0.0)
    return g


def _feed(g: PaperGateway, symbol: str, bid: float, ask: float,
          last: float | None = None) -> None:
    """Suntik harga pasar seolah datang dari stream publik."""
    q = g._quotes.setdefault(symbol, pg_mod._Quote())
    q.bid, q.ask = bid, ask
    q.last = last if last is not None else (bid + ask) / 2


# ===========================================================================
# 1. KEAMANAN: tanpa API key, hanya domain market data
# ===========================================================================
class TestKeamananModePaper:
    def test_tidak_pernah_mengirim_api_key(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        g = PaperGateway(quote_asset="USDT", start_balance=100.0)
        assert g._market.kwargs["api_key"] == ""
        assert g._market.kwargs["api_secret"] == ""
        assert g._market.kwargs["mode"] == "paper"

    def test_url_paper_adalah_domain_market_data_only(self):
        """Domain ini tidak melayani endpoint order, jadi order riil mustahil."""
        from bot.exchange.binance_gateway import (
            MARKET_DATA_REST_URL, MARKET_DATA_WS_URL,
        )
        assert MARKET_DATA_REST_URL == "https://data-api.binance.vision"
        assert MARKET_DATA_WS_URL == "wss://data-stream.binance.vision"

    def test_binance_gateway_menolak_mode_testnet(self):
        from bot.exchange.binance_gateway import BinanceGateway
        with pytest.raises(ValueError):
            BinanceGateway(mode="testnet", api_key="k", api_secret="s")

    def test_mode_atribut_adalah_paper(self, gw):
        assert gw.mode == "paper"

    def test_start_balance_nol_ditolak(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        with pytest.raises(ValueError):
            PaperGateway(start_balance=0.0)


# ===========================================================================
# 2. AKUNTANSI DOMPET
# ===========================================================================
class TestAkuntansiDompet:
    def test_beli_memotong_quote_dan_menambah_base_setelah_fee(self, gw):
        _feed(gw, "BTCUSDT", bid=99.0, ask=100.0)
        fill = asyncio.run(gw.market_buy("BTCUSDT", 1_000.0))

        assert fill.price == pytest.approx(100.0)       # best ask, slippage 0
        assert fill.qty == pytest.approx(10.0)          # 1000 / 100
        assert gw.balance_quote == pytest.approx(9_000.0)
        # fee 0,1% dipotong dari base, persis seperti Binance Spot tanpa BNB
        assert gw.base_free["BTCUSDT"] == pytest.approx(10.0 * 0.999)
        assert fill.fee_quote == pytest.approx(1.0)

    def test_jual_menambah_quote_setelah_fee(self, gw):
        _feed(gw, "BTCUSDT", bid=99.0, ask=100.0)
        asyncio.run(gw.market_buy("BTCUSDT", 1_000.0))
        qty = gw.base_free["BTCUSDT"]

        fill = asyncio.run(gw.market_sell("BTCUSDT", qty))
        assert fill.price == pytest.approx(99.0)        # best bid
        gross = qty * 99.0
        assert gw.balance_quote == pytest.approx(9_000.0 + gross * 0.999)
        assert gw.base_free["BTCUSDT"] == pytest.approx(0.0)

    def test_slippage_memperburuk_kedua_sisi(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        g = PaperGateway(start_balance=10_000.0, fee_pct=0.0, slippage_bps=50.0)
        _feed(g, "BTCUSDT", bid=100.0, ask=100.0)

        buy = asyncio.run(g.market_buy("BTCUSDT", 1_000.0))
        sell = asyncio.run(g.market_sell("BTCUSDT", buy.qty))
        assert buy.price == pytest.approx(100.0 * 1.005)    # beli lebih mahal
        assert sell.price == pytest.approx(100.0 * 0.995)   # jual lebih murah
        assert g.balance_quote < 10_000.0                   # demo tidak optimistis

    def test_beli_melebihi_saldo_ditolak(self, gw):
        _feed(gw, "BTCUSDT", bid=99.0, ask=100.0)
        with pytest.raises(ValueError, match="insufficient balance"):
            asyncio.run(gw.market_buy("BTCUSDT", 10_001.0))
        assert gw.balance_quote == pytest.approx(10_000.0)

    def test_order_tanpa_harga_pasar_ditolak(self, gw):
        with pytest.raises(ValueError, match="belum tersedia"):
            asyncio.run(gw.market_buy("BELUMADAUSDT", 10.0))

    def test_jual_lebih_banyak_dari_saldo_diclamp(self, gw):
        _feed(gw, "BTCUSDT", bid=99.0, ask=100.0)
        asyncio.run(gw.market_buy("BTCUSDT", 100.0))
        punya = gw.base_free["BTCUSDT"]
        fill = asyncio.run(gw.market_sell("BTCUSDT", punya * 5))
        assert fill.qty == pytest.approx(punya)
        assert gw.base_free["BTCUSDT"] == pytest.approx(0.0)

    def test_jual_tanpa_aset_ditolak(self, gw):
        _feed(gw, "BTCUSDT", bid=99.0, ask=100.0)
        with pytest.raises(ValueError, match="insufficient balance"):
            asyncio.run(gw.market_sell("BTCUSDT", 1.0))

    def test_dana_tidak_pernah_tercipta_dari_ketiadaan(self, monkeypatch):
        """Tanpa fee/slippage, beli lalu jual di harga sama = modal utuh."""
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        g = PaperGateway(start_balance=5_000.0, fee_pct=0.0, slippage_bps=0.0)
        _feed(g, "BTCUSDT", bid=250.0, ask=250.0)
        f = asyncio.run(g.market_buy("BTCUSDT", 2_500.0))
        asyncio.run(g.market_sell("BTCUSDT", f.qty))
        assert g.balance_quote == pytest.approx(5_000.0)


# ===========================================================================
# 3. OCO: locked balance + pengisian oleh harga pasar
# ===========================================================================
class TestOCO:
    def _posisi(self, g, quote=1_000.0):
        _feed(g, "BTCUSDT", bid=100.0, ask=100.0)
        return asyncio.run(g.market_buy("BTCUSDT", quote))

    def test_oco_mengunci_base(self, gw):
        fill = self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        asyncio.run(gw.place_oco_sell("BTCUSDT", qty, tp_price=110.0,
                                      stop_price=95.0))
        free, locked = asyncio.run(gw.get_base_balance("BTCUSDT"))
        assert free == pytest.approx(0.0)
        assert locked == pytest.approx(qty)
        assert fill.qty > 0

    def test_cancel_oco_melepas_kunci(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        oid = asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))
        asyncio.run(gw.cancel_oco("BTCUSDT", oid))
        free, locked = asyncio.run(gw.get_base_balance("BTCUSDT"))
        assert free == pytest.approx(qty)
        assert locked == pytest.approx(0.0)

    def test_oco_tanpa_saldo_cukup_ditolak_dan_tidak_mengunci(self, gw):
        self._posisi(gw, quote=100.0)
        qty = gw.base_free["BTCUSDT"]
        with pytest.raises(ValueError, match="insufficient balance"):
            asyncio.run(gw.place_oco_sell("BTCUSDT", qty * 10, 110.0, 95.0))
        free, locked = asyncio.run(gw.get_base_balance("BTCUSDT"))
        assert free == pytest.approx(qty)      # kunci sementara dikembalikan
        assert locked == pytest.approx(0.0)

    def test_stop_di_atas_harga_pasar_ditolak(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        with pytest.raises(ValueError, match="trigger immediately"):
            asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 200.0, 150.0))

    def test_tp_harus_di_atas_stop(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        with pytest.raises(ValueError):
            asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 90.0, 95.0))

    def test_oco_belum_terisi_saat_harga_di_tengah(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        oid = asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))
        _feed(gw, "BTCUSDT", bid=102.0, ask=102.0)
        gw.poll_orders()
        st = asyncio.run(gw.get_oco_status("BTCUSDT", oid))
        assert st["done"] is False and st["filled_qty"] == 0.0

    def test_tp_terisi_saat_harga_naik(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        oid = asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))
        _feed(gw, "BTCUSDT", bid=111.0, ask=111.0)
        gw.poll_orders()

        st = asyncio.run(gw.get_oco_status("BTCUSDT", oid))
        assert st["which"] == "tp" and st["done"] is True
        assert st["filled_qty"] == pytest.approx(qty)
        assert st["avg_price"] == pytest.approx(110.0)   # terisi di harga TP
        assert gw.base_locked.get("BTCUSDT", 0.0) == pytest.approx(0.0)
        assert gw.balance_quote > 9_000.0

    def test_sl_terisi_di_harga_stop_limit(self, monkeypatch):
        """SL memakai harga stop-limit (lebih buruk), bukan harga trigger."""
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        g = PaperGateway(start_balance=10_000.0, fee_pct=0.0, slippage_bps=0.0,
                         sl_limit_buffer_pct=1.0)
        _feed(g, "BTCUSDT", bid=100.0, ask=100.0)
        asyncio.run(g.market_buy("BTCUSDT", 1_000.0))
        qty = g.base_free["BTCUSDT"]
        oid = asyncio.run(g.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))

        _feed(g, "BTCUSDT", bid=94.0, ask=94.0)
        g.poll_orders()
        st = asyncio.run(g.get_oco_status("BTCUSDT", oid))
        assert st["which"] == "sl"
        assert st["avg_price"] == pytest.approx(95.0 * 0.99)

    def test_sl_diprioritaskan_bila_tp_dan_sl_tersentuh_bersamaan(self, gw):
        """Asumsi konservatif: candle lebar dianggap kena SL lebih dulu."""
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        oid = asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))
        # satu tick yang secara harga memenuhi kedua syarat sekaligus
        gw._quotes["BTCUSDT"].last = 94.0
        gw._quotes["BTCUSDT"].bid = 94.0
        gw._quotes["BTCUSDT"].ask = 94.0
        gw._oco_orders[oid]["tp"] = 93.0          # paksa kedua kondisi benar
        gw.poll_orders()
        assert asyncio.run(gw.get_oco_status("BTCUSDT", oid))["which"] == "sl"

    def test_oco_yang_dibatalkan_tidak_ikut_terisi(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        oid = asyncio.run(gw.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))
        asyncio.run(gw.cancel_oco("BTCUSDT", oid))
        _feed(gw, "BTCUSDT", bid=111.0, ask=111.0)
        gw.poll_orders()
        assert asyncio.run(gw.get_oco_status("BTCUSDT", oid))["filled_qty"] == 0.0

    def test_cancel_all_orders_melepas_semua_kunci(self, gw):
        self._posisi(gw)
        qty = gw.base_free["BTCUSDT"]
        asyncio.run(gw.place_oco_sell("BTCUSDT", qty / 2, 110.0, 95.0))
        asyncio.run(gw.cancel_all_orders("BTCUSDT"))
        free, locked = asyncio.run(gw.get_base_balance("BTCUSDT"))
        assert free == pytest.approx(qty) and locked == pytest.approx(0.0)


# ===========================================================================
# 4. LIMIT ORDER
# ===========================================================================
class TestLimitOrder:
    def test_limit_buy_terisi_saat_harga_turun_menyentuh(self, gw):
        _feed(gw, "BTCUSDT", bid=100.0, ask=100.0)
        oid = asyncio.run(gw.place_limit_buy("BTCUSDT", qty=5.0, price=95.0))

        gw.poll_orders()
        assert asyncio.run(gw.get_order_status("BTCUSDT", oid))["status"] == "NEW"

        _feed(gw, "BTCUSDT", bid=94.0, ask=94.0)
        gw.poll_orders()
        st = asyncio.run(gw.get_order_status("BTCUSDT", oid))
        assert st["status"] == "FILLED"
        assert st["executed_qty"] == pytest.approx(5.0)
        assert gw.balance_quote == pytest.approx(10_000.0 - 5.0 * 95.0)

    def test_limit_buy_dibatalkan_tidak_terisi(self, gw):
        _feed(gw, "BTCUSDT", bid=100.0, ask=100.0)
        oid = asyncio.run(gw.place_limit_buy("BTCUSDT", 5.0, 95.0))
        asyncio.run(gw.cancel_order("BTCUSDT", oid))
        _feed(gw, "BTCUSDT", bid=90.0, ask=90.0)
        gw.poll_orders()
        assert asyncio.run(gw.get_order_status("BTCUSDT", oid))["status"] == "CANCELED"

    def test_limit_buy_melebihi_saldo_ditolak(self, gw):
        _feed(gw, "BTCUSDT", bid=100.0, ask=100.0)
        with pytest.raises(ValueError, match="insufficient balance"):
            asyncio.run(gw.place_limit_buy("BTCUSDT", 1_000.0, 95.0))

    def test_status_order_tak_dikenal_aman(self, gw):
        st = asyncio.run(gw.get_order_status("BTCUSDT", 999_999))
        assert st["status"] == "CANCELED" and st["executed_qty"] == 0.0


# ===========================================================================
# 5. HARGA DARI STREAM PUBLIK
# ===========================================================================
class TestStreamHarga:
    def test_callback_bot_tetap_dipanggil_dan_harga_terekam(self, gw):
        got = {"candle": 0, "trade": 0, "book": 0, "ticker": 0}
        asyncio.run(gw.subscribe(
            ["BTCUSDT"],
            lambda s, c: got.__setitem__("candle", got["candle"] + 1),
            lambda s, t: got.__setitem__("trade", got["trade"] + 1),
            lambda s, b: got.__setitem__("book", got["book"] + 1),
            lambda s, x: got.__setitem__("ticker", got["ticker"] + 1)))
        cbs = gw._market.cbs

        cbs["trade"]("BTCUSDT", Trade(ts=1, price=123.0, qty=1.0,
                                      buyer_is_maker=False))
        cbs["book"]("BTCUSDT", BookSnapshot(ts=1, bids=[(122.0, 1.0)],
                                            asks=[(124.0, 1.0)]))
        cbs["candle"]("BTCUSDT", Candle(open_time=0, close_time=1, open=1,
                                        high=2, low=1, close=125.0, volume=1,
                                        quote_volume=1, trades=1,
                                        taker_buy_volume=1, closed=True))
        cbs["ticker"]("BTCUSDT", Ticker24h(ts=2, symbol="BTCUSDT",
                                           last_price=126.0, price_change_pct=1,
                                           high=1, low=1, volume=1,
                                           quote_volume=1, trade_count=1,
                                           bid=125.5, ask=126.5))

        assert got == {"candle": 1, "trade": 1, "book": 1, "ticker": 1}
        assert gw.last_price("BTCUSDT") == pytest.approx(126.0)
        assert gw._quotes["BTCUSDT"].bid == pytest.approx(125.5)
        assert gw._quotes["BTCUSDT"].ask == pytest.approx(126.5)

    def test_harga_simbol_tak_dikenal_nol(self, gw):
        assert gw.last_price("TIDAKADAUSDT") == 0.0

    def test_get_universe_menyegarkan_harga_acuan(self, gw):
        asyncio.run(gw.get_universe())
        assert gw.last_price("BTCUSDT") == pytest.approx(100.0)


# ===========================================================================
# 6. STATE BERTAHAN LINTAS RESTART
# ===========================================================================
class _MemStore:
    def __init__(self, state=None):
        self.state = state

    def load(self):
        return self.state

    def save(self, state):
        self.state = state


class TestStateAkunDemo:
    def test_saldo_bertahan_setelah_restart(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        store = _MemStore()

        g1 = PaperGateway(start_balance=10_000.0, fee_pct=0.0,
                          slippage_bps=0.0, state_store=store)
        _feed(g1, "BTCUSDT", bid=100.0, ask=100.0)
        asyncio.run(g1.market_buy("BTCUSDT", 2_000.0))
        saldo, aset = g1.balance_quote, g1.base_free["BTCUSDT"]

        g2 = PaperGateway(start_balance=10_000.0, fee_pct=0.0,
                          slippage_bps=0.0, state_store=store)
        assert g2.restored_from_state is True
        assert g2.balance_quote == pytest.approx(saldo)
        assert g2.base_free["BTCUSDT"] == pytest.approx(aset)

    def test_aset_terkunci_dibebaskan_saat_restart(self, monkeypatch):
        """Order OCO virtual hilang saat restart, jadi kuncinya harus lepas."""
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        store = _MemStore()
        g1 = PaperGateway(start_balance=10_000.0, fee_pct=0.0,
                          slippage_bps=0.0, state_store=store)
        _feed(g1, "BTCUSDT", bid=100.0, ask=100.0)
        asyncio.run(g1.market_buy("BTCUSDT", 1_000.0))
        qty = g1.base_free["BTCUSDT"]
        asyncio.run(g1.place_oco_sell("BTCUSDT", qty, 110.0, 95.0))
        assert g1.base_free["BTCUSDT"] == pytest.approx(0.0)

        g2 = PaperGateway(start_balance=10_000.0, fee_pct=0.0,
                          slippage_bps=0.0, state_store=store)
        assert g2.base_free["BTCUSDT"] == pytest.approx(qty)
        assert g2.base_locked == {}

    def test_state_rusak_tidak_membuat_bot_mati(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        store = _MemStore({"version": pg_mod.STATE_VERSION,
                           "quote_asset": "USDT",
                           "balance_quote": "bukan-angka"})
        g = PaperGateway(start_balance=777.0, state_store=store)
        assert g.balance_quote == pytest.approx(777.0)

    def test_state_versi_lain_diabaikan(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        store = _MemStore({"version": 999, "quote_asset": "USDT",
                           "balance_quote": 1.0})
        g = PaperGateway(start_balance=555.0, state_store=store)
        assert g.balance_quote == pytest.approx(555.0)
        assert g.restored_from_state is False

    def test_ganti_quote_asset_tidak_mencampur_state(self, monkeypatch):
        monkeypatch.setattr(pg_mod, "BinanceGateway", _StubMarket)
        store = _MemStore({"version": pg_mod.STATE_VERSION,
                           "quote_asset": "FDUSD", "balance_quote": 1.0,
                           "base_free": {}, "base_locked": {}, "next_id": 1})
        g = PaperGateway(quote_asset="USDT", start_balance=333.0,
                         state_store=store)
        assert g.balance_quote == pytest.approx(333.0)

    def test_tanpa_state_store_tetap_jalan(self, gw):
        _feed(gw, "BTCUSDT", bid=100.0, ask=100.0)
        asyncio.run(gw.market_buy("BTCUSDT", 100.0))
        assert gw.balance_quote == pytest.approx(9_900.0)


# ===========================================================================
# 7. LIFECYCLE
# ===========================================================================
class TestLifecycle:
    def test_start_stop_meneruskan_ke_sumber_data(self, gw):
        async def scenario():
            await gw.start()
            assert gw._market.started is True
            assert gw._watcher is not None
            await gw.stop()
            assert gw._market.stopped is True
            assert gw._watcher is None
        asyncio.run(scenario())

    def test_watcher_mengisi_order_saat_berjalan(self, gw):
        async def scenario():
            _feed(gw, "BTCUSDT", bid=100.0, ask=100.0)
            await gw.market_buy("BTCUSDT", 1_000.0)
            qty = gw.base_free["BTCUSDT"]
            oid = await gw.place_oco_sell("BTCUSDT", qty, 110.0, 95.0)
            await gw.start()
            _feed(gw, "BTCUSDT", bid=111.0, ask=111.0)
            await asyncio.sleep(gw.ORDER_WATCH_INTERVAL_S * 4)
            st = await gw.get_oco_status("BTCUSDT", oid)
            await gw.stop()
            return st
        assert asyncio.run(scenario())["which"] == "tp"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
