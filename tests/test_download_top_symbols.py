"""
Test offline untuk BinanceGateway.get_universe: memastikan pendekatan
"exchangeInfo -> ticker24hr(symbols=[...]) per kelompok 100" berfungsi untuk
SEMUA bentuk respons SDK (model pydantic via to_dict, dict, maupun fallback
raw list).

Latar belakang: ticker/24hr tanpa parameter adalah jalur lama; di production
responsnya tidak lagi dijamin dict sehingga 'tuple' object has no attribute
'get' terjadi. Test ini memvalidasi jalur pengganti.

Catatan: test untuk downloader backtest kini ada di
tests/test_backtest_download.py (downloader baru memakai urllib, bukan SDK).
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                "..")))


class _FakeApiResponse:
    """Meniru ApiResponse SDK: punya .data()."""

    def __init__(self, payload):
        self._payload = payload

    def data(self):
        return self._payload


class _FakeTickerModel:
    """Meniru model pydantic hasil SDK (punya .to_dict())."""

    def __init__(self, **kw):
        self._kw = kw

    def to_dict(self):
        return dict(self._kw)


class _FakeRestAPI:
    def __init__(self, ticker_rows, chunk_calls):
        # ticker_rows: dict symbol -> quote_volume
        self._ticker_rows = ticker_rows
        self._chunk_calls = chunk_calls
        self.ticker_chunks: list[list[str]] = []

    def exchange_info(self):
        syms = []
        for i, (sym, _) in enumerate(self._ticker_rows.items()):
            syms.append({"symbol": sym, "status": "TRADING"})
            if i % 3 == 0:                      # selipkan non-TRADING
                syms.append({"symbol": sym + "HALT", "status": "HALT"})
        syms.append({"symbol": "USDCUSDT", "status": "TRADING"})  # stable
        # simbol eksotis non-ASCII + lowercase: sah di exchangeInfo tapi
        # DITOLAK regex param symbols Binance (-1100) -> wajib tersaring
        syms.append({"symbol": "币安人生USDT", "status": "TRADING"})
        syms.append({"symbol": "牛来USDT", "status": "TRADING"})
        syms.append({"symbol": "ethusdt", "status": "TRADING"})
        return _FakeApiResponse({"symbols": syms})

    def ticker24hr(self, symbols=None):
        assert symbols, "ticker24hr TIDAK boleh dipanggil tanpa symbols"
        assert len(symbols) <= 100, "chunk wajib <= 100 simbol"
        self.ticker_chunks.append(list(symbols))
        rows = []
        for s in symbols:
            if s not in self._ticker_rows:
                continue
            # bentuk MODEL (jalur normal SDK) — utk setiap simbol
            rows.append(_FakeTickerModel(
                symbol=s, quoteVolume=str(self._ticker_rows[s]),
                lastPrice="1.0", closeTime=1700000000000))
        # selipkan baris raw list (format tak terduga) — harus di-skip
        rows.append(["BTCUSDT", "1", "2"])
        return _FakeApiResponse(rows)


class _FakeClient:
    def __init__(self, ticker_rows, chunk_calls):
        self.rest_api = _FakeRestAPI(ticker_rows, chunk_calls)


class TestGetUniverseChunking(unittest.TestCase):
    """Gateway bot memakai pendekatan chunk yang sama."""

    def _make_gateway(self, fake):
        from bot.exchange.binance_gateway import BinanceGateway, _to_plain

        gw = BinanceGateway.__new__(BinanceGateway)   # tanpa __init__/network
        gw.quote_asset = "USDT"
        gw._client = fake

        async def _fake_rest(fn, *args, **kwargs):
            out = fn(*args, **kwargs)          # sinkron di test
            return _to_plain(out)

        gw._rest = _fake_rest
        return gw

    def test_universe_chunked_and_parsed(self):
        rows = {f"COIN{i:03d}USDT": 500 - i for i in range(150)}
        fake = _FakeClient(rows, None)
        gw = self._make_gateway(fake)

        universe = asyncio.run(gw.get_universe())

        # 150 pair -> 2 chunk; stable & HALT & raw-row ter-exclude
        self.assertEqual(len(fake.rest_api.ticker_chunks), 2)
        self.assertEqual(len(universe), 150)
        self.assertEqual(universe[0].symbol, "COIN000USDT")
        self.assertEqual(universe[0].quote_volume, 500.0)

    def test_universe_fallback_ke_jalur_lama_saat_400(self):
        """Kalau symbols= ditolak (400), gateway harus jatuh ke panggilan
        tanpa-parameter dan tetap mengembalikan universe yang benar."""
        rows = {f"COIN{i:03d}USDT": 500 - i for i in range(150)}
        legacy_calls = {"n": 0}

        class _LegacyRestAPI(_FakeRestAPI):
            def ticker24hr(self, symbols=None):
                if symbols:                     # jalur chunked -> tolak
                    class _BadRequestError(Exception):
                        error_message = "rejected by WAF"
                        status_code = -1100
                    raise _BadRequestError()
                legacy_calls["n"] += 1          # jalur lama -> jawab
                return _FakeApiResponse([
                    {"symbol": s, "quoteVolume": str(qv),
                     "lastPrice": "1", "closeTime": 1700000000000,
                     "priceChangePercent": "1", "highPrice": "2",
                     "lowPrice": "1", "volume": "3", "count": 7,
                     "bidPrice": "1", "askPrice": "1"}
                    for s, qv in rows.items()
                ] + [                         # baris racun -> harus di-skip
                    ["X", "1"],                # raw row bukan dict
                    {"symbol": "币安人生USDT", "quoteVolume": "999",
                     "lastPrice": "1", "closeTime": 1, "count": 1},
                    {"symbol": "USDCUSDT", "quoteVolume": "999",
                     "lastPrice": "1", "closeTime": 1, "count": 1},
                ])

        fake = _FakeClient.__new__(_FakeClient)
        fake.rest_api = _LegacyRestAPI(rows, None)
        gw = self._make_gateway(fake)

        universe = asyncio.run(gw.get_universe())

        self.assertEqual(legacy_calls["n"], 1)
        self.assertEqual(len(universe), 150)
        self.assertEqual(universe[0].quote_volume, 500.0)


if __name__ == "__main__":
    unittest.main()
