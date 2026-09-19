"""
Unit test DUST SWEEP (konversi sisa koin kecil -> BNB).

Memakai wallet client PALSU (metode sinkron seperti SDK asli) sehingga
test berjalan offline dan memverifikasi logika inti:
  - filter aset (USDT/BNB tidak ikut, batas nilai min_value_usd)
  - pemecahan kelompok maks 10 aset per request
  - pencatatan event ke database
  - tidak ada dust -> tidak ada panggilan transfer
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from bot.config import load_config
from bot.database.db import Database
from bot.dust import MAX_ASSETS_PER_TRANSFER, DustSweeper

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CFG = os.path.join(_ROOT, "config", "config.yaml")


class _Resp:
    """Meniru respons SDK: punya .to_dict() (camelCase seperti JSON asli)."""

    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data


class FakeWallet:
    """Wallet client palsu; metode SINKRON seperti binance-sdk-wallet."""

    def __init__(self, convertible):
        self.convertible = convertible   # list detail utk dust-btc
        self.transfers: list[str] = []   # catatan argumen dust_transfer

    def get_assets_that_can_be_converted_into_bnb(self):
        return _Resp({"details": self.convertible})

    def dust_transfer(self, asset):
        self.transfers.append(asset)
        results = [{"fromAsset": a, "transferedAmount": "0.0001",
                    "amount": "1", "serviceChargeAmount": "0.00001",
                    "tranId": 1, "operateTime": 0}
                   for a in asset.split(",")]
        return _Resp({"totalTransfered": "0.0001",
                      "totalServiceCharge": "0.0",
                      "transferResult": results})


def _detail(asset, amount, to_btc):
    return {"asset": asset, "assetFullName": asset, "amountFree": str(amount),
            "toBTC": str(to_btc), "toBNB": "0.0001",
            "toBNBOffExchange": "0.0001", "exchange": "0.0001"}


@pytest.fixture()
def cfg():
    c = load_config(_CFG)          # mode testnet/live
    return c


@pytest.fixture()
def db():
    return Database(os.path.join(tempfile.mkdtemp(), "dust.db"))


async def _btc_price():
    return 50_000.0   # 1 BTC = 50.000 USDT


def _run(coro):
    import asyncio
    return asyncio.run(coro)


def test_dust_dikonversi_dan_tercatat(cfg, db):
    # 3 dust kecil (nilai < 1 USDT) + USDT (quote, skip) + BNB (skip)
    wallet = FakeWallet([
        _detail("ALPHA", 1.5, 0.00001),   # 0.50 USDT -> dust
        _detail("BETA", 2.0, 0.00002),    # 1.00 USDT -> TEPAT 1.0, bukan < 1 -> skip
        _detail("GAMMA", 9.0, 0.000005),  # 0.25 USDT -> dust
        _detail("USDT", 0.5, 0.00001),    # quote -> skip
        _detail("BNB", 0.01, 0.00001),    # BNB -> skip
    ])
    sw = DustSweeper(cfg, db, wallet, _btc_price)
    hasil = _run(sw.sweep_once())
    # hanya ALPHA & GAMMA (BETA bernilai >= min_value_usd 1.0)
    assert hasil is not None
    assert sorted(hasil["assets"]) == ["ALPHA", "GAMMA"]
    assert wallet.transfers == ["ALPHA,GAMMA"]
    # event tercatat di DB
    row = db._conn.execute(
        "SELECT type, message FROM events WHERE type='DUST_SWEEP'").fetchone()
    assert row is not None and "ALPHA" in row[1] and "GAMMA" in row[1]


def test_aset_bernilai_besar_tidak_disapu(cfg, db):
    # satu aset bernilai 500 USDT -> BUKAN dust, tidak boleh dikonversi
    wallet = FakeWallet([_detail("PEPE", 500_000, 0.01)])  # 0.01 BTC = 500 USDT
    sw = DustSweeper(cfg, db, wallet, _btc_price)
    assert _run(sw.sweep_once()) is None
    assert wallet.transfers == []


def test_chunk_maksimal_10_aset_per_request(cfg, db):
    assets = [f"COIN{i}" for i in range(23)]
    wallet = FakeWallet([_detail(a, 1.0, 0.000001) for a in assets])
    sw = DustSweeper(cfg, db, wallet, _btc_price)
    hasil = _run(sw.sweep_once())
    assert hasil is not None and len(hasil["assets"]) == 23
    assert len(wallet.transfers) == 3                      # 10 + 10 + 3
    for call in wallet.transfers:
        assert len(call.split(",")) <= MAX_ASSETS_PER_TRANSFER
    assert wallet.transfers[0].startswith("COIN0,")
    assert wallet.transfers[2] == "COIN20,COIN21,COIN22"


def test_tidak_ada_dust(cfg, db):
    wallet = FakeWallet([])
    sw = DustSweeper(cfg, db, wallet, _btc_price)
    assert _run(sw.sweep_once()) is None
    assert wallet.transfers == []


def test_harga_btc_tidak_tersedia_filter_nilai_dilewati(cfg, db):
    """Kalau harga BTC tidak diketahui (0), semua aset convertible ikut."""
    async def no_price():
        return 0.0
    wallet = FakeWallet([_detail("ALPHA", 1.5, 0.5)])   # "25000 USDT" kalau dihitung
    sw = DustSweeper(cfg, db, wallet, no_price)
    hasil = _run(sw.sweep_once())
    assert hasil is not None and hasil["assets"] == ["ALPHA"]
