"""
Utilitas bersama tools backtest.

Berisi pemuat config yang aman untuk backtest dan konversi baris CSV menjadi
objek Candle milik bot.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from bot.config import Config, load_config
from bot.models import Candle

# Kunci dummy hanya untuk melewati validasi load_config (mode live/testnet
# mensyaratkan BINANCE_API_KEY). Backtest TIDAK pernah memakai kunci ini dan
# tidak pernah menyentuh endpoint berkunci.
_DUMMY_ENV = {
    "BINANCE_API_KEY": "backtest-dummy-key",
    "BINANCE_API_SECRET": "backtest-dummy-secret",
}


def load_backtest_config(path: str = os.path.join("config", "config.yaml")) -> Config:
    """
    Muat config.yaml tanpa mengubah bot/config.py.

    Validasi bot menolak mode live atau testnet bila API key tidak ada, jadi
    pemanggilan dibungkus environment dummy sementara.
    """
    with patch.dict(os.environ, _DUMMY_ENV):
        return load_config(path)


def row_to_candle(row: dict) -> Candle:
    """Ubah satu baris CSV backtest menjadi Candle bot (selalu sudah close)."""
    return Candle(
        open_time=int(row["open_time"]),
        close_time=int(row["close_time"]),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row["volume"]),
        quote_volume=float(row["quote_volume"]),
        trades=int(row["trades"]),
        taker_buy_volume=float(row["taker_buy_volume"]),
        closed=True,
    )


def rows_to_candles(rows: list[dict]) -> list[Candle]:
    """Ubah daftar baris CSV menjadi daftar Candle terurut naik."""
    return [row_to_candle(r) for r in rows]
