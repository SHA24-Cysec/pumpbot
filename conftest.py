"""Setup bersama untuk seluruh test.

- Menaruh direktori project di sys.path.
- MENGISOLASI test dari file .env sungguhan milik user.

Isolasi .env penting: load_config() memanggil load_dotenv() untuk membaca
BINANCE_API_KEY dari .env. Tanpa isolasi, test yang sengaja
MENGHAPUS env var (mis. test "mode live tanpa API key harus ditolak")
menjadi bergantung pada isi .env di mesin tempat test dijalankan:
.env berisi API key -> key terisi ulang -> error yang diharapkan tidak
pernah muncul -> test gagal walau kodenya benar.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))


@pytest.fixture(autouse=True)
def _isolasi_dotenv(monkeypatch):
    """Isolasi test dari .env dan sediakan kredensial dummy untuk validasi."""
    monkeypatch.setenv("BINANCE_API_KEY", "test-api-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-api-secret")
    try:
        import dotenv
        monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **kw: False)
    except ImportError:
        pass
