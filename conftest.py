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
    """Netralkan load_dotenv() selama test berjalan.

    Semua test harus deterministik: hasilnya tidak boleh berubah hanya
    karena ada .env (berisi API key testnet/live) di folder project.
    """
    try:
        import dotenv
    except ImportError:
        return  # dotenv tidak terpasang -> load_config memang skip .env
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **kw: False)
