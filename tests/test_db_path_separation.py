"""Test pemisahan file database per mode dan konfigurasi tunggal.

Path default ``data/pumpbot.db`` dipisahkan otomatis sesuai mode yang dipilih
DI config/config.yaml. Tidak ada profile YAML maupun mode environment override.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import load_config, resolve_db_path  # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG = os.path.join(_ROOT, "config", "config.yaml")


class TestResolveDbPath:
    def test_default_testnet(self):
        assert resolve_db_path("testnet", "data/pumpbot.db") == \
            "data/pumpbot-testnet.db"

    def test_default_live(self):
        assert resolve_db_path("live", "data/pumpbot.db") == \
            "data/pumpbot-live.db"

    def test_default_paper(self):
        assert resolve_db_path("paper", "data/pumpbot.db") == \
            "data/pumpbot-paper.db"

    def test_path_custom_tidak_diubah(self):
        for mode in ("testnet", "live", "paper"):
            assert resolve_db_path(mode, "data/custom.db") == "data/custom.db"
            assert resolve_db_path(mode, "data/pumpbot-live.db") == \
                "data/pumpbot-live.db"

    def test_normalisasi_path(self):
        assert resolve_db_path("live", "./data/pumpbot.db") == \
            "./data/pumpbot-live.db"


class TestSingleConfigIntegration:
    def test_config_satu_satunya_memulai_paper_dengan_db_terpisah(self):
        cfg = load_config(_CONFIG)
        assert cfg.mode == "paper"
        assert cfg.database.path == "data/pumpbot-paper.db"


if __name__ == "__main__":
    import unittest
    unittest.main()
