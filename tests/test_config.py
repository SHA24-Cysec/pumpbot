"""
Unit test LOADER & VALIDASI KONFIGURASI.

Memastikan konfigurasi salah ketik / berbahaya (risk 0%, sell_pct tidak
total 100%, dsb.) DITOLAK sebelum bot berjalan.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import Config, ConfigError, load_config, validate

CONFIG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "config", "config.yaml")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Bersihkan env yang memengaruhi config supaya test deterministik."""
    monkeypatch.setenv("BINANCE_API_KEY", "test-api-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-api-secret")
    for k in ("DASHBOARD_HOST", "DASHBOARD_PORT", "LOG_LEVEL"):
        monkeypatch.delenv(k, raising=False)


def _base_cfg() -> Config:
    return Config()


# ---------------------------------------------------------------------------
# validasi
# ---------------------------------------------------------------------------

def test_default_config_is_valid():
    errs = validate(_base_cfg())
    assert errs == [], errs


def test_revision_defaults_are_safe_and_match_1_to_2_flow():
    cfg = _base_cfg()
    assert cfg.risk.allow_multiple_positions is False
    assert cfg.risk.max_open_positions == 3
    assert cfg.risk.daily_loss_enabled is False
    assert cfg.stops.mode == "percent" and cfg.stops.percent_pct == 1.0
    assert cfg.take_profit.mode == "rr" and cfg.take_profit.rr == 2.0
    assert cfg.breakeven.trigger_rr == 1.0
    assert cfg.trailing.percent_pct == 0.5


def test_single_yaml_is_valid_and_loads():
    cfg = load_config(CONFIG)
    # Default project WAJIB akun demo: menjalankan bot apa adanya tidak boleh
    # langsung menyentuh uang sungguhan.
    assert cfg.mode == "paper"
    assert cfg.risk.risk_per_trade_pct == 99.0
    assert cfg.signal.score_threshold == 70
    errs = validate(cfg)
    assert errs == [], errs


def test_zero_risk_rejected():
    cfg = _base_cfg()
    cfg.risk.risk_per_trade_pct = 0.0
    errs = validate(cfg)
    assert any("risk_per_trade_pct" in e for e in errs)


def test_negative_risk_rejected():
    cfg = _base_cfg()
    cfg.risk.risk_per_trade_pct = -5.0
    errs = validate(cfg)
    assert any("risk_per_trade_pct" in e for e in errs)


def test_high_risk_has_no_software_cap():
    cfg = _base_cfg()
    cfg.risk.risk_per_trade_pct = 50.0
    errs = validate(cfg)
    assert not any("risk_per_trade_pct" in e for e in errs)


def test_tp_sell_pct_must_total_100():
    cfg = _base_cfg()
    from bot.config import TPTarget
    cfg.take_profit.mode = "multi"
    cfg.take_profit.targets = [TPTarget(1.0, 40), TPTarget(2.0, 40)]  # total 80%
    errs = validate(cfg)
    assert any("100%" in e for e in errs)


def test_bad_mode_rejected():
    cfg = _base_cfg()
    cfg.mode = "demo"
    errs = validate(cfg)
    assert any("mode" in e for e in errs)


def test_mode_testnet_sudah_tidak_didukung():
    """Testnet dihapus: mode yang sah hanya paper (akun demo) dan live."""
    cfg = _base_cfg()
    cfg.mode = "testnet"
    errs = validate(cfg)
    assert any("mode" in e for e in errs)


def test_mode_paper_tidak_butuh_api_key(monkeypatch):
    """Akun demo memakai endpoint publik Binance, jadi tanpa kredensial."""
    cfg = Config(mode="paper")
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    assert validate(cfg) == []


def test_default_config_adalah_paper():
    """Lupa mengisi mode tidak boleh berarti uang sungguhan."""
    assert Config().mode == "paper"


def test_paper_start_balance_harus_positif():
    for bad in (0, -100.0, float("nan"), float("inf"), True, "10000"):
        cfg = _base_cfg()
        cfg.paper.start_balance = bad
        errs = validate(cfg)
        assert any("paper.start_balance" in e for e in errs), bad


def test_paper_slippage_dibatasi_wajar():
    for bad in (-1.0, 501.0, True):
        cfg = _base_cfg()
        cfg.paper.slippage_bps = bad
        errs = validate(cfg)
        assert any("paper.slippage_bps" in e for e in errs), bad
    cfg = _base_cfg()
    cfg.paper.slippage_bps = 0.0          # batas bawah sah (tanpa slippage)
    assert not any("slippage" in e for e in validate(cfg))


def test_paper_reset_on_start_harus_bool():
    cfg = _base_cfg()
    cfg.paper.reset_on_start = "ya"
    assert any("paper.reset_on_start" in e for e in validate(cfg))


def test_unlimited_universe_zero_is_valid():
    cfg = _base_cfg()
    cfg.universe.max_symbols = 0
    assert not any("universe.max_symbols" in e for e in validate(cfg))


def test_negative_or_non_integer_universe_limit_rejected():
    for bad in (-1, 1.5, True, "100"):
        cfg = _base_cfg()
        cfg.universe.max_symbols = bad
        errs = validate(cfg)
        assert any("universe.max_symbols" in e for e in errs), bad


def test_negative_weight_rejected():
    cfg = _base_cfg()
    cfg.signal.weights["volume"] = -0.5
    errs = validate(cfg)
    assert any("weights.volume" in e for e in errs)


def test_bad_stop_bounds_rejected():
    cfg = _base_cfg()
    cfg.stops.min_stop_pct = 5.0
    cfg.stops.max_stop_pct = 1.0           # min > max
    errs = validate(cfg)
    assert any("min_stop_pct" in e for e in errs)


def test_missing_file_raises():
    with pytest.raises(ConfigError):
        load_config("/tmp/tidak/ada.yaml")



def test_live_mode_requires_api_key(monkeypatch):
    cfg = Config(mode="live")
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    errs = validate(cfg)
    assert any("BINANCE_API_KEY" in err for err in errs)
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    assert validate(cfg) == []
