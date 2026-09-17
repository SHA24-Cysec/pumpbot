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
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
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
    assert cfg.mode == "paper"
    assert cfg.risk.risk_per_trade_pct == 1.0
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


def test_absurd_risk_rejected():
    cfg = _base_cfg()
    cfg.risk.risk_per_trade_pct = 50.0     # 50% per trade = bukan manajemen risiko
    errs = validate(cfg)
    assert any("risk_per_trade_pct" in e for e in errs)


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
