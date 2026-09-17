"""Regresi aturan baru: daily-loss opsional dan posisi tunggal default."""

from bot.config import Config
from bot.models import Position
from bot.risk_management.manager import RiskManager


def _position(trade_id: int, symbol: str = "AAAUSDT") -> Position:
    return Position(
        trade_id=trade_id, symbol=symbol, entry_time=0, entry_price=100.0,
        qty_total=1.0, qty_remaining=1.0, quote_value=100.0,
        stop_loss=99.0, initial_stop=99.0, take_profits=[],
    )


def test_default_allows_only_one_open_position():
    risk = RiskManager(Config())
    allowed, reason = risk.can_open_new([_position(1)], equity=1_000.0, paused=False)
    assert not allowed
    assert "satu posisi" in reason


def test_multi_position_is_opt_in_and_limited_to_three():
    risk = RiskManager(Config())
    assert risk.update_params({"allow_multiple_positions": True, "max_open_positions": 3}) == []
    assert risk.can_open_new([_position(1), _position(2, "BBBUSDT")], 1_000.0, False)[0]
    allowed, reason = risk.can_open_new(
        [_position(1), _position(2, "BBBUSDT"), _position(3, "CCCUSDT")], 1_000.0, False)
    assert not allowed
    assert "maksimum (3)" in reason


def test_daily_loss_disabled_does_not_halt_entries():
    risk = RiskManager(Config())
    risk.roll_day_if_needed(1_000.0)
    halted, _ = risk.check_daily_limit(900.0)  # penurunan 10%, namun fitur off
    assert not halted
    assert not risk.daily_halt_reason


def test_daily_loss_uses_equity_when_explicitly_enabled():
    cfg = Config()
    cfg.risk.daily_loss_enabled = True
    cfg.risk.daily_loss_limit_pct = 3.0
    risk = RiskManager(cfg)
    risk.roll_day_if_needed(1_000.0)
    halted, reason = risk.check_daily_limit(970.0)
    assert halted
    assert "3.00%" in reason
    assert "00:00 UTC" in reason
