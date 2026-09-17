"""Unit test sizing: risk hanya % balance + batas saldo spot fisik."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.models import SymbolFilters
from bot.risk_management.sizing import (
    apply_lot_size,
    clamp_to_available_balance,
    compute_raw_qty,
    size_position,
)

F = SymbolFilters(symbol="TESTUSDT", tick_size=0.0001, step_size=0.001,
                  min_qty=0.001, max_qty=1_000_000, min_notional=5.0)


def test_raw_qty_uses_balance_percentage():
    # balance 10.000, risk 1% (=100), entry 1.00, stop 0.98 -> 5.000 qty.
    assert compute_raw_qty(10_000, 1.0, 1.00, 0.98) == pytest.approx(5_000.0)


def test_raw_qty_scales_with_balance_not_unrealized_pnl():
    assert compute_raw_qty(20_000, 1.0, 1.00, 0.98) == pytest.approx(10_000.0)


def test_raw_qty_zero_when_stop_invalid():
    assert compute_raw_qty(10_000, 1.0, 1.00, 1.00) == 0.0
    assert compute_raw_qty(10_000, 1.0, 1.00, 1.05) == 0.0
    assert compute_raw_qty(10_000, 0.0, 1.00, 0.98) == 0.0


def test_lot_size_floor():
    assert apply_lot_size(4999.999, F) == 4999.999
    step_half = SymbolFilters(symbol="X", step_size=0.5, min_qty=0.5)
    assert apply_lot_size(4999.7, step_half) == 4999.5
    assert apply_lot_size(0.3, step_half) == 0.0


def test_only_physical_balance_caps_order_size():
    # Bukan max exposure configurable: spot cuma dapat membeli sebesar saldo.
    assert clamp_to_available_balance(9_000, 1.0, 500) == pytest.approx(497.5)


def test_size_position_hits_target_risk_when_cash_sufficient():
    r = size_position(balance=10_000, available_quote=10_000, entry=1.0, stop=0.98,
                      risk_pct=1.0, filters=F)
    assert r.qty > 0
    assert r.actual_risk_quote <= 100.0
    assert r.actual_risk_quote > 99.0
    assert r.actual_risk_pct <= 1.0


def test_cash_shortfall_can_only_reduce_risk_never_increase_it():
    # SL 0,5% membutuhkan notional 200% balance untuk risk 1%; spot dibatasi cash.
    r = size_position(balance=10_000, available_quote=10_000, entry=1.0, stop=0.995,
                      risk_pct=1.0, filters=F)
    assert r.notional == pytest.approx(9_950.0)
    assert r.actual_risk_pct == pytest.approx(0.4975)
    assert r.actual_risk_pct < 1.0


def test_size_position_rejected_min_notional():
    r = size_position(balance=10_000, available_quote=4, entry=1.0, stop=0.98,
                      risk_pct=1.0, filters=F)
    assert r.qty == 0.0
    assert "notional" in r.reason.lower()


def test_risk_never_exceeds_target():
    for entry, stop in [(1.0, 0.995), (1.0, 0.99), (0.5, 0.49), (100.0, 97.0)]:
        r = size_position(balance=10_000, available_quote=10_000, entry=entry, stop=stop,
                          risk_pct=1.0, filters=F)
        if r.qty > 0:
            assert r.actual_risk_quote <= 100.0 + 1e-9, (entry, stop, r)
