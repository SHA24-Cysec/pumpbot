"""Regresi parity tools/backtest terhadap aturan entry bot live."""

from bot.models import Candle
from tools.backtest.engine import CandleScore, Combo, simulate_combo

T0 = 1_700_000_000_000
MIN = 60_000


def _candles() -> list[Candle]:
    # Candle entry (index 1) flat agar posisi tidak segera menyentuh TP/SL.
    return [
        Candle(T0 + i * MIN, T0 + (i + 1) * MIN - 1, 100, 100.1, 99.9, 100,
               100, 10_000, 50, 50, True)
        for i in range(8)
    ]


def _score() -> CandleScore:
    return CandleScore(ts=T0 + MIN - 1, entry_idx=1, entry_price=100.0,
                       close=100.0, score=80.0, eligible=True, veto=False,
                       swing_low=98.0)


def test_backtest_defaults_match_live_strategy():
    combo = Combo(threshold=70, sl_pct=1.0)
    assert combo.max_open == 1
    assert combo.tp_rr == 2.0
    assert combo.be_rr == 1.0
    assert combo.trail_pct == 0.5


def test_default_backtest_accepts_only_one_simultaneous_position():
    score = _score()
    candles = {sym: _candles() for sym in ("AAAUSDT", "BBBUSDT")}
    trades = simulate_combo({sym: [score] for sym in candles}, candles,
                            Combo(threshold=70, sl_pct=2.5),
                            equity=10_000, ts0=0, ts1=T0 + 10 * MIN)
    assert len(trades) == 1


def test_optional_multi_position_allows_up_to_three_unique_pairs():
    score = _score()
    candles = {sym: _candles() for sym in ("AAAUSDT", "BBBUSDT", "CCCUSDT")}
    trades = simulate_combo(
        {sym: [score] for sym in candles}, candles,
        Combo(threshold=70, sl_pct=2.5, max_open=3),
        equity=10_000, ts0=0, ts1=T0 + 10 * MIN)
    assert len(trades) == 3
    assert len({t.symbol for t in trades}) == 3
