"""Tes untuk tools.backtest.signals_probe (diagnostik edge sinyal)."""

from __future__ import annotations

import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.models import Candle                                     # noqa: E402
from tools.backtest.signals import Entry                          # noqa: E402
from tools.backtest.signals_probe import (_norm_cdf, compare,
                                          forward_returns,
                                          random_entries, summarize,
                                          verdict)                 # noqa: E402

T0 = 1_700_000_000_000
MIN = 60_000


def candle(i: int, price: float) -> Candle:
    """Candle flat di satu harga (open==high==low==close==price)."""
    return Candle(open_time=T0 + i * MIN, close_time=T0 + (i + 1) * MIN - 1,
                  open=price, high=price, low=price, close=price,
                  volume=10.0, quote_volume=10.0 * price, trades=5,
                  taker_buy_volume=5.0, closed=True)


def make_series(prices: list[float]) -> dict[str, list[Candle]]:
    return {"AAAUSDT": [candle(i, p) for i, p in enumerate(prices)]}


# ---------------------------------------------------------------------------
# forward_returns
# ---------------------------------------------------------------------------

def test_forward_returns_harga_dan_indeks_benar():
    """Keluaran = close pada entry_idx + step, masuk = entry_open."""
    data = make_series([100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
    ent = [Entry("AAAUSDT", 1, T0 + MIN, 101.0, 80.0)]
    vals, skipped = forward_returns(ent, data, horizon_min=3, interval_min=1)
    assert skipped == 0
    # entry idx 1 -> keluar di idx 1+3=4, harga 104 -> + (104-101)/101*100
    assert vals == pytest.approx([(104.0 - 101.0) / 101.0 * 100.0])


def test_forward_returns_ujung_data_dilewati():
    """Entry yang tidak punya cukup candle ke depan dihitung skipped."""
    data = make_series([100.0, 100.0, 100.0, 100.0])
    ent = [Entry("AAAUSDT", 1, T0, 100.0, 80.0),
           Entry("AAAUSDT", 3, T0, 100.0, 80.0)]
    vals, skipped = forward_returns(ent, data, horizon_min=2, interval_min=1)
    assert len(vals) == 1 and skipped == 1


def test_forward_returns_simbol_hilang_dan_harga_nol():
    """Simbol tidak dikenal dan entry_open nol tidak menyebabkan crash."""
    data = make_series([100.0, 100.0, 100.0])
    ent = [Entry("ZZZUSDT", 0, T0, 100.0, 80.0),
           Entry("AAAUSDT", 0, T0, 0.0, 80.0)]
    vals, skipped = forward_returns(ent, data, 1, 1)
    assert vals == [] and skipped == 2


def test_forward_returns_step_minimal_satu():
    """Horizon lebih kecil dari interval tetap melompat 1 candle."""
    data = make_series([100.0, 110.0, 121.0])
    ent = [Entry("AAAUSDT", 0, T0, 100.0, 80.0)]
    vals, skipped = forward_returns(ent, data, horizon_min=0, interval_min=5)
    assert skipped == 0 and vals == pytest.approx([10.0])


# ---------------------------------------------------------------------------
# random_entries
# ---------------------------------------------------------------------------

def test_random_entries_jumlah_dan_reproduksi():
    data = make_series([100.0 + i for i in range(120)])
    a = random_entries(data, 25, seed=7, tail_step=10)
    b = random_entries(data, 25, seed=7, tail_step=10)
    assert len(a) == 25 and len(b) == 25
    assert a == b                       # seed sama -> identik
    for e in a:
        assert 0 <= e.entry_idx <= len(data["AAAUSDT"]) - 1 - 10
        assert e.entry_open == data["AAAUSDT"][e.entry_idx].open
        assert e.ts_entry == data["AAAUSDT"][e.entry_idx].open_time


def test_random_entries_seed_beda_hasil_beda():
    data = make_series([100.0 + i for i in range(120)])
    a = random_entries(data, 25, seed=1)
    b = random_entries(data, 25, seed=2)
    assert a != b


def test_random_entries_data_kecil_aman():
    """Simbol yang lebih pendek dari min_idx tidak ikut, tidak crash."""
    data = {"KECILUSDT": [candle(i, 1.0) for i in range(5)]}
    assert random_entries(data, 10, seed=3) == []


def test_random_entries_multisimbol_keduanya_terpakai():
    data = {"AAAUSDT": [candle(i, 1.0) for i in range(100)],
            "BBBUSDT": [candle(i, 2.0) for i in range(100)]}
    ent = random_entries(data, 200, seed=9)
    pakai = {e.symbol for e in ent}
    assert pakai == {"AAAUSDT", "BBBUSDT"}


# ---------------------------------------------------------------------------
# summarize dan compare
# ---------------------------------------------------------------------------

def test_summarize_angka_dasar():
    import statistics as st
    vals = [1.0, -2.0, 3.0, -4.0, 5.0]
    s = summarize(vals)
    assert s["n"] == 5
    assert s["mean"] == pytest.approx(st.fmean(vals))
    assert s["median"] == pytest.approx(1.0)
    assert s["pct_pos"] == pytest.approx(60.0)
    assert s["std"] == pytest.approx(st.stdev(vals))
    assert s["se"] == pytest.approx(st.stdev(vals) / math.sqrt(5))
    assert s["lo"] == pytest.approx(s["mean"] - 1.96 * s["se"])
    assert s["hi"] == pytest.approx(s["mean"] + 1.96 * s["se"])


def test_summarize_kosong_dan_tunggal_aman():
    assert summarize([])["n"] == 0
    s = summarize([2.5])
    assert s["mean"] == 2.5 and s["std"] == 0.0 and s["se"] == 0.0


def test_norm_cdf_titik_kunci():
    assert _norm_cdf(0.0) == pytest.approx(0.5)
    assert _norm_cdf(1.96) == pytest.approx(0.975, abs=1e-3)


def test_compare_himpunan_identik_p_satu():
    a = summarize([0.1, 0.2, 0.3])
    assert compare(a, a)["p"] == 1.0


def test_compare_beda_besar_p_kecil():
    import random as _r
    rng = _r.Random(5)
    a = summarize([rng.gauss(1.0, 0.1) for _ in range(2000)])
    b = summarize([rng.gauss(0.0, 0.1) for _ in range(2000)])
    c = compare(a, b)
    assert c["diff"] > 0.9 and c["p"] < 0.001


def test_compare_tanpa_varians_aman():
    a = summarize([1.0])
    b = summarize([2.0])
    c = compare(a, b)
    assert c["p"] == 1.0


# ---------------------------------------------------------------------------
# verdict
# ---------------------------------------------------------------------------

def _sig(mean, se, n=100):
    return {"n": n, "mean": mean, "median": mean, "pct_pos": 55.0,
            "std": se * math.sqrt(n), "se": se,
            "lo": mean - 1.96 * se, "hi": mean + 1.96 * se}


def test_verdict_edge_melebihi_fee():
    s = _sig(0.5, 0.05)                    # CI bawah 0.402 > 0.2
    assert verdict(s, {"p": 0.001, "diff": 0.1}, 0.2) == "EDGE MELEBIHI FEE"


def test_verdict_positif_tipis_di_bawah_fee():
    s = _sig(0.1, 0.02)                    # CI bawah 0.0608 > 0 tapi < 0.2
    assert verdict(s, {"p": 0.01, "diff": 0.05}, 0.2) == "POSITIF TAPI DI BAWAH FEE"


def test_verdict_tidak_signifikan():
    s = _sig(0.5, 0.05)
    assert verdict(s, {"p": 0.4, "diff": 0.1}, 0.2) == "TIDAK SIGNIFIKAN / NOL"
    s2 = _sig(-0.3, 0.02)
    assert verdict(s2, {"p": 0.001, "diff": 0.01}, 0.2) == "TIDAK SIGNIFIKAN / NOL"


def test_verdict_data_kurang():
    s = _sig(1.0, 0.01, n=10)
    assert verdict(s, {"p": 0.001}, 0.2) == "DATA KURANG (n<30)"


def test_verdict_selisih_negatif_tidak_boleh_jadi_edge():
    """CI di atas fee tetapi sinyal KALAH dari acak -> bukan edge."""
    s = _sig(1.0, 0.02)                    # CI bawah jauh di atas fee
    c = {"p": 0.001, "diff": -0.5}         # tapi selisih vs acak negatif
    assert verdict(s, c, 0.2) == "LEBIH BURUK DARI ACAK"
