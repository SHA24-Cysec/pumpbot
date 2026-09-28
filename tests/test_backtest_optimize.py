"""Test grid search: kanonikalisasi, skor gabungan, diskualifikasi, split OOS."""

from __future__ import annotations

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.models import Candle                                    # noqa: E402
from tools.backtest.engine import Params                         # noqa: E402
from tools.backtest.optimize import (                            # noqa: E402
    DEFAULT_BE_BUFFER, DEFAULT_BE_RR, DEFAULT_SL, DEFAULT_TP_RR,
    DEFAULT_TRAIL, build_grid, canonical, parse_floats,
    score_results, split_chronological, yaml_snippet)
from tools.backtest.util import load_backtest_config             # noqa: E402

T0 = 1_700_000_000_000
MIN = 60_000


@pytest.fixture(scope="module")
def cfg():
    """Config asli repo."""
    return load_backtest_config(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config", "config.yaml"))


def flat(i: int, price: float = 100.0) -> Candle:
    """Candle datar sintetis (helper test)."""
    return Candle(T0 + i * MIN, T0 + (i + 1) * MIN - 1, price, price, price,
                  price, 10.0, 10.0 * price, 5, 5.0, True)


# --------------------------------------------------------------------------
# Kanonikalisasi grid
# --------------------------------------------------------------------------

def test_kanonikalisasi_be_mati_mematikan_buffer_dan_trailing():
    """be_rr 0 -> buffer dan trailing ikut dinolkan."""
    assert canonical(1.0, 2.0, 0.0, 0.5, 0.5) == (1.0, 2.0, 0.0, 0.0, 0.0)


def test_kanonikalisasi_be_rr_lebih_besar_dari_tp_dianggap_mati():
    """be_rr >= tp_rr tidak pernah tercapai sebelum TP -> BE mati."""
    assert canonical(1.0, 1.0, 1.0, 0.3, 0.5) == (1.0, 1.0, 0.0, 0.0, 0.0)
    assert canonical(1.0, 2.0, 3.0, 0.3, 0.5) == (1.0, 2.0, 0.0, 0.0, 0.0)


def test_kanonikalisasi_trail_nol_membuang_beda_trail():
    """trail_pct 0 selalu menghasilkan kunci yang sama."""
    assert canonical(1.0, 2.0, 1.0, 0.3, 0.0) == (1.0, 2.0, 1.0, 0.3, 0.0)


def test_grid_tanpa_duplikat_dan_menghormati_batas_stop(cfg, capsys):
    """sl_pct di luar [min_stop_pct, max_stop_pct] dibuang dengan peringatan."""
    cfg.stops.min_stop_pct = 0.5
    cfg.stops.max_stop_pct = 4.0
    grid = build_grid(cfg, [0.1, 1.0, 9.0], DEFAULT_TP_RR, DEFAULT_BE_RR,
                      DEFAULT_BE_BUFFER, DEFAULT_TRAIL)
    out = capsys.readouterr().out
    assert "0.1" in out and "9.0" in out
    assert all(p.sl_pct == 1.0 for p in grid)
    keys = [p.key() for p in grid]
    assert len(keys) == len(set(keys))


def test_grid_default_lengkap_unik(cfg):
    """Grid default tidak mengandung kombinasi duplikat."""
    grid = build_grid(cfg, DEFAULT_SL, DEFAULT_TP_RR, DEFAULT_BE_RR,
                      DEFAULT_BE_BUFFER, DEFAULT_TRAIL)
    keys = [p.key() for p in grid]
    assert len(keys) == len(set(keys))
    assert len(grid) > 50
    assert all(p.trail_step_pct == cfg.trailing.update_step_pct for p in grid)


def test_parse_floats():
    """Parser daftar angka CLI menangani input kosong dan tidak valid."""
    assert parse_floats("0.5,1,1.5", [9.0]) == [0.5, 1.0, 1.5]
    assert parse_floats("", [9.0]) == [9.0]
    assert parse_floats("abc", [9.0]) == [9.0]


def test_parse_floats_rentang_otomatis():
    """Suku 'awal..akhir:langkah' diperluas menjadi daftar lengkap."""
    assert parse_floats("0.5..2.0:0.25", []) == \
        [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]
    assert parse_floats("1..5", []) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert parse_floats("2..1:0.5", []) == [2.0, 1.5, 1.0]
    assert parse_floats("0.5,2..3:0.5,5", []) == [0.5, 2.0, 2.5, 3.0, 5.0]
    # cacat float 0.1+2*0.05 tidak boleh bocor ke daftar
    assert parse_floats("0.1..0.3:0.05", []) == [0.1, 0.15, 0.2, 0.25, 0.3]


def test_parse_floats_rentang_salah_ditolak():
    """Rentang salah format dan langkah nol harus error keras, bukan skip."""
    import pytest as _pytest
    for tolakan in ("0.5..abc:1", "1...5"):
        with _pytest.raises(ValueError, match="[Rr]entang"):
            parse_floats(tolakan, [])
    with _pytest.raises(ValueError, match="nol"):
        parse_floats("1..5:0", [])
    with _pytest.raises(ValueError, match="10,000"):
        parse_floats("0..1000000:0.001", [])


# --------------------------------------------------------------------------
# Skor gabungan
# --------------------------------------------------------------------------

def _row(trades: int, ret: float, pf: float, dd: float) -> dict:
    """Baris hasil palsu untuk menguji skor gabungan."""
    return {"params": Params(sl_pct=1.0, tp_rr=2.0),
            "metrics": {"trades": trades, "net_return_pct": ret,
                        "profit_factor": pf, "max_dd_pct": dd,
                        "win_rate": 50.0, "avg_r": 0.1, "expectancy": 1.0}}


def test_skor_gabungan_urutan_masuk_akal():
    """Kombinasi terbaik di tiga metrik harus mendapat skor tertinggi."""
    rows = [_row(50, 10.0, 2.0, 5.0), _row(50, 0.0, 1.0, 20.0)]
    score_results(rows, 0.4, 0.3, 0.3, min_trades=30)
    assert rows[0]["score"] == pytest.approx(1.0)
    assert rows[1]["score"] == pytest.approx(0.0)


def test_diskualifikasi_trade_sedikit():
    """Kombinasi dengan trade < min_trades tidak diberi skor."""
    rows = [_row(50, 10.0, 2.0, 5.0), _row(5, 99.0, 9.0, 1.0)]
    score_results(rows, 0.4, 0.3, 0.3, min_trades=30)
    assert rows[0]["score"] is not None
    assert rows[1]["score"] is None
    assert rows[1]["disqualified"] is True


def test_profit_factor_tak_hingga_dipotong():
    """PF inf diperlakukan sama dengan PF 5 (cap) saat normalisasi."""
    rows = [_row(50, 10.0, float("inf"), 5.0), _row(50, 10.0, 5.0, 5.0)]
    score_results(rows, 0.0, 1.0, 0.0, min_trades=30)
    assert rows[0]["score"] == pytest.approx(rows[1]["score"])


def test_semua_nilai_sama_menghasilkan_skor_netral():
    """Normalisasi tidak boleh membagi nol saat semua nilai identik."""
    rows = [_row(50, 5.0, 2.0, 3.0), _row(50, 5.0, 2.0, 3.0)]
    score_results(rows, 0.4, 0.3, 0.3, min_trades=30)
    assert all(r["score"] == pytest.approx(0.5) for r in rows)


# --------------------------------------------------------------------------
# Split OOS
# --------------------------------------------------------------------------

def test_split_kronologis():
    """30 persen data terakhir masuk out of sample, tanpa tumpang tindih."""
    data = {"AAAUSDT": [flat(i) for i in range(100)]}
    is_d, oos_d = split_chronological(data, 0.3)
    assert len(is_d["AAAUSDT"]) + len(oos_d["AAAUSDT"]) == 100
    assert is_d["AAAUSDT"][-1].open_time < oos_d["AAAUSDT"][0].open_time
    assert len(oos_d["AAAUSDT"]) == pytest.approx(30, abs=2)


def test_split_nonaktif():
    """oos 0 berarti seluruh data masuk in sample."""
    data = {"AAAUSDT": [flat(i) for i in range(10)]}
    is_d, oos_d = split_chronological(data, 0.0)
    assert oos_d == {}
    assert is_d == data


def test_split_batas_global_lintas_simbol():
    """Batas potong sama untuk semua simbol (urutan kronologis terjaga)."""
    data = {"AAAUSDT": [flat(i) for i in range(100)],
            "BBBUSDT": [flat(i) for i in range(50, 100)]}
    is_d, oos_d = split_chronological(data, 0.5)
    cut = max(c.open_time for c in is_d["AAAUSDT"])
    for candles in oos_d.values():
        assert all(c.open_time > cut for c in candles)


def test_yaml_snippet_mematikan_fitur_yang_nol(cfg):
    """Snippet YAML menandai breakeven dan trailing mati saat nilainya nol."""
    text = yaml_snippet(Params(sl_pct=1.0, tp_rr=2.0, be_rr=0.0,
                               trail_pct=0.0), cfg)
    assert "breakeven:\n  enabled: false" in text
    assert "trailing:\n  enabled: false" in text
    assert "rr: 2.0" in text
    assert "--" not in text


def test_diskualifikasi_tidak_tertukar_antar_baris_identik():
    """Dua baris dengan metrik identik dinilai menurut jumlah trade sendiri."""
    rows = [_row(50, 10.0, 2.0, 5.0), _row(5, 10.0, 2.0, 5.0),
            _row(50, 10.0, 2.0, 5.0)]
    score_results(rows, 0.4, 0.3, 0.3, min_trades=30)
    assert [r["disqualified"] for r in rows] == [False, True, False]


# --------------------------------------------------------------------------
# Callback kemajuan (progress bar)
# --------------------------------------------------------------------------

def test_load_data_progress_cb(tmp_path):
    """Callback dipanggil per file cache yang dibaca."""
    from tools.backtest.download import write_csv
    from tools.backtest.optimize import load_data
    rows = [{"open_time": T0 + i * MIN, "close_time": T0 + (i + 1) * MIN - 1,
             "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
             "volume": 10.0, "quote_volume": 1000.0, "trades": 5,
             "taker_buy_volume": 5.0} for i in range(3)]
    write_csv(str(tmp_path / "AAAUSDT_1m.csv"), rows)
    write_csv(str(tmp_path / "BBBUSDT_1m.csv"), rows)
    calls: list[int] = []
    data = load_data([], "1m", 30, str(tmp_path), progress_cb=calls.append)
    assert len(data) == 2
    assert calls == [1, 1]


def test_run_grid_progress_cb(cfg):
    """Callback dipanggil per kombinasi pada mode serial."""
    from tools.backtest.optimize import run_grid
    params = [Params(sl_pct=1.0, tp_rr=2.0, be_rr=1.0, be_buffer_pct=0.2,
                     trail_pct=0.0, trail_step_pct=0.15, fee_pct=0.1,
                     slippage_pct=0.0) for _ in range(3)]
    data = {"AAAUSDT": [flat(i) for i in range(50)]}
    calls: list[int] = []
    rows = run_grid(params, [], data, cfg, 1000.0, None, 1,
                    progress_cb=calls.append)
    assert len(rows) == 3
    assert calls == [1, 1, 1]


# --------------------------------------------------------------------------
# Grid sinyal dan varian config
# --------------------------------------------------------------------------

def test_signal_grid_kartesius_tanpa_duplikat(cfg):
    """Ukuran grid = produk panjang daftar, dan semua unik."""
    from tools.backtest.optimize import build_signal_grid
    grid = build_signal_grid(cfg, [40.0, 50.0, 60.0], [0.5, 0.6, 0.7],
                             [10, 20], [3.0], [30], [20], [2], [30])
    assert len(grid) == 3 * 3 * 2
    assert len({sp for sp in grid}) == len(grid)
    sp = grid[0]
    assert sp.threshold == 40.0 and sp.w_pa == 0.5 and sp.ma_period == 10


def test_signal_grid_wpa_tak_valid_dibuang(cfg, capsys):
    """w_pa 0 atau 1 mematikan salah satu detector -> dibuang."""
    from tools.backtest.optimize import build_signal_grid
    grid = build_signal_grid(cfg, [40.0], [0.0, 0.5, 1.0],
                             [20], [3.0], [30], [20], [2], [30])
    assert [sp.w_pa for sp in grid] == [0.5]
    assert "di luar (0, 1)" in capsys.readouterr().out


def test_variant_cfg_menerapkan_nilai_dan_tidak_mengubah_asli(cfg):
    """Varian memuat nilai grid; config asli tetap utuh."""
    from tools.backtest.optimize import SignalParams, variant_cfg
    w_asli = dict(cfg.signal.weights)
    sp = SignalParams(threshold=45.0, w_pa=0.7, ma_period=10,
                      spike_scale=2.5, structure_candles=45,
                      breakout_lookback=15, swing_neighbors=3,
                      min_candles=25)
    v = variant_cfg(cfg, sp)
    assert v.signal.score_threshold == 45.0
    assert v.signal.weights["price_action"] == 0.7
    assert abs(v.signal.weights["volume"] - 0.3) < 1e-12
    assert v.signal.volume.ma_period == 10
    assert v.signal.volume.spike_scale == 2.5
    assert v.signal.price_action.structure_candles == 45
    assert v.signal.price_action.breakout_lookback == 15
    assert v.signal.price_action.swing_neighbors == 3
    assert v.signal.min_candles == 25
    # asli utuh
    assert cfg.signal.weights == w_asli
    assert cfg.signal.volume.ma_period == 20


def test_build_grid_cooldown_menambah_dimensi(cfg):
    """Daftar cooldown mengalikan jumlah kombinasi exit."""
    grid1 = build_grid(cfg, [1.0], [2.0], [0.5], [0.2], [0.0])
    grid2 = build_grid(cfg, [1.0], [2.0], [0.5], [0.2], [0.0],
                       cooldown_list=[0.0, 15.0, 30.0])
    assert len(grid1) == 1
    assert len(grid2) == 3
    assert sorted(p.cooldown_min for p in grid2) == [0.0, 15.0, 30.0]
    assert len({p.key() for p in grid2}) == 3


def test_parse_ints(capsys):
    from tools.backtest.optimize import parse_ints
    assert parse_ints("", [20]) == [20]
    assert parse_ints("10, 20, x, 30", [20]) == [10, 20, 30]
    assert "bukan angka" in capsys.readouterr().out
    assert parse_ints("10..30:10", []) == [10, 20, 30]


def test_csv_memuat_kolom_sinyal(tmp_path):
    """write_results_csv menulis kolom sinyal + cooldown per baris."""
    from tools.backtest.optimize import SignalParams, write_results_csv
    sp = SignalParams(threshold=40.0, w_pa=0.6, ma_period=20,
                      spike_scale=3.0, structure_candles=30,
                      breakout_lookback=20, swing_neighbors=2,
                      min_candles=30)
    params = Params(sl_pct=1.0, tp_rr=2.0, be_rr=1.0, be_buffer_pct=0.2,
                    trail_pct=0.0, trail_step_pct=0.15, fee_pct=0.1,
                    slippage_pct=0.0, cooldown_min=30.0)
    metrics = {"trades": 5, "win_rate": 60.0, "net_return_pct": 1.5,
               "profit_factor": 1.2, "max_dd_pct": 2.0, "avg_r": 0.1,
               "expectancy": 0.5}
    rows = [{"params": params, "metrics": metrics, "signal": sp,
             "score": 0.5, "disqualified": False}]
    out = write_results_csv(rows, str(tmp_path / "r.csv"), {})
    isi = open(out, encoding="utf-8").read().splitlines()
    assert isi[0].startswith("threshold,w_pa,ma_period,spike_scale")
    assert ",30.0,5," in isi[1]      # cooldown_min ikut tercetak
