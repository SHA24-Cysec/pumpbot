"""Test downloader backtest tanpa jaringan (HTTP dipalsukan)."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import urllib.error

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.backtest import download                              # noqa: E402
from tools.backtest.download import (                            # noqa: E402
    DownloadError, HttpResult, count_gaps, download_symbol, fetch_klines,
    is_tradeable_symbol, merge_rows, normalize_ts, parse_kline_row, read_csv,
    top_symbols, write_csv)
from tools.backtest.util import load_backtest_config             # noqa: E402

T0 = 1_700_000_000_000
MIN = 60_000


@pytest.fixture(scope="module")
def cfg():
    """Config asli repo."""
    return load_backtest_config(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config", "config.yaml"))


def kline_row(open_time: int, price: float = 100.0, micro: bool = False) -> list:
    """Baris klines palsu dengan format 12 elemen seperti Binance."""
    close_time = open_time + MIN - 1
    factor = 1000 if micro else 1
    return [open_time * factor, f"{price}", f"{price + 1}", f"{price - 1}",
            f"{price}", "10.0", close_time * factor, "1000.0", 42, "5.0",
            "500.0", "0"]


# --------------------------------------------------------------------------
# Normalisasi dan parsing
# --------------------------------------------------------------------------

def test_normalisasi_timestamp_mikrodetik():
    """Nilai di atas 1e14 dianggap mikrodetik lalu dibagi 1000."""
    assert normalize_ts(T0) == T0
    assert normalize_ts(T0 * 1000) == T0
    assert normalize_ts(1_500_000_000_000) == 1_500_000_000_000


def test_parse_baris_klines_mikrodetik():
    """Baris dengan timestamp mikrodetik ternormalisasi ke milidetik."""
    row = parse_kline_row(kline_row(T0, micro=True))
    assert row["open_time"] == T0
    assert row["close_time"] == T0 + MIN - 1
    assert row["trades"] == 42


def test_parse_baris_rusak_dilewati():
    """Baris pendek atau berisi nilai tidak valid menghasilkan None."""
    assert parse_kline_row([1, 2, 3]) is None
    assert parse_kline_row([T0, "x", "1", "1", "1", "1", T0, "1", 1, "1", "1",
                            "0"]) is None


def test_filter_simbol():
    """Stablecoin, leveraged token, dan simbol non-ASCII harus ditolak."""
    assert is_tradeable_symbol("BTCUSDT")
    assert not is_tradeable_symbol("USDCUSDT")
    assert not is_tradeable_symbol("BTCUPUSDT")
    assert not is_tradeable_symbol("ETHDOWNUSDT")
    assert not is_tradeable_symbol("ethusdt")
    assert not is_tradeable_symbol("币安人生USDT")
    assert not is_tradeable_symbol("BTCBUSD")


# --------------------------------------------------------------------------
# Paging klines
# --------------------------------------------------------------------------

def test_paging_klines_tanpa_duplikat():
    """Paging memakai open_time terakhir + 1 dan tidak menghasilkan duplikat."""
    total = 2500
    all_rows = [kline_row(T0 + i * MIN) for i in range(total)]
    calls: list[dict] = []

    def fake(path, params, **kw):
        calls.append(dict(params))
        start = params["startTime"]
        end = params["endTime"]
        sel = [r for r in all_rows if start <= r[0] <= end][:1000]
        return HttpResult(payload=sel)

    now = T0 + (total + 5) * MIN
    rows = fetch_klines("AAAUSDT", "1m", T0, T0 + total * MIN,
                        fetcher=fake, now_ms=now)
    assert len(rows) == total
    assert len(calls) == 3
    times = [r["open_time"] for r in rows]
    assert times == sorted(times)
    assert len(set(times)) == len(times)
    # halaman kedua mulai tepat satu milidetik setelah candle terakhir
    assert calls[1]["startTime"] == T0 + 999 * MIN + 1
    assert calls[0]["limit"] == 1000


def test_candle_belum_close_dibuang():
    """Candle yang masih berjalan tidak boleh ikut tersimpan."""
    rows_raw = [kline_row(T0 + i * MIN) for i in range(5)]

    def fake(path, params, **kw):
        return HttpResult(payload=rows_raw)

    # now berada di tengah candle indeks 4 -> candle 4 belum close
    now = T0 + 4 * MIN + 30_000
    rows = fetch_klines("AAAUSDT", "1m", T0, T0 + 10 * MIN,
                        fetcher=fake, now_ms=now)
    assert [r["open_time"] for r in rows] == [T0 + i * MIN for i in range(4)]


def test_interval_tidak_didukung():
    """Interval di luar daftar harus gagal cepat."""
    with pytest.raises(DownloadError):
        fetch_klines("AAAUSDT", "1h", T0, T0 + MIN, fetcher=lambda *a, **k: None)


# --------------------------------------------------------------------------
# Cache CSV dan resume
# --------------------------------------------------------------------------

def test_resume_tanpa_duplikat():
    """Unduhan lanjutan hanya meminta candle baru dan tidak menduplikasi."""
    with tempfile.TemporaryDirectory() as tmp:
        all_rows = [kline_row(T0 + i * MIN) for i in range(20)]
        now = T0 + 25 * MIN

        def fake(path, params, **kw):
            start = params["startTime"]
            end = params["endTime"]
            return HttpResult(payload=[r for r in all_rows
                                       if start <= r[0] <= end][:1000])

        end_ms = T0 + 20 * MIN
        first = download_symbol("AAAUSDT", "1m", T0, T0 + 10 * MIN - 1,
                                data_dir=tmp, fetcher=fake, now_ms=now)
        assert first["rows"] == 10
        assert first["new_rows"] == 10

        second = download_symbol("AAAUSDT", "1m", T0, end_ms,
                                 data_dir=tmp, fetcher=fake, now_ms=now)
        assert second["new_rows"] == 10       # hanya 10 candle baru
        assert second["rows"] == 20
        stored = read_csv(second["path"])
        times = [r["open_time"] for r in stored]
        assert len(times) == len(set(times)) == 20
        assert times == sorted(times)


def test_merge_rows_dedup():
    """merge_rows mengurutkan dan membuang duplikat open_time."""
    a = [{"open_time": 2}, {"open_time": 1}]
    b = [{"open_time": 2}, {"open_time": 3}]
    assert [r["open_time"] for r in merge_rows(a, b)] == [1, 2, 3]


def test_hitung_candle_bolong():
    """count_gaps menghitung candle yang hilang di tengah deret."""
    rows = [{"open_time": T0}, {"open_time": T0 + MIN},
            {"open_time": T0 + 5 * MIN}]
    assert count_gaps(rows, "1m") == 3
    assert count_gaps(rows[:1], "1m") == 0


def test_csv_roundtrip_dan_file_rusak():
    """Cache CSV bisa dibaca kembali; file rusak menghasilkan list kosong."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "AAAUSDT_1m.csv")
        rows = [parse_kline_row(kline_row(T0 + i * MIN)) for i in range(3)]
        write_csv(path, rows)
        assert len(read_csv(path)) == 3
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("bukan,csv,valid\n1,2\n")
        assert read_csv(path) == []
        assert read_csv(os.path.join(tmp, "tidak-ada.csv")) == []


# --------------------------------------------------------------------------
# Daftar simbol dan lapisan HTTP
# --------------------------------------------------------------------------

def test_top_symbols_urut_dan_tersaring(cfg):
    """Simbol diurut volume menurun dan disaring sesuai aturan bot."""
    payload = [
        {"symbol": "AAAUSDT", "quoteVolume": "50000000"},
        {"symbol": "BBBUSDT", "quoteVolume": "90000000"},
        {"symbol": "USDCUSDT", "quoteVolume": "99000000"},   # stable
        {"symbol": "BTCUPUSDT", "quoteVolume": "98000000"},  # leveraged
        {"symbol": "CCCUSDT", "quoteVolume": "100"},         # volume kecil
        {"symbol": "ETHBTC", "quoteVolume": "80000000"},     # bukan USDT
        ["baris", "tak", "terduga"],
    ]

    def fake(path, params, **kw):
        assert path == "/api/v3/ticker/24hr"
        assert params == {"type": "MINI"}
        return HttpResult(payload=payload)

    assert top_symbols(10, cfg, "USDT", fetcher=fake) == ["BBBUSDT", "AAAUSDT"]


def test_top_symbols_hormati_exclude(cfg):
    """exclude_symbols dari config dihormati."""
    cfg.universe.exclude_symbols = ["BBBUSDT"]
    payload = [{"symbol": "AAAUSDT", "quoteVolume": "50000000"},
               {"symbol": "BBBUSDT", "quoteVolume": "90000000"}]
    try:
        out = top_symbols(10, cfg, "USDT",
                          fetcher=lambda p, q, **k: HttpResult(payload=payload))
        assert out == ["AAAUSDT"]
    finally:
        cfg.universe.exclude_symbols = []


def _fake_response(body: bytes, headers: dict):
    """Objek mirip respons urlopen untuk test."""
    class _Resp(io.BytesIO):
        def __init__(self):
            super().__init__(body)
            self.headers = headers

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.close()
            return False
    return _Resp()


def test_http_get_json_membaca_bobot(monkeypatch):
    """Header x-mbx-used-weight-1m dibaca dan dikembalikan."""
    def fake_urlopen(req, timeout=None):
        return _fake_response(json.dumps([1, 2]).encode(),
                              {"x-mbx-used-weight-1m": "77"})
    monkeypatch.setattr(download.urllib.request, "urlopen", fake_urlopen)
    res = download.http_get_json("/api/v3/klines", {"symbol": "AAAUSDT"},
                                 sleeper=lambda s: None)
    assert res.payload == [1, 2]
    assert res.used_weight == 77


def test_http_get_json_tidur_saat_bobot_tinggi(monkeypatch):
    """Bobot mendekati batas memicu jeda sebelum request berikutnya."""
    slept: list[float] = []

    def fake_urlopen(req, timeout=None):
        return _fake_response(b"[]", {"x-mbx-used-weight-1m": "5900"})
    monkeypatch.setattr(download.urllib.request, "urlopen", fake_urlopen)
    download.http_get_json("/api/v3/klines", {}, sleeper=slept.append)
    assert slept and slept[0] > 0


def test_http_400_fail_fast(monkeypatch):
    """HTTP 400 langsung melempar DownloadError tanpa retry."""
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError(req.full_url, 400, "Bad Request",
                                     {}, io.BytesIO(b'{"code":-1121}'))
    monkeypatch.setattr(download.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(DownloadError):
        download.http_get_json("/api/v3/klines", {}, sleeper=lambda s: None)
    assert calls["n"] == 1


def test_http_429_retry_after_lalu_sukses(monkeypatch):
    """HTTP 429 menghormati Retry-After lalu mencoba lagi."""
    slept: list[float] = []
    state = {"n": 0}

    def fake_urlopen(req, timeout=None):
        state["n"] += 1
        if state["n"] == 1:
            raise urllib.error.HTTPError(
                req.full_url, 429, "Too Many Requests",
                {"Retry-After": "7"}, io.BytesIO(b""))
        return _fake_response(b"[1]", {})
    monkeypatch.setattr(download.urllib.request, "urlopen", fake_urlopen)
    res = download.http_get_json("/api/v3/klines", {}, sleeper=slept.append)
    assert res.payload == [1]
    assert 7.0 in slept


def test_error_jaringan_retry_lalu_menyerah(monkeypatch):
    """Error jaringan diulang dengan backoff lalu melempar DownloadError."""
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise urllib.error.URLError("koneksi putus")
    monkeypatch.setattr(download.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(DownloadError):
        download.http_get_json("/api/v3/klines", {}, max_retry=3,
                               sleeper=lambda s: None)
    assert calls["n"] == 3


def test_backfill_riwayat_lebih_panjang():
    """Cache lama tidak boleh membuat bagian awal riwayat terlewat."""
    with tempfile.TemporaryDirectory() as tmp:
        all_rows = [kline_row(T0 + i * MIN) for i in range(30)]
        now = T0 + 40 * MIN

        def fake(path, params, **kw):
            start, end = params["startTime"], params["endTime"]
            return HttpResult(payload=[r for r in all_rows
                                       if start <= r[0] <= end][:1000])

        # unduhan pertama hanya bagian belakang
        download_symbol("AAAUSDT", "1m", T0 + 20 * MIN, T0 + 30 * MIN,
                        data_dir=tmp, fetcher=fake, now_ms=now)
        # lalu minta riwayat yang jauh lebih panjang ke belakang
        info = download_symbol("AAAUSDT", "1m", T0, T0 + 30 * MIN,
                               data_dir=tmp, fetcher=fake, now_ms=now)
        times = [r["open_time"] for r in read_csv(info["path"])]
        assert times == [T0 + i * MIN for i in range(30)]
        assert info["gaps"] == 0


# --------------------------------------------------------------------------
# Callback kemajuan (progress bar)
# --------------------------------------------------------------------------

def test_fetch_klines_progress_cb_per_halaman():
    """Callback dipanggil tiap halaman dengan jumlah candle yang baru masuk."""
    rows = [kline_row(T0 + i * MIN) for i in range(2200)]
    now = T0 + 3000 * MIN
    calls: list[int] = []

    def fake(path, params, **kw):
        start, end = params["startTime"], params["endTime"]
        return HttpResult(payload=[r for r in rows
                                   if start <= r[0] <= end][:1000])

    out = fetch_klines("AAAUSDT", "1m", T0, T0 + 2200 * MIN,
                       fetcher=fake, now_ms=now, progress_cb=calls.append)
    assert len(out) == 2200
    assert calls == [1000, 1000, 200]
    assert sum(calls) == len(out)


def test_download_symbol_meneruskan_progress_cb():
    """download_symbol meneruskan callback ke kedua arah pengisian."""
    with tempfile.TemporaryDirectory() as tmp:
        rows = [kline_row(T0 + i * MIN) for i in range(5)]
        now = T0 + 10 * MIN
        calls: list[int] = []

        def fake(path, params, **kw):
            start = params["startTime"]
            return HttpResult(payload=[r for r in rows if r[0] >= start])

        info = download_symbol("AAAUSDT", "1m", T0, T0 + 5 * MIN,
                               data_dir=tmp, fetcher=fake, now_ms=now,
                               progress_cb=calls.append)
        assert info["new_rows"] == 5
        assert sum(calls) == 5


def test_hitung_baris_cache(tmp_path):
    """Estimasi cache untuk bar per-simbol: baris CSV dikurangi header."""
    from tools.backtest.download import _hitung_baris_cache
    path = str(tmp_path / "AAAUSDT_1m.csv")
    rows = [{"open_time": T0 + i * MIN, "close_time": T0 + (i + 1) * MIN - 1,
             "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
             "volume": 1.0, "quote_volume": 1.0, "trades": 1,
             "taker_buy_volume": 1.0} for i in range(7)]
    write_csv(path, rows)
    assert _hitung_baris_cache(path) == 7
    assert _hitung_baris_cache(str(tmp_path / "tidak_ada.csv")) == 0
