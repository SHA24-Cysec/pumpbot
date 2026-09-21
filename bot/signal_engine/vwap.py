"""
Anchored VWAP sebagai FILTER entry (bukan sumber skor).

VWAP berlabuh (anchored) dihitung dari sebuah candle ANCHOR sampai candle
closed terakhir. Anchor biasanya adalah awal sebuah pump, sehingga VWAP
mewakili harga rata-rata yang benar benar dibayar pasar sejak pump dimulai:

    VWAP = jumlah(nilai transaksi) / jumlah(volume)

Nilai per candle memakai quote_volume bila tersedia (elemen indeks 7 pada
respons kline Binance Spot, yaitu Quote asset volume), selain itu jatuh ke
typical price: ((high + low + close) / 3) x volume.

Keputusan filter:
    dist_pct = (last_price - vwap) / vwap x 100
    lolos bila min_above_pct <= dist_pct <= max_above_pct (inklusif)

Harga di bawah VWAP berarti penjual mengendalikan sejak anchor, harga terlalu
jauh di atas berarti kita mengejar puncak. Keduanya ditolak.

Modul ini murni: anchored_vwap() dan find_anchor() bisa dites tanpa buffer.
Kelas VWAPFilter memakai kontrak yang sama seperti detector biasa
(score(buf, cfg) -> DetectorResult) tetapi TIDAK masuk ALL_DETECTORS karena
tidak menyumbang skor.
"""

from __future__ import annotations

import math
from typing import Optional, Sequence

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.models import Candle, DetectorResult


# ---------------------------------------------------------------------------
# Helper murni
# ---------------------------------------------------------------------------

def _finite(x: float) -> bool:
    """True bila x angka finite (bukan NaN maupun inf)."""
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def sanitize_candles(candles: Sequence[Candle]) -> list[Candle]:
    """
    Bersihkan deret candle sebelum dipakai.

    Membuang candle yang belum close, duplikat, atau open_time yang tidak naik
    (bisa terjadi setelah reconnect WebSocket mengirim ulang candle lama).
    Urutan asli dipertahankan; yang disimpan adalah kemunculan PERTAMA untuk
    tiap open_time yang naik monoton.
    """
    out: list[Candle] = []
    last_ot: Optional[int] = None
    for c in candles:
        if not getattr(c, "closed", True):
            continue
        ot = getattr(c, "open_time", None)
        if ot is None or not _finite(ot):
            continue
        ot = int(ot)
        if last_ot is not None and ot <= last_ot:
            continue
        if not (_finite(c.high) and _finite(c.low) and _finite(c.close)
                and _finite(c.volume)):
            continue
        out.append(c)
        last_ot = ot
    return out


def candle_value(c: Candle) -> float:
    """Nilai transaksi satu candle (quote volume, atau typical price x volume)."""
    qv = float(getattr(c, "quote_volume", 0.0) or 0.0)
    if _finite(qv) and qv > 0:
        return qv
    typical = (float(c.high) + float(c.low) + float(c.close)) / 3.0
    return typical * float(c.volume)


def anchored_vwap(candles: Sequence[Candle],
                  anchor_idx: int) -> tuple[float, float, int]:
    """
    Hitung VWAP dari anchor_idx (inklusif) sampai candle terakhir.

    Hanya candle dengan volume > 0 yang dijumlahkan. Mengembalikan
    (vwap, total_volume, jumlah_candle_terpakai). vwap 0.0 berarti tidak
    terdefinisi (total volume 0 atau rentang kosong).

    Candle yang diterima di sini diasumsikan sudah lewat sanitize_candles.
    """
    if anchor_idx < 0 or anchor_idx >= len(candles):
        return 0.0, 0.0, 0
    tot_val = 0.0
    tot_vol = 0.0
    used = 0
    for c in candles[anchor_idx:]:
        vol = float(c.volume)
        if not _finite(vol) or vol <= 0:
            continue
        val = candle_value(c)
        if not _finite(val):
            continue
        tot_val += val
        tot_vol += vol
        used += 1
    if tot_vol <= 0 or not _finite(tot_val):
        return 0.0, tot_vol if _finite(tot_vol) else 0.0, used
    vwap = tot_val / tot_vol
    if not _finite(vwap) or vwap <= 0:
        return 0.0, tot_vol, used
    return vwap, tot_vol, used


def _impulse_low_idx(candles: Sequence[Candle], win_start: int) -> Optional[int]:
    """Indeks candle dengan low TERENDAH dalam window (kemunculan pertama)."""
    best_i: Optional[int] = None
    best_low = math.inf
    for i in range(win_start, len(candles)):
        low = float(candles[i].low)
        if _finite(low) and low < best_low:
            best_low = low
            best_i = i
    return best_i


def _pump_indices(candles: Sequence[Candle], win_start: int,
                  pump_volume_mult: float, pump_baseline_candles: int) -> list[int]:
    """
    Daftar indeks candle yang "berlonjak" di dalam window.

    Syarat candle k: volume_k >= pump_volume_mult x rata rata volume dari
    pump_baseline_candles candle SEBELUM k, dan close_k > open_k (bullish).
    Candle yang baseline-nya tidak lengkap atau nol dilewati.
    """
    out: list[int] = []
    base_n = max(int(pump_baseline_candles), 1)
    for k in range(win_start, len(candles)):
        start = k - base_n
        if start < 0:
            continue
        window = candles[start:k]
        if len(window) < base_n:
            continue
        vols = [float(c.volume) for c in window if _finite(c.volume)]
        if len(vols) < base_n:
            continue
        avg = sum(vols) / base_n
        if avg <= 0:
            continue
        c = candles[k]
        if float(c.volume) >= pump_volume_mult * avg and float(c.close) > float(c.open):
            out.append(k)
    return out


def find_anchor(candles: Sequence[Candle], *, anchor_mode: str,
                anchor_lookback_candles: int, pump_volume_mult: float,
                pump_baseline_candles: int, pump_max_gap_candles: int,
                no_anchor_action: str, manual_anchors: Optional[dict] = None,
                symbol: str = "") -> tuple[Optional[int], str, str]:
    """
    Tentukan indeks candle anchor.

    Return (anchor_idx, anchor_mode_used, reason). anchor_idx None berarti
    tidak ada anchor (pemanggil mengikuti no_anchor_action = "block").

    Window = anchor_lookback_candles candle TERAKHIR (inklusif candle terakhir).
    """
    n = len(candles)
    if n == 0:
        return None, anchor_mode, "tidak ada candle"
    win = max(int(anchor_lookback_candles), 1)
    win_start = max(0, n - win)

    def _fallback(reason: str) -> tuple[Optional[int], str, str]:
        if no_anchor_action == "impulse_low":
            idx = _impulse_low_idx(candles, win_start)
            if idx is None:
                return None, "impulse_low", reason + "; impulse_low gagal"
            return idx, "impulse_low", reason + "; fallback impulse_low"
        return None, anchor_mode, reason + "; diblokir"

    if anchor_mode == "impulse_low":
        idx = _impulse_low_idx(candles, win_start)
        if idx is None:
            return None, "impulse_low", "window kosong"
        return idx, "impulse_low", "low terendah dalam window"

    if anchor_mode == "manual":
        anchors = manual_anchors or {}
        ts = anchors.get(symbol)
        if ts is None:
            return _fallback("simbol tidak ada di manual_anchors")
        ts = int(ts)
        # Cari candle yang open_time-nya tepat sama, atau candle terakhir yang
        # open_time <= ts (anchor bisa menunjuk waktu di tengah candle).
        idx: Optional[int] = None
        for i in range(win_start, n):
            if int(candles[i].open_time) <= ts:
                idx = i
            else:
                break
        if idx is None:
            return _fallback("anchor manual di luar window")
        return idx, "manual", "anchor manual"

    # ---- anchor_mode == "pump_start" ----
    pumps = _pump_indices(candles, win_start, pump_volume_mult,
                          pump_baseline_candles)
    if not pumps:
        return _fallback("tidak ada lonjakan volume")
    # Rantai mundur dari lonjakan TERBARU selama jaraknya <= gap maksimum.
    gap = max(int(pump_max_gap_candles), 0)
    chain_start = pumps[-1]
    for prev in reversed(pumps[:-1]):
        if chain_start - prev <= gap:
            chain_start = prev
        else:
            break
    return chain_start, "pump_start", "awal rantai lonjakan"


def evaluate_vwap(candles: Sequence[Candle], last_price: float, *,
                  symbol: str, vcfg) -> dict:
    """
    Hitung anchor, VWAP, dan keputusan filter untuk satu deret candle.

    Bagian yang bergantung pada last_price (dist_pct dan passed) selalu
    dihitung ulang; bagian VWAP-nya bisa di-cache oleh pemanggil.
    """
    base = vwap_core(candles, symbol=symbol, vcfg=vcfg)
    return decide(base, last_price, vcfg)


def needed_candles(vcfg) -> int:
    """Berapa candle terakhir yang benar benar dibaca filter.

    Anchor dicari di window anchor_lookback_candles terakhir, dan deteksi
    lonjakan butuh pump_baseline_candles candle sebelum kandidat paling awal.
    Riwayat yang lebih tua tidak pernah mengubah hasil, jadi irisan ini aman
    sekaligus membuat biaya per panggilan tidak tumbuh dengan panjang buffer.
    """
    return max(int(vcfg.anchor_lookback_candles), 1) + \
        max(int(vcfg.pump_baseline_candles), 1)


def vwap_core(candles: Sequence[Candle], *, symbol: str, vcfg) -> dict:
    """Bagian yang HANYA bergantung pada candle (bisa di-cache)."""
    need = needed_candles(vcfg)
    if len(candles) > need:
        candles = candles[-need:]
    clean = sanitize_candles(candles)
    idx, mode_used, reason = find_anchor(
        clean,
        anchor_mode=vcfg.anchor_mode,
        anchor_lookback_candles=vcfg.anchor_lookback_candles,
        pump_volume_mult=vcfg.pump_volume_mult,
        pump_baseline_candles=vcfg.pump_baseline_candles,
        pump_max_gap_candles=vcfg.pump_max_gap_candles,
        no_anchor_action=vcfg.no_anchor_action,
        manual_anchors=vcfg.manual_anchors,
        symbol=symbol,
    )
    if idx is None:
        return {"vwap": 0.0, "anchor_open_time": 0, "anchor_mode_used": mode_used,
                "n_candles": 0, "insufficient": True, "reason": reason}
    vwap, tot_vol, used = anchored_vwap(clean, idx)
    n_since = len(clean) - idx
    info = {
        "vwap": vwap,
        "anchor_open_time": int(clean[idx].open_time),
        "anchor_mode_used": mode_used,
        "n_candles": n_since,
        "insufficient": False,
        "reason": reason,
    }
    if n_since < max(int(vcfg.min_anchor_candles), 1):
        info["insufficient"] = True
        info["reason"] = (f"candle sejak anchor {n_since} < "
                          f"min {vcfg.min_anchor_candles}")
    elif tot_vol <= 0 or vwap <= 0 or used == 0:
        info["insufficient"] = True
        info["reason"] = "total volume 0"
    return info


def decide(core: dict, last_price: float, vcfg) -> dict:
    """Gabungkan hasil VWAP dengan harga terakhir menjadi keputusan filter."""
    out = dict(core)
    out["price"] = float(last_price) if _finite(last_price) else 0.0
    out["dist_pct"] = 0.0
    insufficient = bool(core.get("insufficient"))
    if not _finite(last_price) or float(last_price) <= 0:
        insufficient = True
        out["reason"] = "last_price tidak valid"
    if insufficient:
        allow = (getattr(vcfg, "on_insufficient_data", "block") == "allow")
        out["passed"] = allow
        out["insufficient"] = True
        if allow:
            out["reason"] = f"data kurang ({out.get('reason', '-')}) -> allow"
        else:
            out["reason"] = f"data kurang ({out.get('reason', '-')}) -> block"
        return out

    vwap = float(core["vwap"])
    dist = (float(last_price) - vwap) / vwap * 100.0
    if not _finite(dist):
        out["passed"] = (getattr(vcfg, "on_insufficient_data", "block") == "allow")
        out["insufficient"] = True
        out["reason"] = "dist_pct tidak finite"
        return out
    out["dist_pct"] = round(dist, 4)
    # Batas INKLUSIF. Toleransi kecil dipakai supaya galat pembulatan float
    # (mis. 1.08 / 1.0 - 1 = 8.000000000000007) tidak menolak nilai tepat batas.
    eps = 1e-9
    if dist < float(vcfg.min_above_pct) - eps:
        out["passed"] = False
        out["reason"] = (f"jarak {dist:.1f}% < min {float(vcfg.min_above_pct):.1f}%")
    elif dist > float(vcfg.max_above_pct) + eps:
        out["passed"] = False
        out["reason"] = (f"jarak {dist:.1f}% > max {float(vcfg.max_above_pct):.1f}%")
    else:
        out["passed"] = True
        out["reason"] = f"jarak {dist:.1f}% di zona"
    return out


def params_key(vcfg) -> tuple:
    """Tuple parameter VWAP untuk kunci cache (harus hashable)."""
    anchors = vcfg.manual_anchors or {}
    return (
        vcfg.anchor_mode, int(vcfg.anchor_lookback_candles),
        float(vcfg.pump_volume_mult), int(vcfg.pump_baseline_candles),
        int(vcfg.pump_max_gap_candles), vcfg.no_anchor_action,
        tuple(sorted((str(k), int(v)) for k, v in anchors.items())),
        float(vcfg.min_above_pct), float(vcfg.max_above_pct),
        int(vcfg.min_anchor_candles), vcfg.on_insufficient_data,
    )


# ---------------------------------------------------------------------------
# Filter dengan kontrak detector
# ---------------------------------------------------------------------------

class VWAPFilter:
    """
    Gate entry berbasis Anchored VWAP.

    Kontrak sama dengan detector: score(buf, cfg) -> DetectorResult.
    Tetapi skor di sini hanya penanda (100 lolos, 0 ditolak) dan TIDAK
    dimasukkan ke rata rata tertimbang engine.

    Cache per simbol memakai kunci (simbol, open_time candle terakhir, jumlah
    candle, tuple parameter). Perbandingan dengan last_price selalu dihitung
    ulang supaya keputusan mengikuti harga terbaru tanpa I/O.
    """

    name = "vwap"

    def __init__(self) -> None:
        self._cache: dict[str, tuple[tuple, dict]] = {}


    def clear_cache(self) -> None:
        self._cache.clear()

    def core(self, buf: SymbolBuffer, cfg: Config) -> dict:
        """Bagian VWAP yang di-cache (tanpa last_price)."""
        vcfg = cfg.signal.vwap
        # Kunci cache dibangun TANPA menyalin deque: len() dan indexing deque
        # keduanya O(1), sedangkan list(deque) O(n) dan akan dibayar tiap
        # panggilan meski hasilnya sudah ada di cache.
        n = len(buf.candles)
        last_ot = int(buf.candles[-1].open_time) if n else 0
        key = (buf.symbol, last_ot, n, params_key(vcfg))
        hit = self._cache.get(buf.symbol)
        if hit is not None and hit[0] == key:
            return hit[1]
        # Salin ke list dulu (seperti detector lain) sebelum diiris.
        core = vwap_core(list(buf.candles), symbol=buf.symbol, vcfg=vcfg)
        self._cache[buf.symbol] = (key, core)
        return core

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        vcfg = cfg.signal.vwap
        core = self.core(buf, cfg)
        info = decide(core, buf.last_price, vcfg)
        passed = bool(info["passed"])
        details = {
            "vwap": round(float(info.get("vwap", 0.0)), 10),
            "price": round(float(info.get("price", 0.0)), 10),
            "dist_pct": float(info.get("dist_pct", 0.0)),
            "anchor_open_time": int(info.get("anchor_open_time", 0)),
            "anchor_mode_used": info.get("anchor_mode_used", vcfg.anchor_mode),
            "n_candles": int(info.get("n_candles", 0)),
            "passed": passed,
            "reason": info.get("reason", ""),
        }
        return DetectorResult(self.name, score=100.0 if passed else 0.0,
                              eligible=passed, details=details)


# Filter TIDAK ikut ALL_DETECTORS: bukan sumber skor.
FILTER_DETECTORS = {"vwap": VWAPFilter}
