"""
Logika stop loss / take profit / breakeven / trailing - PURE FUNCTIONS
(target unit test utama).

Semua fungsi tanpa side-effect supaya perilakunya mudah diverifikasi:
  * initial_stop()      : SL awal dari struktur atau persen
  * take_profit_levels(): daftar level TP + porsi jual per level
  * breakeven_*()       : kapan & ke mana SL dipindah ke entry
  * trailing_update()   : SL mengikuti harga (persen), monoton naik
"""

from __future__ import annotations

from typing import Optional


# ---------------------------------------------------------------------------
# SL awal
# ---------------------------------------------------------------------------

def initial_stop(entry: float, swing_low: Optional[float], mode: str,
                 percent_pct: float, min_stop_pct: float, max_stop_pct: float) -> float:
    """
    Tentukan stop loss awal.

    mode:
      structure -> di bawah swing low terdekat (dari price action detector).
                   Kalau swing tidak tersedia, fallback ke percent.
      percent   -> persen tetap di bawah entry.

    Hasil SELALU dijepit ke [entry*(1-max_stop_pct), entry*(1-min_stop_pct)]:
      - min_stop_pct mencegah SL absurdly-dekat -> qty meledak
      - max_stop_pct mencegah SL kelewat jauh (pola rusak / data aneh)
    """
    if entry <= 0:
        return 0.0

    if mode == "structure" and swing_low and 0 < swing_low < entry:
        stop = swing_low
    else:
        stop = entry * (1.0 - percent_pct / 100.0)

    floor = entry * (1.0 - max_stop_pct / 100.0)   # SL paling jauh (bawah)
    ceil_ = entry * (1.0 - min_stop_pct / 100.0)   # SL paling dekat (atas)
    return max(floor, min(stop, ceil_))


# ---------------------------------------------------------------------------
# Take profit
# ---------------------------------------------------------------------------

def take_profit_levels(entry: float, stop: float, mode: str, rr: float,
                       targets: list) -> list[dict]:
    """
    Daftar level TP -> [{"price", "sell_pct", "gain_pct"}] urut naik.

    mode:
      multi  -> semua target dari config (partial TP)
      single -> hanya target pertama, porsi 100%
      rr     -> satu target di entry + rr x jarak SL (risk:reward)
    """
    if entry <= 0:
        return []
    out: list[dict] = []

    if mode == "rr":
        dist = entry - stop
        tp = entry + max(rr, 0.1) * dist
        return [{"price": tp, "sell_pct": 100.0,
                 "gain_pct": (tp / entry - 1.0) * 100.0}]

    if not targets:
        return [{"price": entry * 1.02, "sell_pct": 100.0, "gain_pct": 2.0}]

    if mode == "single":
        t = targets[0]
        tp = entry * (1.0 + t.gain_pct / 100.0)
        return [{"price": tp, "sell_pct": 100.0, "gain_pct": t.gain_pct}]

    # mode multi
    for t in targets:
        tp = entry * (1.0 + t.gain_pct / 100.0)
        out.append({"price": tp, "sell_pct": t.sell_pct, "gain_pct": t.gain_pct})
    return out


# ---------------------------------------------------------------------------
# Breakeven
# ---------------------------------------------------------------------------

def breakeven_price(entry: float, buffer_pct: float, fee_pct: float) -> float:
    """
    Harga SL tujuan saat breakeven: entry + buffer.

    Buffer default 0.25% menutup fee round-trip (beli + jual @0.1%)
    plus slippage kecil, sehingga posisi benar-benar tidak rugi.
    """
    # minimal buffer = 2x fee (round trip) supaya selalu imbal positif
    min_buffer = max(buffer_pct, fee_pct * 2.0)
    return entry * (1.0 + min_buffer / 100.0)


def breakeven_trigger_price(entry: float, initial_stop: float,
                            trigger_rr: float = 1.0) -> float:
    """Harga pemicu BE berbasis R.

    Satu R adalah jarak risiko awal (``entry - initial_stop``). Contoh entry
    100 dan SL 99 menghasilkan pemicu 101 saat ``trigger_rr=1``. Dengan TP
    2R, BE dan trailing selalu mulai sebelum target TP, berapa pun jarak SL.
    """
    if entry <= 0 or initial_stop <= 0 or initial_stop >= entry or trigger_rr <= 0:
        return 0.0
    return entry + (entry - initial_stop) * trigger_rr


def should_trigger_breakeven(price: float, entry: float, initial_stop: float,
                             trigger_rr: float = 1.0) -> bool:
    """True bila harga telah mencapai trigger BE berbasis R."""
    trigger = breakeven_trigger_price(entry, initial_stop, trigger_rr)
    return trigger > 0 and price >= trigger


# ---------------------------------------------------------------------------
# Trailing stop
# ---------------------------------------------------------------------------

def trailing_stop_price(highest: float, percent_pct: float) -> float:
    """
    Kandidat SL trailing berdasarkan harga tertinggi sejak entry:
    highest x (1 - percent_pct%).

    (Mode ATR pernah tersedia di sini; dihapus atas permintaan pemilik bot
    karena tidak pernah dipakai dan tidak bisa divalidasi backtest.)
    """
    if highest <= 0:
        return 0.0
    return highest * (1.0 - percent_pct / 100.0)


def update_trailing(current_sl: float, highest: float,
                    percent_pct: float) -> float:
    """
    SL trailing baru. SL TIDAK PERNAH turun (monoton naik) dan tidak boleh
    di bawah SL saat ini. Kandidat hanya dipakai kalau lebih tinggi.
    """
    candidate = trailing_stop_price(highest, percent_pct)
    return max(current_sl, candidate)


def should_update_exit_order(old_sl: float, new_sl: float, update_step_pct: float) -> bool:
    """
    True kalau perubahan SL cukup berarti untuk republish order di exchange
    (menghemat rate limit cancel/replace). Return False jika turun/kecil.
    """
    if new_sl <= old_sl or old_sl <= 0:
        return False
    step = old_sl * update_step_pct / 100.0
    return (new_sl - old_sl) >= step
