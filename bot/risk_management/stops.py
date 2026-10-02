"""
Logika stop loss / take profit / breakeven / trailing - PURE FUNCTIONS
(target unit test utama).

Semua fungsi tanpa side-effect supaya perilakunya mudah diverifikasi:
  * initial_stop()      : SL awal dari struktur, persen, atau ATR
  * take_profit_levels(): daftar level TP + porsi jual per level
  * breakeven_*()       : kapan & ke mana SL dipindah ke entry
  * update_trailing*()  : SL mengikuti harga (persen atau ATR), monoton naik

TIGA BASIS JARAK yang didukung:

  | basis     | jarak SL            | jarak TP              | trailing      |
  |-----------|---------------------|-----------------------|---------------|
  | percent   | persen tetap        | kelipatan risiko (R)  | persen        |
  | structure | swing low terdekat  | kelipatan risiko (R)  | persen / ATR  |
  | atr       | kelipatan ATR       | kelipatan ATR         | kelipatan ATR |

Nilai ATR yang dipakai bot DIBEKUKAN saat entry (lihat Position.atr_entry)
dan tidak dihitung ulang selama posisi hidup. Alasannya: SL awal, TP, dan
pemicu BE harus tetap pada angka yang sama seperti saat keputusan entry
diambil, sehingga risiko yang sudah diukur saat sizing tidak berubah di
tengah jalan. Untuk perhitungan ATR-nya sendiri lihat bot.risk_management.atr.
"""

from __future__ import annotations

from typing import Optional

# Lantai mutlak jarak SL (persen harga). Bukan pengaman ekonomi, hanya jaring
# terakhir agar SL tidak menempel di entry saat data ATR tidak wajar (mis.
# ATR nyaris nol). Nilai ini juga berlaku untuk mode percent/structure.
ABSOLUTE_MIN_STOP_PCT = 0.02


# ---------------------------------------------------------------------------
# SL awal
# ---------------------------------------------------------------------------

def atr_stop_price(entry: float, atr: float, multiplier: float) -> float:
    """
    Harga SL dari basis ATR: entry - (multiplier x ATR).

    Mengembalikan 0.0 bila ATR atau multiplier tidak bisa dipakai, supaya
    pemanggil tahu bahwa basis ATR TIDAK valid dan harus memakai fallback
    (bukan diam-diam memakai angka nol yang berarti SL di harga 0).
    """
    if entry <= 0 or atr <= 0 or multiplier <= 0:
        return 0.0
    return entry - multiplier * atr


def clamp_stop(entry: float, stop: float, min_stop_pct: float,
               max_stop_pct: float) -> float:
    """
    Jepit harga SL ke [entry x (1 - max_stop_pct%), entry x (1 - min_stop_pct%)].

      - min_stop_pct mencegah SL sangat dekat -> qty meledak saat sizing
      - max_stop_pct mencegah SL kelewat jauh (pola rusak / ATR ekstrem)

    Dijepit juga saat basis ATR: ATR yang tiba-tiba melebar (mis. koin baru
    listing) tidak boleh membuat risiko per trade jauh dari rencana.
    """
    floor = entry * (1.0 - max_stop_pct / 100.0)   # SL paling jauh (bawah)
    ceil_ = entry * (1.0 - min_stop_pct / 100.0)   # SL paling dekat (atas)
    return max(floor, min(stop, ceil_))


def clamp_atr_multiplier(multiplier: float, min_multiplier: float,
                         max_multiplier: float) -> float:
    """
    Jepit kelipatan ATR ke rentang yang diizinkan.

    Kelipatan adalah satuan yang sama dengan basisnya, jadi pengaman untuk
    SL berbasis ATR juga harus berbasis ATR. Kalau memakai pengaman persen
    (min_stop_pct), pada koin bervolatilitas rendah jarak ATR yang sah
    (mis. 0,077% pada BTC) langsung tertimpa batas 0,5% sehingga TP berbasis
    ATR bisa berakhir lebih dekat daripada SL dan rasio imbal terbalik.
    """
    lo = min_multiplier if min_multiplier > 0 else 0.0
    hi = max_multiplier if max_multiplier > 0 else float("inf")
    if hi < lo:
        lo, hi = hi, lo
    return max(lo, min(multiplier, hi))


def initial_stop(entry: float, swing_low: Optional[float], mode: str,
                 percent_pct: float, min_stop_pct: float,
                 max_stop_pct: float, atr: float = 0.0,
                 atr_multiplier: float = 0.0,
                 atr_min_multiplier: float = 0.0,
                 atr_max_multiplier: float = 0.0) -> float:
    """
    Tentukan stop loss awal.

    mode:
      structure -> di bawah swing low terdekat (dari price action detector).
                   Kalau swing tidak tersedia, fallback ke percent.
      percent   -> persen tetap di bawah entry.
      atr       -> entry - (atr_multiplier x ATR), kelipatan dijepit ke
                   [atr_min_multiplier, atr_max_multiplier]. Kalau ATR tidak
                   tersedia (atr <= 0, mis. simbol baru dengan candle kurang
                   dari period+1), fallback ke percent dan pemanggil
                   mencatatnya di log.

    Pengaman per mode:

      percent/structure -> clamp_stop() persen seperti sebelumnya
                           (min_stop_pct mencegah qty meledak).
      atr               -> penjepit KELIPATAN ATR, ditambah satu batas atas
                           mutlak max_stop_pct. Batas atas ini HANYA membuat
                           SL lebih sempit saat ATR ekstrem (mis. koin baru
                           listing), sehingga risiko tetap terbatas; batas
                           bawah persen sengaja TIDAK dipakai karena akan
                           menimpa jarak ATR yang sah.

    Tabel ringkas pengaman yang berlaku per mode:

      | pengaman                | percent | structure | atr          |
      |-------------------------|---------|-----------|--------------|
      | min_stop_pct (bawah)    | ya      | ya        | tidak        |
      | max_stop_pct (atas)     | ya      | ya        | ya (mutlak)  |
      | atr_min/max_multiplier  | tidak   | tidak     | ya           |
      | lantai mutlak 0,02%     | ya      | ya        | ya           |
    """
    if entry <= 0:
        return 0.0

    if mode == "structure" and swing_low and 0 < swing_low < entry:
        return clamp_stop(entry, swing_low, min_stop_pct, max_stop_pct)

    if mode == "atr":
        # atr_multiplier <= 0 berarti pemanggil tidak benar-benar meminta
        # basis ATR (mis. config setengah terisi): itu harus jatuh ke
        # fallback persen, bukan dinaikkan diam-diam oleh atr_min_multiplier
        # menjadi 0,5 x ATR.
        mult = (clamp_atr_multiplier(atr_multiplier, atr_min_multiplier,
                                     atr_max_multiplier)
                if atr_multiplier > 0 else 0.0)
        if atr > 0 and mult > 0:
            # Dihitung sebagai JARAK (satuan harga) supaya arah tiap pengaman
            # tidak ambigu: jarak lebih besar = SL lebih jauh dari entry.
            jarak = mult * atr
            # Batas atas mutlak: ATR raksasa tidak boleh melebarkan risiko
            # melewati max_stop_pct.
            jarak = min(jarak, entry * max_stop_pct / 100.0)
            # Lantai mutlak kecil supaya SL tidak menempel di entry saat ATR
            # nyaris nol (data simbol tidak wajar).
            jarak = max(jarak, entry * ABSOLUTE_MIN_STOP_PCT / 100.0)
            return entry - jarak
        # ATR tidak bisa dipakai -> fallback persen (pemanggil mencatatnya).
        stop = entry * (1.0 - percent_pct / 100.0)
        return clamp_stop(entry, stop, min_stop_pct, max_stop_pct)

    stop = entry * (1.0 - percent_pct / 100.0)
    return clamp_stop(entry, stop, min_stop_pct, max_stop_pct)


# ---------------------------------------------------------------------------
# Take profit
# ---------------------------------------------------------------------------

def take_profit_levels(entry: float, stop: float, mode: str, rr: float,
                       targets: list, atr: float = 0.0,
                       atr_multiplier: float = 0.0) -> list[dict]:
    """
    Daftar level TP -> [{"price", "sell_pct", "gain_pct"}] urut naik.

    mode:
      multi  -> semua target dari config (partial TP)
      single -> hanya target pertama, porsi 100%
      rr     -> satu target di entry + rr x jarak SL (risk:reward)
      atr    -> satu target di entry + (atr_multiplier x ATR), porsi 100%.
                Mengembalikan list KOSONG bila ATR tidak tersedia, supaya
                pemanggil memakai fallback (mode rr) dan tidak memasang TP
                yang tidak berdasar volatilitas.

    Catatan penting soal basis ATR: TP di sini diukur dari ENTRY, bukan dari
    harga terakhir. Jadi TP tetap tidak bergerak walau harga naik turun,
    sama seperti saat SL dihitung.
    """
    if entry <= 0:
        return []
    out: list[dict] = []

    if mode == "atr":
        if atr <= 0 or atr_multiplier <= 0:
            return []
        tp = entry + atr_multiplier * atr
        return [{"price": tp, "sell_pct": 100.0,
                 "gain_pct": (tp / entry - 1.0) * 100.0}]

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


def breakeven_trigger_price_atr(entry: float, atr: float,
                                trigger_atr_mult: float = 1.5) -> float:
    """
    Harga pemicu BE berbasis ATR: entry + (trigger_atr_mult x ATR).

    Dipakai saat jarak SL juga berbasis ATR, supaya BE tetap berada pada
    "1R" milik strategi ATR. Contoh: SL 1.5 x ATR dan trigger 1.5 x ATR
    berarti BE aktif tepat saat posisi untung sebesar risiko awalnya.
    """
    if entry <= 0 or atr <= 0 or trigger_atr_mult <= 0:
        return 0.0
    return entry + trigger_atr_mult * atr


def be_trigger_mode(trigger_mode: str, stops_mode: str) -> str:
    """
    Terjemahkan `breakeven.trigger_mode` menjadi basis yang benar-benar
    dipakai: 'rr' atau 'atr'.

    mode 'auto' mengikuti basis SL (stops.mode): SL berbasis ATR -> BE
    berbasis ATR, selain itu BE berbasis R. Dengan begitu satu tombol
    "auto" menjaga BE dan SL selalu sejalan, sementara operator yang ingin
    mencampur (mis. SL persen tapi BE ATR) bisa memilih eksplisit.
    """
    if trigger_mode == "auto":
        return "atr" if stops_mode == "atr" else "rr"
    return trigger_mode if trigger_mode in ("rr", "atr") else "rr"


def be_trigger_price(entry: float, initial_stop: float, mode: str,
                     trigger_rr: float = 1.0, atr: float = 0.0,
                     trigger_atr_mult: float = 1.5) -> float:
    """Harga pemicu BE untuk basis 'rr' atau 'atr'. 0.0 = tidak bisa dihitung.

    Bila basis 'atr' dipilih tetapi ATR tidak tersedia, hasilnya 0.0 dan
    pemanggil (PositionManager) memakai basis R sebagai fallback supaya BE
    tidak hilang sama sekali.
    """
    if mode == "atr":
        return breakeven_trigger_price_atr(entry, atr, trigger_atr_mult)
    return breakeven_trigger_price(entry, initial_stop, trigger_rr)


def should_trigger_be(price: float, entry: float, initial_stop: float,
                      mode: str, trigger_rr: float = 1.0, atr: float = 0.0,
                      trigger_atr_mult: float = 1.5) -> bool:
    """True bila harga sudah mencapai pemicu BE pada basis yang dipilih."""
    trigger = be_trigger_price(entry, initial_stop, mode, trigger_rr, atr,
                               trigger_atr_mult)
    if trigger <= 0 and mode == "atr":
        # ATR tidak tersedia -> jangan kehilangan proteksi BE: pakai basis R.
        trigger = breakeven_trigger_price(entry, initial_stop, trigger_rr)
    return trigger > 0 and price >= trigger


# ---------------------------------------------------------------------------
# Trailing stop
# ---------------------------------------------------------------------------

def trailing_stop_price(highest: float, percent_pct: float) -> float:
    """
    Kandidat SL trailing berdasarkan harga tertinggi sejak entry:
    highest x (1 - percent_pct%).
    """
    if highest <= 0:
        return 0.0
    return highest * (1.0 - percent_pct / 100.0)


def update_trailing(current_sl: float, highest: float,
                    percent_pct: float) -> float:
    """
    SL trailing baru (basis persen). SL TIDAK PERNAH turun (monoton naik)
    dan tidak boleh di bawah SL saat ini. Kandidat hanya dipakai kalau
    lebih tinggi.
    """
    candidate = trailing_stop_price(highest, percent_pct)
    return max(current_sl, candidate)


def trailing_stop_price_atr(highest: float, atr: float,
                            multiplier: float) -> float:
    """
    Kandidat SL trailing berbasis ATR: highest - (multiplier x ATR).

    ATR yang dipakai adalah nilai yang DIBEKUKAN saat entry (satu posisi
    memakai satu nilai ATR). Konsekuensinya, saat volatilitas pasar naik
    setelah entry, jarak trailing tidak ikut melebar; itu pertukaran yang
    dipilih agar level SL tidak bergerak tanpa aksi harga baru. Mengembalikan
    0.0 bila ATR tidak tersedia.
    """
    if highest <= 0 or atr <= 0 or multiplier <= 0:
        return 0.0
    return highest - multiplier * atr


def update_trailing_atr(current_sl: float, highest: float, atr: float,
                        multiplier: float) -> float:
    """
    SL trailing baru (basis ATR), monoton naik.

    Bila kandidat ATR berada di bawah SL sekarang (mis. harga tertinggi
    belum cukup naik), SL lama dipertahankan. Perhatikan konsekuensi basis
    ATR: begitu ATR > 0, kandidat BISA berada di bawah entry pada awal
    posisi, jadi trailing baru efektif setelah BE (PositionManager dan
    backtest hanya mengaktifkan trailing setelah BE) atau setelah harga
    naik melewati multiplier x ATR dari entry.
    """
    candidate = trailing_stop_price_atr(highest, atr, multiplier)
    if candidate <= 0:
        return current_sl
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
