"""Position sizing berbasis satu parameter risiko: persentase balance.

Prinsipnya:

    risk_amount = balance x risk_pct
    qty         = risk_amount / (entry - stop)          [spot long]

`balance` adalah saldo akun pada harga modal (bukan equity mark-to-market),
sehingga unrealized PnL posisi terbuka tidak mengubah risiko transaksi baru.
Pada spot, qty tetap tidak boleh melebihi quote balance yang benar-benar
tersedia. Pembatas saldo ini adalah batas fisik ketersediaan dana, bukan cap
risiko/eksposur tambahan yang dapat dikonfigurasi.
"""

from __future__ import annotations

from dataclasses import dataclass

from bot.models import SymbolFilters


@dataclass
class SizingResult:
    qty: float                    # qty final (0 = tidak layak)
    notional: float               # qty x entry
    actual_risk_quote: float      # rugi quote jika SL kena (qty x jarak SL)
    actual_risk_pct: float        # % dari balance acuan
    reason: str = ""              # alasan kalau qty = 0


def compute_raw_qty(balance: float, risk_pct: float, entry: float, stop: float) -> float:
    """Hitung qty mentah agar kerugian di SL sama dengan ``risk_pct`` balance."""
    if balance <= 0 or risk_pct <= 0:
        return 0.0
    if entry <= 0 or stop <= 0 or stop >= entry:
        return 0.0
    risk_amount = balance * (risk_pct / 100.0)
    return risk_amount / (entry - stop)


def apply_lot_size(qty: float, filters: SymbolFilters) -> float:
    """Bulatkan qty ke bawah menurut LOT_SIZE dan jepit ke batas exchange."""
    if qty <= 0:
        return 0.0
    stepped = filters.round_qty(qty)
    if stepped < filters.min_qty:
        return 0.0
    return min(stepped, filters.max_qty)


def clamp_to_available_balance(qty: float, entry: float, available_quote: float) -> float:
    """Jepit qty agar order spot tidak melebihi saldo quote.

    Buffer 0,5% disisakan untuk fee/perbedaan harga ketika market buy. Ini
    bukan parameter risiko tambahan dan tidak dapat mengerek risiko di atas
    persentase yang dipilih pengguna.
    """
    if qty <= 0 or entry <= 0 or available_quote <= 0:
        return 0.0
    return min(qty, (available_quote * 0.995) / entry)


def size_position(
    balance: float,
    available_quote: float,
    entry: float,
    stop: float,
    risk_pct: float,
    filters: SymbolFilters,
) -> SizingResult:
    """Pipeline sizing sederhana: risk % -> saldo tersedia -> aturan exchange."""
    raw = compute_raw_qty(balance, risk_pct, entry, stop)
    if raw <= 0:
        return SizingResult(0, 0, 0, 0, "SL tidak valid (harus di bawah entry)")

    qty = clamp_to_available_balance(raw, entry, available_quote)
    if qty <= 0:
        return SizingResult(0, 0, 0, 0, "saldo quote tersedia tidak cukup")

    qty = apply_lot_size(qty, filters)
    if qty <= 0:
        return SizingResult(0, 0, 0, 0, f"qty < min_qty ({filters.min_qty})")

    notional = qty * entry
    if notional < filters.min_notional:
        return SizingResult(0, 0, 0, 0,
                            f"notional {notional:.2f} < MIN_NOTIONAL {filters.min_notional}")

    actual_risk = qty * (entry - stop)
    return SizingResult(
        qty=qty,
        notional=notional,
        actual_risk_quote=actual_risk,
        actual_risk_pct=(actual_risk / balance * 100.0) if balance > 0 else 0.0,
    )
