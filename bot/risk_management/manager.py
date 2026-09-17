"""Penjaga aturan entry dan sizing PumpBot.

Aturan yang diterapkan:
* Risk per transaksi hanya satu input: persentase dari balance (bukan PnL
  berjalan/equity).
* Secara default hanya satu posisi. Mode multi-posisi harus dinyalakan secara
  eksplisit dan dibatasi maksimal tiga posisi pada pair yang berbeda.
* Batas rugi harian adalah fitur opsional. Jika aktif, equity dibandingkan
  dengan equity awal hari dan hari berganti pada 00:00 UTC.

Semua keputusan entry dicatat oleh Executor agar dapat diaudit.
"""

from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

from bot.config import Config
from bot.models import Position, SymbolFilters
from bot.risk_management import sizing

logger = logging.getLogger("pumpbot.risk")


@dataclass
class RuntimeParams:
    """Parameter yang dapat diubah dari dashboard tanpa restart."""

    risk_per_trade_pct: float
    allow_multiple_positions: bool
    max_open_positions: int
    daily_loss_enabled: bool
    daily_loss_limit_pct: float
    score_threshold: float
    trailing_enabled: bool
    breakeven_enabled: bool

    @property
    def effective_open_position_limit(self) -> int:
        """Default aman: satu posisi, kecuali multi-posisi benar-benar aktif."""
        return self.max_open_positions if self.allow_multiple_positions else 1

    def to_dict(self) -> dict:
        data = dict(self.__dict__)
        data["effective_open_position_limit"] = self.effective_open_position_limit
        return data


class RiskManager:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.params = RuntimeParams(
            risk_per_trade_pct=cfg.risk.risk_per_trade_pct,
            allow_multiple_positions=cfg.risk.allow_multiple_positions,
            max_open_positions=cfg.risk.max_open_positions,
            daily_loss_enabled=cfg.risk.daily_loss_enabled,
            daily_loss_limit_pct=cfg.risk.daily_loss_limit_pct,
            score_threshold=cfg.signal.score_threshold,
            trailing_enabled=cfg.trailing.enabled,
            breakeven_enabled=cfg.breakeven.enabled,
        )
        self._lock = threading.Lock()

        # Status harian selalu UTC sesuai keputusan pengguna.
        self.current_day: str = ""
        self.equity_start_of_day: float = 0.0
        self.daily_halt_reason: str = ""

    # ------------------------------------------------------------------
    # Parameter live (dashboard)
    # ------------------------------------------------------------------
    def update_params(self, updates: dict) -> list[str]:
        """Ubah parameter runtime; return error validasi (kosong = sukses)."""
        errors: list[str] = []
        bool_keys = {
            "allow_multiple_positions", "daily_loss_enabled",
            "trailing_enabled", "breakeven_enabled",
        }
        int_keys = {"max_open_positions"}

        with self._lock:
            p = RuntimeParams(**self.params.__dict__)
            for key, value in updates.items():
                if not hasattr(p, key):
                    errors.append(f"parameter tidak dikenal: {key}")
                    continue
                if key in bool_keys:
                    if not isinstance(value, bool):
                        errors.append(f"{key} harus boolean")
                    else:
                        setattr(p, key, value)
                    continue
                if key in int_keys:
                    try:
                        parsed = int(value)
                        if float(value) != parsed:
                            raise ValueError
                        setattr(p, key, parsed)
                    except (TypeError, ValueError):
                        errors.append(f"{key} harus bilangan bulat")
                    continue
                try:
                    setattr(p, key, float(value))
                except (TypeError, ValueError):
                    errors.append(f"{key} harus angka")

            if (not math.isfinite(p.risk_per_trade_pct)
                    or p.risk_per_trade_pct <= 0):
                errors.append("risk_per_trade_pct harus angka positif (> 0); tidak ada batas atas software")
            if not 1 <= p.max_open_positions <= 3:
                errors.append("max_open_positions harus di rentang [1, 3]")
            if not 0 < p.daily_loss_limit_pct <= 50:
                errors.append("daily_loss_limit_pct harus di rentang (0, 50]")
            if not 0 <= p.score_threshold <= 100:
                errors.append("score_threshold harus di rentang [0, 100]")

            if not errors:
                self.params = p
                if not p.daily_loss_enabled:
                    self.daily_halt_reason = ""
                logger.info("Parameter runtime diubah: %s", updates)
        return errors

    # ------------------------------------------------------------------
    # Roll hari baru / batas rugi harian
    # ------------------------------------------------------------------
    def roll_day_if_needed(self, equity: float) -> bool:
        """Mulai hari UTC baru dan catat equity baseline. Return True jika baru."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_day:
            self.current_day = today
            self.equity_start_of_day = equity
            if self.daily_halt_reason:
                logger.info("Hari UTC baru %s: halt harian direset (equity awal %.2f)",
                            today, equity)
            self.daily_halt_reason = ""
            return True
        return False

    def check_daily_limit(self, equity: float) -> tuple[bool, str]:
        """Cek opsi daily loss berdasarkan equity (termasuk unrealized PnL)."""
        self.roll_day_if_needed(equity)
        p = self.params
        if not p.daily_loss_enabled:
            # Membuat toggle off langsung membuka kembali pintu entry.
            self.daily_halt_reason = ""
            return False, ""
        if self.equity_start_of_day <= 0:
            return False, ""

        dd_pct = (self.equity_start_of_day - equity) / self.equity_start_of_day * 100.0
        if dd_pct >= p.daily_loss_limit_pct:
            reason = (f"BATAS RUGI HARIAN tercapai: equity turun {dd_pct:.2f}% "
                      f"(limit {p.daily_loss_limit_pct}%, reset 00:00 UTC) "
                      "- entry baru dihentikan")
            if not self.daily_halt_reason:
                logger.error(reason)
                from bot.utils import notify
                notify(f"⚠️ {reason}", level="ERROR")
            self.daily_halt_reason = reason
            return True, reason
        return False, ""

    # ------------------------------------------------------------------
    # Pintu masuk posisi baru
    # ------------------------------------------------------------------
    def can_open_new(self, open_positions: list[Position], equity: float,
                     paused: bool) -> tuple[bool, str]:
        """Cek pause, daily-loss opsional, serta mode satu/multi posisi."""
        p = self.params
        if paused:
            return False, "bot di-pause (dashboard)"
        halted, reason = self.check_daily_limit(equity)
        if halted:
            return False, reason
        limit = p.effective_open_position_limit
        if len(open_positions) >= limit:
            if not p.allow_multiple_positions:
                return False, "mode satu posisi aktif; tunggu posisi terbuka selesai"
            return False, f"jumlah posisi terbuka maksimum ({limit}) sudah tercapai"
        return True, ""

    def size_position(self, balance: float, available_quote: float,
                      entry: float, stop: float,
                      filters: SymbolFilters) -> sizing.SizingResult:
        """Hitung qty dengan satu parameter risiko: % dari balance."""
        return sizing.size_position(
            balance=balance,
            available_quote=available_quote,
            entry=entry,
            stop=stop,
            risk_pct=self.params.risk_per_trade_pct,
            filters=filters,
        )
