"""
SignalEngine - orkestrator detector.

Alur:
  1. Tiap `reevaluate_sec`, hitung skor SEMUA simbol watchlist.
  2. Skor positif (orderbook, trade_flow, volume, whale, price_action)
     digabung rata-rata tertimbang sesuai bobot config.
  3. Skor manipulasi mengurangi skor akhir (bobot config) dan bisa VETO.
  4. Kalau skor akhir >= threshold & lolos cooldown -> keluarkan Signal
     untuk diproses Risk Management.

Hasil skor terbaru tiap simbol juga disimpan untuk panel dashboard.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from bot.config import Config
from bot.data_collector.collector import DataCollector
from bot.models import Signal
from bot.signal_engine.change24h import FILTER_DETECTORS as CHANGE24H_FILTERS
from bot.signal_engine.detectors import ALL_DETECTORS
from bot.signal_engine.vwap import FILTER_DETECTORS as VWAP_FILTERS
from bot.utils import clamp, now_ms

logger = logging.getLogger("pumpbot.signal")

# callback async yang dipanggil saat sinyal layak eksekusi muncul
OnSignal = Callable[[Signal], Awaitable[None]]


class SignalEngine:
    def __init__(self, cfg: Config, collector: DataCollector):
        self.cfg = cfg
        self.collector = collector
        # skor terbaru per simbol (untuk dashboard & debugging)
        self.latest: dict[str, dict] = {}
        self.paused = False                     # diatur dari dashboard
        self._detectors = {name: cls() for name, cls in ALL_DETECTORS.items()}
        # Filter gate (tidak menyumbang skor): Anchored VWAP dan band
        # perubahan 24 jam (nilai absolut). Keduanya hanya menentukan
        # `eligible`.
        self._filters = {name: cls() for name, cls in
                         {**VWAP_FILTERS, **CHANGE24H_FILTERS}.items()}
        # Override runtime dari dashboard: None = ikut config.
        self.vwap_enabled_override: Optional[bool] = None
        self.change24h_enabled_override: Optional[bool] = None
        # provider waktu aktivitas terakhir per simbol (untuk cooldown),
        # di-inject dari main (membaca database)
        self.cooldown_provider: Callable[[str], float] = lambda sym: 1e9
        self._task: Optional[asyncio.Task] = None
        self.on_signal: Optional[OnSignal] = None
        self.evaluated = 0

    # ------------------------------------------------------------------
    def _vwap_enabled(self) -> bool:
        """Filter VWAP aktif? Override runtime menang atas config."""
        if self.vwap_enabled_override is not None:
            return bool(self.vwap_enabled_override)
        return bool(self.cfg.signal.vwap.enabled)

    def _change24h_enabled(self) -> bool:
        """Gate band perubahan 24 jam (nilai absolut) aktif? Override menang."""
        if self.change24h_enabled_override is not None:
            return bool(self.change24h_enabled_override)
        return bool(self.cfg.signal.change_24h.enabled)

    # ------------------------------------------------------------------
    def evaluate(self, symbol: str) -> Optional[dict]:
        """Hitung skor gabungan satu simbol. Return snapshot skor."""
        buf = self.collector.buffer(symbol)
        if buf is None:
            return None
        if not buf.ready(self.cfg.signal.min_candles):
            self.latest[symbol] = {
                "symbol": symbol, "ts": now_ms(), "price": buf.last_price,
                "score": 0.0, "eligible": False, "veto": False,
                "reason": "menunggu data", "breakdown": {},
            }
            return None

        cfg = self.cfg
        results = {name: det.score(buf, cfg) for name, det in self._detectors.items()}

        # --- gabungkan detector positif (rata-rata tertimbang) ---
        weights = cfg.signal.weights
        pos_names = ["orderbook", "trade_flow", "volume", "whale", "price_action"]
        wsum = sum(weights.get(n, 0.0) for n in pos_names)
        if wsum <= 0:
            wsum = 1.0
        base = sum(results[n].score * weights.get(n, 0.0) for n in pos_names) / wsum

        # --- penalti & veto manipulasi ---
        manip = results["manipulation"]
        final = clamp(base * (1.0 - cfg.signal.manipulation.weight * manip.score / 100.0),
                      0.0, 100.0)

        eligible = all(results[n].eligible for n in pos_names)

        # --- filter Anchored VWAP (gate, bukan skor) ---
        vwap_ok = True
        vwap_info = None
        if self._vwap_enabled():
            vres = self._filters["vwap"].score(buf, cfg)
            vwap_info = dict(vres.details)
            vwap_ok = bool(vres.eligible)
            eligible = eligible and vwap_ok

        # --- gate band perubahan 24 jam, nilai absolut (bukan skor) ---
        band_ok = True
        band_info = None
        if self._change24h_enabled():
            cres = self._filters["change24h"].score(buf, cfg)
            band_info = dict(cres.details)
            band_ok = bool(cres.eligible)
            eligible = eligible and band_ok

        veto = manip.veto
        snapshot = {
            "symbol": symbol,
            "ts": now_ms(),
            "price": buf.last_price,
            "score": round(final, 1),
            "base_score": round(base, 1),
            "eligible": eligible,
            "veto": veto,
            "breakdown": {n: round(results[n].score, 1) for n in results},
            "manip_details": manip.details,
        }
        if vwap_info is not None:
            snapshot["vwap"] = vwap_info
        if band_info is not None:
            snapshot["change_24h"] = band_info
        self.latest[symbol] = snapshot
        self.evaluated += 1

        if veto:
            snapshot["reason"] = f"VETO manipulasi (skor {manip.score:.0f})"
        elif not eligible:
            bad = [n for n in pos_names if not results[n].eligible]
            if not vwap_ok:
                bad.append("vwap")
            if not band_ok:
                bad.append("change24h")
            reason = f"gate: {','.join(bad)}"
            if not vwap_ok and vwap_info:
                reason += f" ({vwap_info.get('reason', '')})"
            if not band_ok and band_info:
                reason += f" ({band_info.get('reason', '')})"
            snapshot["reason"] = reason
        return snapshot

    # ------------------------------------------------------------------
    def _should_emit(self, symbol: str, snapshot: dict) -> tuple[bool, str]:
        """Cek threshold + cooldown + veto sebelum mengeluarkan sinyal."""
        if self.paused:
            return False, "bot paused"
        if snapshot["veto"] or not snapshot["eligible"]:
            return False, snapshot.get("reason", "not eligible")
        threshold = self._runtime_threshold()
        if snapshot["score"] < threshold:
            return False, f"skor {snapshot['score']:.0f} < threshold {threshold:.0f}"
        # cooldown: simbol yang baru saja keluar dari trade harus dingin dulu
        since = self.cooldown_provider(symbol)
        cooldown_s = self.cfg.signal.cooldown_after_exit_min * 60
        if since < cooldown_s:
            return False, f"cooldown {int(cooldown_s - since)}s lagi"
        return True, ""

    def _runtime_threshold(self) -> float:
        """Threshold bisa diubah live dari dashboard."""
        return getattr(self, "_override_threshold", None) or self.cfg.signal.score_threshold

    # ------------------------------------------------------------------
    async def run(self, on_signal: OnSignal) -> None:
        """Loop evaluasi periodik seluruh watchlist."""
        self.on_signal = on_signal
        self._task = asyncio.create_task(self._loop(), name="signal-engine")

    async def _loop(self) -> None:
        interval = max(1.0, self.cfg.signal.reevaluate_sec)
        logger.info(f"SignalEngine loop tiap {interval}s, threshold={self._runtime_threshold()}")
        try:
            while True:
                for symbol in list(self.collector.watchlist):
                    snapshot = self.evaluate(symbol)
                    if snapshot is None:
                        continue
                    ok, reason = self._should_emit(symbol, snapshot)
                    if not ok:
                        continue
                    # >>> sinyal layak eksekusi <<<
                    sig = Signal(
                        ts=now_ms(),
                        symbol=symbol,
                        price=snapshot["price"],
                        score=snapshot["score"],
                        breakdown={
                            "scores": snapshot["breakdown"],
                            "manip": snapshot["manip_details"],
                            "vwap": snapshot.get("vwap"),
                            "change24h": snapshot.get("change_24h"),
                        },
                        suggested_stop=self._structure_stop(symbol),
                        entry_type="breakout",
                        # ATR candle tertutup terakhir. Executor yang membekukan
                        # nilai ini ke posisi (pos.atr_entry) dan seluruh basis
                        # ATR posisi memakai angka yang sama seumur posisi.
                        atr=self.collector.atr(symbol),
                        reason=(
                            f"skor {snapshot['score']:.0f} >= {self._runtime_threshold():.0f}; "
                            f"detail={snapshot['breakdown']}"
                        ),
                    )
                    vw = snapshot.get("vwap")
                    vwap_txt = (f" vwap_dist={vw.get('dist_pct')}%" if vw else "")
                    c24 = snapshot.get("change_24h")
                    c24_txt = (f" chg24={c24.get('change_pct')}%"
                               if c24 else "")
                    logger.info(
                        f"SINYAL {symbol} @ {sig.price:.6f} skor={sig.score:.0f} "
                        f"breakdown={snapshot['breakdown']}{vwap_txt}{c24_txt}"
                    )
                    try:
                        await self.on_signal(sig)
                    except Exception as exc:
                        logger.exception(f"Handler sinyal error: {exc}")
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    def _structure_stop(self, symbol: str) -> float:
        """Ambil usulan stop dari detector price action (swing low terakhir)."""
        buf = self.collector.buffer(symbol)
        if not buf or not buf.candles:
            return 0.0
        # jalankan ulang detector PA hanya untuk ambil swing stop
        det = self._detectors["price_action"]
        res = det.score(buf, self.cfg)
        return res.details.get("swing_stop", 0.0)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    def top_signals(self, limit: int = 50) -> list[dict]:
        """Daftar skor tertinggi untuk dashboard."""
        items = sorted(self.latest.values(), key=lambda s: -s.get("score", 0))
        return items[:limit]
