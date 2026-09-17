"""Utilitas umum: logging, notifikasi, helper angka & waktu."""

from __future__ import annotations

import logging
import logging.handlers
import os
import time
from typing import Optional

import requests

logger = logging.getLogger("pumpbot")


def now_ms() -> int:
    """Waktu saat ini dalam epoch milliseconds (konvensi Binance)."""
    return int(time.time() * 1000)


def clamp01(x: float) -> float:
    """Batasi nilai ke rentang [0, 1]."""
    return max(0.0, min(1.0, x))


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_float(v, default: float = 0.0) -> float:
    """Konversi aman ke float (Binance kadang kirim string/None)."""
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def setup_logging(level: str = "INFO", log_file: Optional[str] = None,
                  max_bytes: int = 10 * 1024 * 1024, backups: int = 5) -> None:
    """Setup logger aplikasi: output ke console + file rotasi."""
    root = logging.getLogger("pumpbot")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # jangan teruskan ke root logger — beberapa library (mis. binance_common)
    # memasang handler di root, sehingga pesan kita tampil DOBEL
    root.propagate = False
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not root.handlers:
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        root.addHandler(console)

        if log_file:
            os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
            fh = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            root.addHandler(fh)

    # Reduksi noise dari library pihak ketiga
    for noisy in ("binance_common", "websockets", "aiohttp", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def notify(message: str, level: str = "INFO") -> None:
    """
    Kirim notifikasi penting ke Telegram (jika dikonfigurasi via .env)
    dan selalu catat ke log.
    """
    log_fn = getattr(logger, level.lower() if level.lower() in ("info", "warning", "error") else "info")
    log_fn(f"[NOTIF] {message}")

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return  # Telegram tidak dikonfigurasi -> cukup log saja

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as exc:  # jangan pernah biarkan notifikasi menjatuhkan bot
        logger.warning(f"Gagal kirim notifikasi Telegram: {exc}")
