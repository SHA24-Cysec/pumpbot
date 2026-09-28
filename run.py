#!/usr/bin/env python3
"""PumpBot entry point.

PumpBot hanya memakai satu file konfigurasi: ``config/config.yaml``.
Ubah field ``mode`` di file tersebut menjadi:

    paper -> AKUN DEMO. Harga, volume, dan order book diambil dari pasar
             Binance SUNGGUHAN lewat endpoint publik (tanpa API key),
             sedangkan saldo dan order sepenuhnya virtual.
    live  -> UANG SUNGGUHAN. Butuh API key di .env.

Tidak ada profile YAML maupun override mode dari command line.

Bot TIDAK auto-start trading: setelah dijalankan, bot berada dalam kondisi
PAUSE sampai tombol "Resume Bot" ditekan di dashboard. Posisi lama yang
dipulihkan dari database tetap dikelola (SL/TP/trailing) selama pause.

Pemakaian:
    python run.py
"""

from __future__ import annotations

import argparse
import asyncio
import sys

CONFIG_PATH = "config/config.yaml"


def _siapkan_stderr_windows() -> None:
    """Cegah UnicodeEncodeError di Windows saat output diarahkan ke file.

    Di Windows, stderr yang dialihkan ke file/pipe (Task Scheduler, layanan,
    "python run.py > log.txt") memakai encoding lawas seperti cp1252, sehingga
    pesan log ber-emoji (contoh "❌") bisa gagal di-encode. Mengubah kebijakan
    error stream menjadi "replace" membuat karakter tersebut sekadar diganti
    "?" alih-alih melempar exception. Di Linux/macOS fungsi ini no-op.
    """
    if sys.platform != "win32":
        return
    try:
        if sys.stderr is not None and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(errors="replace")
    except Exception:
        pass  # stream tidak bisa direconfigure -> biarkan apa adanya


def main(argv=None) -> int:
    # CLI sengaja tidak menyediakan --config / --mode / --simulate; seluruh
    # pilihan strategi dan mode hanya berasal dari config/config.yaml.
    parser = argparse.ArgumentParser(
        prog="pumpbot",
        description="PumpBot - konfigurasi tunggal di config/config.yaml",
    )
    parser.parse_args(argv)
    _siapkan_stderr_windows()
    from bot.config import ConfigError, load_config
    from bot.utils import setup_logging

    try:
        cfg = load_config(CONFIG_PATH)
    except ConfigError as exc:
        print(f"\n❌ {exc}\n", file=sys.stderr)
        return 2

    setup_logging(level=cfg.logging.level, log_file=cfg.logging.file,
                  max_bytes=cfg.logging.max_bytes, backups=cfg.logging.backups)

    from bot.main import BotApp
    import logging
    log = logging.getLogger("pumpbot.run")
    log.info("Konfigurasi tunggal: %s | mode=%s", CONFIG_PATH, cfg.mode.upper())

    app = BotApp(cfg)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print("\nDihentikan pengguna.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
