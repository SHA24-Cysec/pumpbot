#!/usr/bin/env python3
"""PumpBot entry point.

PumpBot hanya memakai satu file konfigurasi: ``config/config.yaml``.
Ubah field ``mode`` di file tersebut menjadi ``paper``, ``testnet``, atau
``live``. Tidak ada profile YAML maupun override mode dari command line.

Pemakaian:
    python run.py
"""

from __future__ import annotations

import argparse
import asyncio
import sys

CONFIG_PATH = "config/config.yaml"


def main(argv=None) -> int:
    # CLI sengaja tidak menyediakan --config / --mode / --simulate; seluruh
    # pilihan strategi dan mode hanya berasal dari config/config.yaml.
    parser = argparse.ArgumentParser(
        prog="pumpbot",
        description="PumpBot — konfigurasi tunggal di config/config.yaml",
    )
    parser.parse_args(argv)
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
