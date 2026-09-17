"""
Bootstrap bersama untuk seluruh script tools/backtest.

Menaruh root project ke sys.path supaya script bisa mengimpor modul bot
(detektor, sizing, stops) — logika backtest MEMAKAI kode yang sama dengan
yang berjalan live, bukan reimplementasi.
"""

import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# folder data & hasil default
DATA_DIR = os.path.join(_ROOT, "tools", "backtest", "data")
OUT_DIR = os.path.join(_ROOT, "tools", "backtest", "out")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
