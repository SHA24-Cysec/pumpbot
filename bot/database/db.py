"""
Database SQLite (mode WAL) untuk pencatatan lengkap aktivitas bot.

Tabel:
  trades            : siklus hidup tiap posisi (entry, SL/TP, BE, trailing, exit)
  trade_events      : peristiwa per posisi (partial TP, BE aktif, SL digeser, dst.)
  signals           : audit trail skor sinyal
  equity_snapshots  : kurva equity (dasar grafik dashboard & max drawdown)
  events            : log aplikasi penting (order, error, dsb.)
  kv                : key-value sederhana (equity awal, dsb.)

Catatan upgrade ke PostgreSQL:
  Semua query di file ini sengaja memakai SQL standar + parameter `?`.
  Untuk migrasi: ganti sqlite3.connect() dengan psycopg3, ubah placeholder
  `?` -> `%s`, dan sesuaikan DDL (AUTOINCREMENT -> GENERATED ... AS IDENTITY).
  Lihat README bagian "Upgrade ke PostgreSQL".
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from typing import Optional

from bot.utils import now_ms

logger = logging.getLogger("pumpbot.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT    NOT NULL,
    status           TEXT    NOT NULL DEFAULT 'OPEN',   -- OPEN | CLOSED
    entry_time       INTEGER NOT NULL,
    entry_price      REAL    NOT NULL,
    qty_total        REAL    NOT NULL,
    qty_remaining    REAL    NOT NULL,
    quote_value      REAL    NOT NULL,
    stop_loss        REAL,
    initial_stop     REAL,
    take_profits     TEXT,                               -- JSON list harga TP
    be_triggered     INTEGER NOT NULL DEFAULT 0,
    trail_active     INTEGER NOT NULL DEFAULT 0,
    highest_price    REAL    NOT NULL DEFAULT 0,
    score            REAL,
    entry_reason     TEXT,
    exit_mode        TEXT,
    realized_pnl     REAL    NOT NULL DEFAULT 0,
    fees_paid        REAL    NOT NULL DEFAULT 0,
    exit_time        INTEGER,
    exit_price       REAL,
    exit_reason      TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades(symbol, entry_time);

CREATE TABLE IF NOT EXISTS trade_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id   INTEGER NOT NULL,
    ts         INTEGER NOT NULL,
    event      TEXT    NOT NULL,      -- ENTRY / TP_FILL / SL_FILL / BREAKEVEN / TRAIL_UPDATE / MANUAL_CLOSE ...
    price      REAL,
    qty        REAL,
    pnl        REAL,
    detail     TEXT
);
CREATE INDEX IF NOT EXISTS idx_tevents_trade ON trade_events(trade_id);

CREATE TABLE IF NOT EXISTS signals (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      INTEGER NOT NULL,
    symbol  TEXT    NOT NULL,
    price   REAL,
    score   REAL,
    breakdown TEXT
);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts);

CREATE TABLE IF NOT EXISTS equity_snapshots (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       INTEGER NOT NULL,
    equity   REAL NOT NULL,
    available REAL NOT NULL,
    in_position REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_equity_ts ON equity_snapshots(ts);

CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      INTEGER NOT NULL,
    level   TEXT NOT NULL,
    type    TEXT NOT NULL,
    symbol  TEXT,
    message TEXT
);

CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


class Database:
    """Wrapper SQLite thread-safe (satu koneksi + lock; operasi mikro-detik)."""

    def __init__(self, path: str = "data/pumpbot.db"):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")   # aman utk baca-tulis bersamaan
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
        logger.info(f"Database siap: {path}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _exec(self, sql: str, params: tuple = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur.lastrowid

    # ------------------------------------------------------------------
    # TRADES
    # ------------------------------------------------------------------
    def open_trade(self, symbol: str, entry_time: int, entry_price: float,
                   qty: float, quote_value: float, stop_loss: float,
                   take_profits: list[float], score: float, entry_reason: str,
                   exit_mode: str) -> int:
        tp_json = json.dumps([round(p, 10) for p in take_profits])
        trade_id = self._exec(
            """INSERT INTO trades (symbol, status, entry_time, entry_price, qty_total,
               qty_remaining, quote_value, stop_loss, initial_stop, take_profits,
               highest_price, score, entry_reason, exit_mode)
               VALUES (?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, entry_time, entry_price, qty, qty, quote_value,
             stop_loss, stop_loss, tp_json, entry_price, score, entry_reason, exit_mode))
        self.add_trade_event(trade_id, "ENTRY", entry_price, qty, 0.0,
                             f"score={score:.0f}; {entry_reason}")
        return trade_id

    def update_trade(self, trade_id: int, **fields) -> None:
        """Update kolom posisi terbuka (qty_remaining, stop_loss, be_triggered, dst.)."""
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [trade_id]
        self._exec(f"UPDATE trades SET {cols} WHERE id = ?", tuple(vals))

    def close_trade(self, trade_id: int, exit_time: int, exit_price: float,
                    realized_pnl: float, exit_reason: str,
                    qty_remaining: float = 0.0) -> None:
        self._exec(
            """UPDATE trades SET status='CLOSED', exit_time=?, exit_price=?,
               realized_pnl=?, exit_reason=?, qty_remaining=? WHERE id=?""",
            (exit_time, exit_price, round(realized_pnl, 6), exit_reason,
             qty_remaining, trade_id))

    def add_trade_event(self, trade_id: int, event: str, price: float,
                        qty: float, pnl: float, detail: str = "") -> None:
        self._exec(
            "INSERT INTO trade_events (trade_id, ts, event, price, qty, pnl, detail)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (trade_id, now_ms(), event, price, qty, pnl, detail))

    def get_open_trades(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time").fetchall()
        return [dict(r) for r in rows]

    def get_trade_history(self, limit: int = 200) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE status='CLOSED' "
                "ORDER BY exit_time DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_closed_trades_today(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM trades WHERE status='CLOSED' "
                "ORDER BY exit_time DESC").fetchall()
        # saring hari UTC hari ini
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return [dict(r) for r in rows
                if r["exit_time"] and datetime.fromtimestamp(
                    r["exit_time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d") == today]

    def last_activity_time(self, symbol: str) -> Optional[int]:
        """Timestamp aktivitas trade terakhir (entry ATAU exit) sebuah simbol."""
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(ts) AS m FROM ("
                "  SELECT MAX(entry_time) AS ts FROM trades WHERE symbol=?"
                "  UNION ALL SELECT MAX(exit_time) FROM trades WHERE symbol=?)",
                (symbol, symbol)).fetchone()
        return row["m"] if row and row["m"] else None

    def seconds_since_last_activity(self, symbol: str) -> float:
        """Untuk cooldown sinyal (detik sejak trade terakhir simbol ini)."""
        last = self.last_activity_time(symbol)
        if not last:
            return 1e9
        return max(0.0, (now_ms() - last) / 1000.0)

    # ------------------------------------------------------------------
    # SINYAL & EQUITY & EVENTS
    # ------------------------------------------------------------------
    def record_signal(self, symbol: str, price: float, score: float,
                      breakdown: dict) -> None:
        self._exec(
            "INSERT INTO signals (ts, symbol, price, score, breakdown) VALUES (?,?,?,?,?)",
            (now_ms(), symbol, price, round(score, 2), json.dumps(breakdown, default=str)))

    def record_event(self, level: str, type_: str, message: str,
                     symbol: Optional[str] = None) -> None:
        self._exec(
            "INSERT INTO events (ts, level, type, symbol, message) VALUES (?,?,?,?,?)",
            (now_ms(), level, type_, symbol, message))

    def get_recent_events(self, limit: int = 50) -> list[dict]:
        """Ambil event aplikasi terbaru untuk dashboard (warning/error/order)."""
        limit = max(1, min(int(limit), 500))
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, ts, level, type, symbol, message FROM events "
                "ORDER BY ts DESC, id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def snapshot_equity(self, equity: float, available: float, in_position: float) -> None:
        self._exec(
            "INSERT INTO equity_snapshots (ts, equity, available, in_position)"
            " VALUES (?,?,?,?)", (now_ms(), equity, available, in_position))

    def get_equity_curve(self, limit: int = 1000) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, equity FROM equity_snapshots ORDER BY ts DESC LIMIT ?",
                (limit,)).fetchall()
        out = [dict(r) for r in reversed(rows)]
        return out

    # ------------------------------------------------------------------
    # KEY-VALUE
    # ------------------------------------------------------------------
    def kv_get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self._lock:
            row = self._conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def kv_set(self, key: str, value: str) -> None:
        self._exec("INSERT INTO kv (key, value) VALUES (?, ?) "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
