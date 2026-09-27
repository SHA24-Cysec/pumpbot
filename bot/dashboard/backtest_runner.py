"""
Pengelola job backtest untuk dashboard.

Grid search itu berat: bisa menghabiskan seluruh inti CPU selama beberapa
menit. Kalau dijalankan di dalam event loop bot, WebSocket dashboard akan
membeku dan pengawasan posisi ikut telat. Karena itu job dijalankan sebagai
PROSES TERPISAH lewat tools.backtest.service, dan modul ini hanya bertugas:

  * menyalakan proses anak dan mengirim parameter job lewat stdin,
  * membaca aliran NDJSON dari stdout secara asinkron,
  * merangkum kemajuan jadi satu objek state yang bisa dibaca dashboard,
  * menghentikan job beserta seluruh anak prosesnya bila dibatalkan.

Hanya satu job boleh berjalan pada satu waktu.
"""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import signal
import sys
import time
import uuid
from typing import Optional

logger = logging.getLogger("pumpbot.backtest")

# Jumlah baris log terakhir yang disimpan di memori.
MAX_LOG_LINES = 600

# Waktu tunggu setelah SIGTERM sebelum proses dipaksa mati.
KILL_GRACE_S = 5.0

STATUS_IDLE = "idle"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_ERROR = "error"
STATUS_CANCELLED = "cancelled"


class BacktestBusy(RuntimeError):
    """Sudah ada job yang berjalan."""


class BacktestManager:
    """Menyalakan, memantau, dan menghentikan satu job optimasi."""

    def __init__(self, cwd: Optional[str] = None,
                 python_exe: Optional[str] = None):
        self.cwd = cwd or os.getcwd()
        self.python_exe = python_exe or sys.executable

        self._proc: Optional[asyncio.subprocess.Process] = None
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        self.status: str = STATUS_IDLE
        self.job_id: str = ""
        self.request: dict = {}
        self.plan: dict = {}
        self.phase: dict = {}
        self.progress: dict = {}
        self.rows: list[dict] = []
        self.meta: dict = {}
        self.csv_path: str = ""
        self.error: str = ""
        self.started_at: int = 0
        self.finished_at: int = 0

        self._logs: list[dict] = []
        self._log_seq: int = 0

    # ------------------------------------------------------------------
    # Log
    # ------------------------------------------------------------------
    def _log(self, text: str, level: str = "info") -> None:
        """Simpan satu baris log dengan nomor urut agar UI bisa ambil delta."""
        self._log_seq += 1
        self._logs.append({"i": self._log_seq, "t": int(time.time() * 1000),
                           "level": level, "text": text})
        if len(self._logs) > MAX_LOG_LINES:
            del self._logs[: len(self._logs) - MAX_LOG_LINES]

    # ------------------------------------------------------------------
    # State untuk dashboard
    # ------------------------------------------------------------------
    def summary(self) -> dict:
        """Ringkasan sangat ringan, ikut disisipkan ke snapshot WebSocket."""
        return {
            "status": self.status,
            "job_id": self.job_id,
            "phase": self.phase.get("label", ""),
            "pct": self.progress.get("pct", 0.0),
            "running": self.status == STATUS_RUNNING,
        }

    def state(self, since: int = 0) -> dict:
        """State lengkap. `since` membatasi log ke baris yang belum terkirim."""
        now = int(time.time() * 1000)
        akhir = self.finished_at or (now if self.started_at else 0)
        return {
            "status": self.status,
            "job_id": self.job_id,
            "request": self.request,
            "plan": self.plan,
            "phase": self.phase,
            "progress": self.progress,
            "rows": self.rows,
            "meta": self.meta,
            "csv": self.csv_path,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed_sec": (round((akhir - self.started_at) / 1000, 1)
                            if self.started_at else 0.0),
            "logs": [r for r in self._logs if r["i"] > since],
            "log_seq": self._log_seq,
        }

    # ------------------------------------------------------------------
    # Kendali
    # ------------------------------------------------------------------
    async def start(self, request: dict) -> str:
        """Nyalakan job baru. Melempar BacktestBusy bila masih ada yang jalan."""
        async with self._lock:
            if self.status == STATUS_RUNNING:
                raise BacktestBusy("masih ada backtest yang berjalan")

            self.job_id = uuid.uuid4().hex[:12]
            self.request = dict(request)
            self.plan = {}
            self.phase = {"key": "mulai", "label": "Menyalakan proses",
                          "total": 1}
            self.progress = {"current": 0, "total": 1, "pct": 0.0,
                             "detail": ""}
            self.rows = []
            self.meta = {}
            self.csv_path = ""
            self.error = ""
            self.started_at = int(time.time() * 1000)
            self.finished_at = 0
            self._logs = []
            self._log_seq = 0
            self.status = STATUS_RUNNING

            cmd = [self.python_exe, "-u", "-m", "tools.backtest.service",
                   "--job", "-"]
            env = dict(os.environ)
            env.setdefault("PYTHONUNBUFFERED", "1")
            try:
                self._proc = await asyncio.create_subprocess_exec(
                    *cmd, cwd=self.cwd, env=env,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    # sesi baru supaya seluruh pohon proses (termasuk pekerja
                    # ProcessPoolExecutor) bisa dimatikan sekaligus
                    start_new_session=True,
                )
            except OSError as exc:
                self.status = STATUS_ERROR
                self.error = f"gagal menjalankan proses backtest: {exc}"
                self.finished_at = int(time.time() * 1000)
                self._log(self.error, "error")
                raise

            # Jaring pengaman terakhir: kalau interpreter mati mendadak
            # (exception fatal, KeyboardInterrupt ganda) dan jalur shutdown
            # rapi tidak sempat jalan, proses anak tetap dibereskan.
            atexit.register(_bunuh_paksa, self._proc)

            self._log(f"Job {self.job_id} dimulai (pid {self._proc.pid})")
            self._task = asyncio.create_task(self._pantau(request))
            return self.job_id

    async def cancel(self) -> bool:
        """Hentikan job berjalan beserta seluruh anak prosesnya."""
        proc = self._proc
        if self.status != STATUS_RUNNING or proc is None:
            return False
        self._log("Pembatalan diminta pengguna.", "warning")
        _bunuh_grup(proc, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=KILL_GRACE_S)
        except asyncio.TimeoutError:
            self._log("Proses tidak berhenti, dipaksa mati.", "warning")
            _bunuh_grup(proc, signal.SIGKILL)
        return True

    async def shutdown(self) -> None:
        """Dipanggil saat dashboard berhenti; jangan tinggalkan proses yatim."""
        if self.status == STATUS_RUNNING:
            await self.cancel()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    # ------------------------------------------------------------------
    # Pemantau proses
    # ------------------------------------------------------------------
    async def _pantau(self, request: dict) -> None:
        """Kirim job ke stdin, lalu serap stdout NDJSON dan stderr sampai habis."""
        proc = self._proc
        assert proc is not None
        try:
            if proc.stdin:
                proc.stdin.write(json.dumps(request).encode("utf-8"))
                await proc.stdin.drain()
                proc.stdin.close()
        except (BrokenPipeError, ConnectionResetError) as exc:
            self._log(f"Gagal mengirim parameter job: {exc}", "error")

        tugas = [asyncio.create_task(self._baca_stdout(proc)),
                 asyncio.create_task(self._baca_stderr(proc))]
        try:
            await asyncio.gather(*tugas)
            rc = await proc.wait()
        except asyncio.CancelledError:
            for t in tugas:
                t.cancel()
            raise
        except Exception as exc:
            rc = -1
            self._log(f"Pemantau job gagal: {exc}", "error")

        try:
            atexit.unregister(_bunuh_paksa)
        except Exception:
            pass

        self.finished_at = int(time.time() * 1000)
        if self.status != STATUS_RUNNING:
            # sudah diset oleh event done atau error
            pass
        elif rc == 0:
            self.status = STATUS_DONE
        elif rc in (-signal.SIGTERM, -signal.SIGKILL, 130, 143, 137):
            self.status = STATUS_CANCELLED
            self.error = self.error or "Dibatalkan pengguna."
        else:
            self.status = STATUS_ERROR
            self.error = self.error or f"proses berhenti dengan kode {rc}"

        if self.status == STATUS_RUNNING:
            self.status = STATUS_DONE
        self._log(f"Job selesai dengan status {self.status} (kode {rc}).")

    async def _baca_stdout(self, proc) -> None:
        """Baca NDJSON baris per baris dan perbarui state."""
        if proc.stdout is None:
            return
        while True:
            try:
                raw = await proc.stdout.readline()
            except (asyncio.LimitOverrunError, ValueError):
                # satu baris terlalu panjang: buang sisa sampai newline
                self._log("Baris keluaran terlalu panjang, dilewati.",
                          "warning")
                continue
            if not raw:
                break
            baris = raw.decode("utf-8", "replace").strip()
            if not baris:
                continue
            try:
                ev = json.loads(baris)
            except json.JSONDecodeError:
                self._log(baris)
                continue
            self._terapkan(ev)

    async def _baca_stderr(self, proc) -> None:
        """Serap stderr supaya pipa tidak penuh dan pesannya tetap terlihat."""
        if proc.stderr is None:
            return
        while True:
            raw = await proc.stderr.readline()
            if not raw:
                break
            teks = raw.decode("utf-8", "replace").rstrip()
            if teks:
                self._log(teks, "warning")

    def _terapkan(self, ev: dict) -> None:
        """Perbarui state dari satu event NDJSON."""
        jenis = ev.get("ev")
        if jenis == "log":
            self._log(str(ev.get("text", "")))
        elif jenis == "plan":
            self.plan = {k: v for k, v in ev.items() if k != "ev"}
            self._log(
                f"Rencana: {self.plan.get('n_signal', 0)} kombinasi sinyal x "
                f"{self.plan.get('n_exit', 0)} kombinasi exit = "
                f"{self.plan.get('total', 0):,} baris hasil")
        elif jenis == "phase":
            self.phase = {"key": ev.get("key", ""),
                          "label": ev.get("label", ""),
                          "total": ev.get("total", 0)}
            self.progress = {"current": 0, "total": ev.get("total", 0) or 1,
                             "pct": 0.0, "detail": ""}
        elif jenis == "progress":
            total = float(ev.get("total") or 0) or 1.0
            cur = float(ev.get("current") or 0)
            self.progress = {
                "current": cur,
                "total": total,
                "pct": round(max(0.0, min(100.0, cur / total * 100.0)), 1),
                "detail": ev.get("detail", ""),
            }
        elif jenis == "done":
            self.rows = ev.get("rows", []) or []
            self.meta = ev.get("meta", {}) or {}
            self.csv_path = ev.get("csv", "") or ""
            self.status = STATUS_DONE
            self.progress = {"current": 1, "total": 1, "pct": 100.0,
                             "detail": "selesai"}
            self.phase = {"key": "selesai", "label": "Selesai", "total": 1}
            self._log(f"Hasil siap: {len(self.rows)} baris teratas, "
                      f"CSV lengkap di {self.csv_path}")
        elif jenis == "error":
            self.status = STATUS_ERROR
            self.error = str(ev.get("text", "kesalahan tidak diketahui"))
            self._log(self.error, "error")


def _bunuh_paksa(proc) -> None:
    """Dipanggil atexit: pastikan proses anak tidak jadi yatim."""
    try:
        if proc.returncode is not None:
            return
    except Exception:
        return
    _bunuh_grup(proc, signal.SIGTERM)
    try:
        os.waitpid(proc.pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        pass


def _bunuh_grup(proc, sig: int) -> None:
    """Kirim sinyal ke seluruh grup proses anak, dengan cadangan per proses."""
    try:
        os.killpg(os.getpgid(proc.pid), sig)
        return
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.send_signal(sig)
    except (ProcessLookupError, OSError):
        pass
