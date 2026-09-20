"""Efek loading untuk tools backtest.

Memakai ``rich.progress`` bila modul ``rich`` terpasang dan output mengarah ke
terminal interaktif. Bila tidak (redirect ke file, CI, atau rich belum
terpasang), otomatis turun ke log baris biasa sehingga file log tetap rapi.

Pemakaian:

    with ProgressUI() as ui:
        bar = ui.bar("Memindai sinyal", total=len(items))
        for item in items:
            proses(item)
            bar.advance()
        bar.close()
        ui.log("tahap selesai")

Keluaran non-interaktif mencetak garis kemajuan tiap kelipatan 10 persen,
sedangkan mode interaktif menampilkan progress bar dengan ETA dan kecepatan.
"""

from __future__ import annotations

import sys
import time
from typing import Optional

try:  # pragma: no cover - ketersediaan tergantung lingkungan
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        ProgressColumn,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
    )
    from rich.text import Text

    _RICH_TERSEDIA = True
except Exception:  # pragma: no cover - rich memang opsional
    _RICH_TERSEDIA = False

# Jarak garis kemajuan pada mode fallback (persen).
LANGKAH_PERSEN = 10


def rich_aktif(enabled: bool = True) -> bool:
    """True bila progress bar interaktif boleh dipakai saat ini.

    Syaratnya: modul rich terpasang, progres tidak dimatikan lewat argumen,
    dan output menuju terminal (bukan redirect ke file atau CI).
    """
    return bool(enabled and _RICH_TERSEDIA and sys.stdout.isatty())


def format_durasi(detik: float) -> str:
    """Ubah durasi detik menjadi teks ringkas, misalnya ``1m 05d``."""
    if detik < 60:
        return f"{detik:.1f} dtk"
    menit, sisa = divmod(int(detik), 60)
    if menit < 60:
        return f"{menit}m {sisa:02d}d"
    jam, menit = divmod(menit, 60)
    return f"{jam}j {menit:02d}m"


def _buat_progress(console=None) -> "Progress":
    """Bangun objek Progress rich dengan kolom bahasa Indonesia.

    ``console`` opsional, dipakai terutama untuk pengujian render.
    """

    class _KolomKecepatan(ProgressColumn):
        """Kolom kecepatan proses; task.speed bisa None di awal."""

        def render(self, task) -> "Text":
            if task.speed is None:
                return Text("-/dtk", style="progress.data.speed")
            return Text(f"{task.speed:.1f}/dtk", style="progress.data.speed")

    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn("ETA"),
        TimeRemainingColumn(),
        _KolomKecepatan(),
        console=console,
    )


class ProgressUI:
    """Konteks bersama untuk beberapa bar progres dalam satu tahap program.

    Mode interaktif (rich) merender bar hidup; mode fallback mencetak garis
    kemajuan berkala. ``log`` selalu aman dipanggil di kedua mode tanpa
    merusak tampilan bar.
    """

    def __init__(self, enabled: bool = True):
        self._pakai_rich = rich_aktif(enabled)
        self._progress: Optional["Progress"] = None

    def __enter__(self) -> "ProgressUI":
        if self._pakai_rich:
            self._progress = _buat_progress()
            self._progress.start()
        return self

    def __exit__(self, *exc) -> bool:
        if self._progress is not None:
            self._progress.stop()
            self._progress = None
        return False

    def bar(self, deskripsi: str, total: Optional[int],
            quiet: bool = False) -> "Bar":
        """Buat satu bar progres.

        ``quiet=True`` berarti bar tidak mencetak apa pun pada mode fallback
        (dipakai untuk bar per item yang sudah punya garis log sendiri).
        """
        return Bar(self, deskripsi, total, quiet=quiet)

    def log(self, pesan: str) -> None:
        """Cetak satu baris pesan tanpa merusak progress bar yang aktif."""
        if self._progress is not None:
            self._progress.console.print(pesan)
        else:
            print(pesan)


class Bar:
    """Satu bar progres untuk satu tahap pekerjaan."""

    def __init__(self, ui: ProgressUI, deskripsi: str,
                 total: Optional[int], quiet: bool = False):
        self._ui = ui
        self.deskripsi = deskripsi
        self.total = max(0, int(total)) if total else None
        self.quiet = quiet
        self._selesai = 0
        self._tutup = False
        self._t0 = time.monotonic()
        self._milestone = LANGKAH_PERSEN
        self._task = None
        if ui._progress is not None:
            # total minimal 1 supaya rich tidak menganggap bar tak tentu.
            rich_total = max(1, self.total or 0)
            self._task = ui._progress.add_task(deskripsi, total=rich_total)
        elif not quiet:
            print(f"[ mulai ] {deskripsi} (total {self._teks_total()})")

    def _teks_total(self) -> str:
        """Teks total untuk garis log fallback."""
        return str(self.total) if self.total else "?"

    def advance(self, n: int = 1) -> None:
        """Majukan bar sebanyak n unit (dijepit agar tidak melewati total)."""
        if self._tutup:
            return
        n = max(0, int(n))
        if n == 0:
            return
        baru = self._selesai + n
        if self.total:
            baru = min(baru, self.total)
        delta = baru - self._selesai
        self._selesai = baru
        if self._task is not None:
            if delta:  # pragma: no cover
                self._ui._progress.update(self._task, advance=delta)
            return
        if self.quiet or not self.total:
            return
        pct = (self._selesai * 100) // self.total
        langkah = min(pct // LANGKAH_PERSEN * LANGKAH_PERSEN, 100)
        if langkah >= self._milestone:
            # Satu baris per lintasan milestone: lompatan besar hanya
            # mencetak milestone tertinggi yang dilintasi.
            print(f"[{langkah:>3}%  ] {self.deskripsi} "
                  f"({self._selesai}/{self.total})")
            self._milestone = langkah + LANGKAH_PERSEN

    def close(self, catatan: str = "") -> None:
        """Tutup bar: isi penuh, lalu cetak durasi pada mode fallback."""
        if self._tutup:
            return
        self._tutup = True
        durasi = time.monotonic() - self._t0
        if self._task is not None:
            if self.total:
                self._ui._progress.update(self._task,
                                          completed=self.total)  # pragma: no cover
            self._ui._progress.remove_task(self._task)  # pragma: no cover
            self._task = None
        if self.quiet:
            return
        ekor = f" {catatan}" if catatan else ""
        self._ui.log(f"[selesai] {self.deskripsi} "
                     f"dalam {format_durasi(durasi)}{ekor}")

    def __enter__(self) -> "Bar":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False
