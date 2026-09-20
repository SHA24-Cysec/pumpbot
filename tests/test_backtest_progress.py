"""Tes untuk tools.backtest.progress.

Lingkungan pytest tidak punya TTY, jadi cabang fallback (log baris biasa)
adalah yang diuji di sini. Cabang rich diuji terpisah secara manual.
"""

from __future__ import annotations

import sys

from tools.backtest import progress
from tools.backtest.progress import Bar, ProgressUI, format_durasi, rich_aktif


def test_rich_aktif_false_tanpa_tty(capsys):
    """Output pytest bukan terminal, jadi mode interaktif harus mati."""
    assert not sys.stdout.isatty()
    assert rich_aktif() is False


def test_rich_aktif_hormati_enabled():
    """Argumen enabled=False selalu mematikan mode interaktif."""
    assert rich_aktif(enabled=False) is False


def test_format_durasi():
    assert format_durasi(5.25) == "5.2 dtk"
    assert format_durasi(65) == "1m 05d"
    assert format_durasi(3600 + 120) == "1j 02m"


def test_bar_fallback_mulai_dan_selesai(capsys):
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Memuat data", total=4)
        bar.close()
    out = capsys.readouterr().out
    assert "[ mulai ] Memuat data (total 4)" in out
    assert "[selesai] Memuat data dalam" in out


def test_bar_fallback_cetak_tiap_10_persen(capsys):
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Grid", total=10)
        for _ in range(10):
            bar.advance()
        bar.close()
    out = capsys.readouterr().out
    # Milestone 10% sampai 100% masing-masing muncul tepat satu kali.
    for pct in range(10, 101, 10):
        assert out.count(f"[{pct:>3}%  ] Grid") == 1


def test_bar_milestone_hanya_saat_melintasi(capsys):
    """Advance kecil yang belum melintasi milestone tidak mencetak apa pun."""
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Grid", total=100)
        bar.advance(5)   # 5% (< 10%)
        out = capsys.readouterr().out
        assert "%  ] Grid" not in out
        bar.advance(6)   # 11% -> lintas 10%
        out = capsys.readouterr().out
        assert out.count("[ 10%  ] Grid (11/100)") == 1
        bar.close()


def test_bar_dijepit_di_total(capsys):
    """Advance berlebih tidak menghasilkan persen di atas 100."""
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Grid", total=5)
        bar.advance(99)
        bar.close()
    out = capsys.readouterr().out
    assert "(5/5)" in out
    assert "(99/5)" not in out
    assert "(104/5)" not in out


def test_bar_quiet_tidak_mencetak(capsys):
    """Bar quiet (untuk item berulang) diam pada mode fallback."""
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Simbol kecil", total=3, quiet=True)
        bar.advance(2)
        bar.close()
    out = capsys.readouterr().out
    assert "Simbol kecil" not in out


def test_bar_total_tak_diketahui(capsys):
    """total=None atau 0 tidak menyebabkan pembagian dengan nol."""
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Tak tentu", total=None)
        bar.advance(3)
        bar.close()
    out = capsys.readouterr().out
    assert "(total ?)" in out
    assert "[selesai] Tak tentu" in out


def test_close_idempoten(capsys):
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Grid", total=2)
        bar.close()
        bar.close()
    out = capsys.readouterr().out
    assert out.count("[selesai] Grid") == 1


def test_advance_setelah_close_diabaikan(capsys):
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Grid", total=100)
        bar.close()
        bar.advance(50)
    out = capsys.readouterr().out
    assert "%  ] Grid" not in out


def test_log_fallback(capsys):
    with ProgressUI(enabled=False) as ui:
        ui.log("pesan penting")
    out = capsys.readouterr().out
    assert "pesan penting" in out


def test_bar_sebagai_context_manager(capsys):
    with ProgressUI(enabled=False) as ui:
        with ui.bar("Grid", total=2) as bar:
            bar.advance(2)
    out = capsys.readouterr().out
    assert "[selesai] Grid" in out


def test_catatan_close_ikut_tercetak(capsys):
    with ProgressUI(enabled=False) as ui:
        bar = ui.bar("Scan", total=1)
        bar.advance()
        bar.close("-> 12 entry")
    out = capsys.readouterr().out
    assert "-> 12 entry" in out


def test_langkah_persen_konstan():
    """Konstanta jarak milestone fallback tetap 10 persen."""
    assert progress.LANGKAH_PERSEN == 10
    assert Bar is not None
