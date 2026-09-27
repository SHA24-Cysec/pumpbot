"""
Penulis config.yaml yang menjaga komentar tetap utuh.

Kenapa tidak yaml.safe_dump saja? Karena config/config.yaml berisi ratusan
baris komentar penjelasan dalam bahasa Indonesia, sementara PyYAML membuang
seluruh komentar saat menulis ulang. ruamel.yaml yang bisa menjaga komentar
tidak ada di requirements, dan menambah dependency baru hanya untuk ini
berlebihan.

Jadi pendekatannya bedah per baris: cari baris kunci yang dituju berdasarkan
jalur bertitik dan indentasi, lalu ganti bagian NILAI-nya saja. Komentar di
atas baris, komentar di ujung baris, urutan kunci, dan seluruh format file
tidak tersentuh.

Alur aman yang dipakai setiap kali menulis:

    1. Tolak kunci yang tidak ada di daftar putih.
    2. Bedah salinan isi file di memori.
    3. Tulis ke file sementara di direktori yang sama.
    4. Muat ulang lewat load_backtest_config: kalau config jadi tidak sah,
       batal total dan file asli tidak pernah tersentuh.
    5. Periksa ulang bahwa nilai yang terbaca memang sama dengan yang diminta.
    6. Salin file asli ke config/backup/, baru ganti secara atomik.
"""

from __future__ import annotations

import os
import re
import shutil
import time
from typing import Any, Optional

import yaml

# ---------------------------------------------------------------------------
# Daftar putih
# ---------------------------------------------------------------------------
# Hanya kunci di bawah ini yang boleh ditulis ulang dari dashboard. Apa pun di
# luar daftar ini ditolak, jadi endpoint tidak bisa dipakai mengubah mode,
# kredensial, alamat dashboard, atau jalur database.

ALLOWED_KEYS: dict[str, type] = {
    # --- parameter exit (hasil utama optimizer) ---
    "stops.mode": str,
    "stops.percent_pct": float,
    "take_profit.mode": str,
    "take_profit.rr": float,
    "breakeven.enabled": bool,
    "breakeven.trigger_rr": float,
    "breakeven.buffer_pct": float,
    "trailing.enabled": bool,
    "trailing.percent_pct": float,
    "trailing.update_step_pct": float,
    # --- lookback detector ---
    "signal.min_candles": int,
    "signal.volume.ma_period": int,
    "signal.volume.spike_scale": float,
    "signal.price_action.structure_candles": int,
    "signal.price_action.breakout_lookback": int,
    "signal.price_action.swing_neighbors": int,
    # --- skoring (perlu kalibrasi, lihat catatan di service.py) ---
    "signal.score_threshold": float,
    "signal.weights.price_action": float,
    "signal.weights.volume": float,
    # --- cooldown ---
    "signal.cooldown_after_exit_min": float,
}

# Nilai teks yang diizinkan untuk kunci bertipe string.
# take_profit.mode mengikuti validasi bot (rr | multi | single); nilai lain
# seperti "percent" akan lolos di sini tapi ditolak load_config sehingga
# seluruh penulisan dibatalkan.
ALLOWED_ENUMS: dict[str, tuple[str, ...]] = {
    "stops.mode": ("percent", "structure"),
    "take_profit.mode": ("rr", "multi", "single"),
}

BACKUP_DIRNAME = "backup"
MAX_BACKUPS = 20


class ConfigWriteError(RuntimeError):
    """Penulisan config gagal dan file asli tidak diubah sama sekali."""


# ---------------------------------------------------------------------------
# Format nilai
# ---------------------------------------------------------------------------

def format_value(value: Any) -> str:
    """Ubah nilai Python menjadi teks YAML sederhana untuk satu baris skalar."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ConfigWriteError(f"nilai {value} tidak bisa ditulis ke YAML")
        if value == int(value) and abs(value) < 1e15:
            # 3.0 ditulis 3.0 supaya tetap terbaca sebagai angka pecahan
            return f"{value:.1f}"
        return repr(round(value, 10))
    text = str(value)
    if not text:
        return '""'
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_\-]*", text):
        return text
    return yaml.safe_dump(text, default_flow_style=True).strip().rstrip("\n...").strip()


def coerce(key: str, value: Any) -> Any:
    """Paksa nilai ke tipe yang benar untuk kunci tersebut, atau lempar error."""
    if key not in ALLOWED_KEYS:
        raise ConfigWriteError(f"kunci '{key}' tidak diizinkan diubah")
    want = ALLOWED_KEYS[key]
    try:
        if want is bool:
            if isinstance(value, str):
                low = value.strip().lower()
                if low in ("true", "1", "ya", "on"):
                    return True
                if low in ("false", "0", "tidak", "off"):
                    return False
                raise ValueError(value)
            return bool(value)
        if want is int:
            out = int(round(float(value)))
            return out
        if want is float:
            out = float(value)
            if out != out or out in (float("inf"), float("-inf")):
                raise ValueError(value)
            return out
        text = str(value).strip()
        allowed = ALLOWED_ENUMS.get(key)
        if allowed and text not in allowed:
            raise ConfigWriteError(
                f"nilai '{text}' untuk {key} harus salah satu dari "
                f"{', '.join(allowed)}")
        return text
    except ConfigWriteError:
        raise
    except (TypeError, ValueError):
        raise ConfigWriteError(
            f"nilai '{value}' untuk {key} bukan {want.__name__} yang sah")


# ---------------------------------------------------------------------------
# Bedah baris
# ---------------------------------------------------------------------------

_KEY_RE = re.compile(r"^(?P<indent>\s*)(?P<key>[A-Za-z0-9_]+)\s*:(?P<rest>.*)$")


def _indent_of(line: str) -> int:
    """Jumlah spasi indentasi sebuah baris."""
    return len(line) - len(line.lstrip(" "))


def _is_skippable(line: str) -> bool:
    """Baris kosong atau komentar murni, tidak mempengaruhi batas blok."""
    s = line.strip()
    return not s or s.startswith("#")


def _split_inline_comment(rest: str) -> tuple[str, str]:
    """
    Pisahkan bagian nilai dari komentar di ujung baris.

    Tanda pagar hanya dianggap awal komentar bila didahului spasi dan berada
    di luar tanda kutip, sesuai aturan YAML.
    """
    in_single = in_double = False
    for i, ch in enumerate(rest):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            if i == 0 or rest[i - 1] in " \t":
                return rest[:i], rest[i:]
    return rest, ""


def _find_block_end(lines: list[str], start: int, indent: int) -> int:
    """Indeks akhir (eksklusif) blok anak dari sebuah kunci induk."""
    i = start
    last_content = start
    while i < len(lines):
        line = lines[i]
        if _is_skippable(line):
            i += 1
            continue
        if _indent_of(line) <= indent:
            break
        last_content = i + 1
        i += 1
    return last_content


def _find_key_line(lines: list[str], path: list[str],
                   lo: int = 0, hi: Optional[int] = None,
                   indent: int = 0) -> tuple[Optional[int], int, int, int]:
    """
    Cari baris untuk jalur bertitik.

    Return (indeks_baris, awal_blok_induk, akhir_blok_induk, indentasi_anak).
    indeks_baris None berarti kunci terakhir belum ada tetapi induknya ketemu.
    """
    if hi is None:
        hi = len(lines)
    head, rest = path[0], path[1:]

    i = lo
    while i < hi:
        line = lines[i]
        if _is_skippable(line):
            i += 1
            continue
        m = _KEY_RE.match(line)
        if m and _indent_of(line) == indent and m.group("key") == head:
            if not rest:
                return i, lo, hi, indent
            blok_awal = i + 1
            blok_akhir = _find_block_end(lines, blok_awal, indent)
            # indentasi anak: ambil dari baris isi pertama, default +2
            anak_indent = indent + 2
            for j in range(blok_awal, blok_akhir):
                if not _is_skippable(lines[j]):
                    anak_indent = _indent_of(lines[j])
                    break
            return _find_key_line(lines, rest, blok_awal, blok_akhir,
                                  anak_indent)
        i += 1

    if len(path) == 1:
        return None, lo, hi, indent
    raise ConfigWriteError(
        f"bagian '{head}' tidak ditemukan di config.yaml")


def set_value(lines: list[str], key: str, value: Any) -> list[str]:
    """
    Ganti nilai satu kunci pada daftar baris, komentar dipertahankan.

    Bila kunci belum ada tetapi induknya ada, baris baru disisipkan di akhir
    blok induk dengan indentasi yang benar.
    """
    path = key.split(".")
    idx, blok_awal, blok_akhir, anak_indent = _find_key_line(lines, path)
    teks = format_value(value)

    if idx is None:
        baris_baru = " " * anak_indent + f"{path[-1]}: {teks}"
        sisip = blok_akhir
        return lines[:sisip] + [baris_baru] + lines[sisip:]

    m = _KEY_RE.match(lines[idx])
    if not m:  # tidak mungkin sampai sini, tapi tetap dijaga
        raise ConfigWriteError(f"baris untuk '{key}' tidak bisa dibaca")

    nilai_lama, komentar = _split_inline_comment(m.group("rest"))
    if nilai_lama.strip() == "":
        raise ConfigWriteError(
            f"'{key}' adalah blok bersarang, bukan nilai tunggal")

    # jaga jarak komentar ujung baris seperti semula bila ada
    if komentar:
        spasi = len(nilai_lama) - len(nilai_lama.rstrip())
        ekor = " " * max(1, spasi) + komentar
    else:
        ekor = ""

    out = list(lines)
    out[idx] = f"{m.group('indent')}{m.group('key')}: {teks}{ekor}"
    return out


def get_nested(data: dict, key: str) -> Any:
    """Ambil nilai bersarang dari dict hasil parse YAML memakai jalur bertitik."""
    cur: Any = data
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(key)
        cur = cur[part]
    return cur


# ---------------------------------------------------------------------------
# Cadangan
# ---------------------------------------------------------------------------

def backup_dir_for(path: str) -> str:
    """Direktori cadangan di sebelah file config."""
    return os.path.join(os.path.dirname(os.path.abspath(path)), BACKUP_DIRNAME)


def make_backup(path: str) -> str:
    """Salin config saat ini ke direktori cadangan bertanda waktu."""
    d = backup_dir_for(path)
    os.makedirs(d, exist_ok=True)
    nama = os.path.basename(path)
    batang, ext = os.path.splitext(nama)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    tujuan = os.path.join(d, f"{batang}-{stamp}{ext}")
    n = 1
    while os.path.exists(tujuan):
        tujuan = os.path.join(d, f"{batang}-{stamp}-{n}{ext}")
        n += 1
    shutil.copy2(path, tujuan)
    _prune_backups(d, batang, ext)
    return tujuan


def _prune_backups(d: str, batang: str, ext: str) -> None:
    """Sisakan MAX_BACKUPS cadangan terbaru saja."""
    try:
        items = [os.path.join(d, f) for f in os.listdir(d)
                 if f.startswith(batang + "-") and f.endswith(ext)]
    except OSError:
        return
    items.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    for p in items[MAX_BACKUPS:]:
        try:
            os.remove(p)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# API utama
# ---------------------------------------------------------------------------

def apply_updates(path: str, updates: dict[str, Any],
                  dry_run: bool = False) -> dict:
    """
    Terapkan sekumpulan perubahan ke config.yaml dengan aman.

    Return ringkasan berisi daftar perubahan, jalur cadangan, dan diff singkat.
    Melempar ConfigWriteError bila ada yang salah; pada kasus itu file asli
    dijamin tidak berubah.
    """
    if not updates:
        raise ConfigWriteError("tidak ada perubahan yang diminta")
    if not os.path.exists(path):
        raise ConfigWriteError(f"file config tidak ditemukan: {path}")

    bersih: dict[str, Any] = {}
    for k, v in updates.items():
        bersih[k] = coerce(k, v)

    with open(path, encoding="utf-8") as fh:
        asli = fh.read()
    akhiran_newline = asli.endswith("\n")
    lines = asli.splitlines()

    try:
        lama_doc = yaml.safe_load(asli) or {}
    except yaml.YAMLError as exc:
        raise ConfigWriteError(f"config.yaml saat ini tidak sah: {exc}")

    perubahan: list[dict] = []
    baru = list(lines)
    for k, v in bersih.items():
        try:
            sebelum = get_nested(lama_doc, k)
        except KeyError:
            sebelum = None
        baru = set_value(baru, k, v)
        perubahan.append({"key": k, "before": sebelum, "after": v,
                          "changed": sebelum != v})

    isi_baru = "\n".join(baru) + ("\n" if akhiran_newline else "")

    # ---- validasi 1: YAML tetap sah dan nilainya benar-benar berubah ----
    try:
        doc_baru = yaml.safe_load(isi_baru) or {}
    except yaml.YAMLError as exc:
        raise ConfigWriteError(f"hasil penulisan bukan YAML yang sah: {exc}")

    for k, v in bersih.items():
        try:
            terbaca = get_nested(doc_baru, k)
        except KeyError:
            raise ConfigWriteError(
                f"gagal memverifikasi '{k}' setelah ditulis")
        if isinstance(v, float):
            cocok = isinstance(terbaca, (int, float)) and abs(
                float(terbaca) - v) < 1e-9
        else:
            cocok = terbaca == v
        if not cocok:
            raise ConfigWriteError(
                f"verifikasi gagal untuk '{k}': ditulis {v!r} "
                f"tetapi terbaca {terbaca!r}")

    # ---- validasi 2: bot masih bisa memuat config ini ----
    tmp = f"{path}.tmp-{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(isi_baru)
        _validate_loadable(tmp)

        if dry_run:
            os.remove(tmp)
            return {"ok": True, "dry_run": True, "changes": perubahan,
                    "backup": None, "path": path}

        cadangan = make_backup(path)
        os.replace(tmp, path)
    except ConfigWriteError:
        _hapus_diam(tmp)
        raise
    except OSError as exc:
        _hapus_diam(tmp)
        raise ConfigWriteError(f"gagal menulis file: {exc}")

    return {"ok": True, "dry_run": False, "changes": perubahan,
            "backup": cadangan, "path": path}


def _hapus_diam(path: str) -> None:
    """Hapus file sementara tanpa ribut bila sudah tidak ada."""
    try:
        os.remove(path)
    except OSError:
        pass


def _validate_loadable(path: str) -> None:
    """Pastikan bot masih bisa memuat dan memvalidasi config hasil tulisan."""
    try:
        from tools.backtest.util import load_backtest_config
    except ImportError as exc:  # paket tools tidak ikut terpasang
        raise ConfigWriteError(f"validator config tidak tersedia: {exc}")
    try:
        load_backtest_config(path)
    except Exception as exc:
        raise ConfigWriteError(
            f"config hasil perubahan ditolak validator bot: {exc}")
