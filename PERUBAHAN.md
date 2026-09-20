# Daftar Perubahan - Tools Backtest PumpBot

Arsip ini berisi file yang DIBUAT dan DIUBAH. File yang DIHAPUS tentu saja
tidak ada isinya, jadi dicatat di bagian bawah supaya Anda bisa menghapusnya
sendiri di repo.

## File BARU

```
tools/__init__.py
tools/backtest/__init__.py
tools/backtest/util.py          pemuat config aman + konversi CSV ke Candle
tools/backtest/download.py      unduh klines publik ke CSV (urllib, tanpa API key)
tools/backtest/signals.py       pencari entry proksi candle only
tools/backtest/engine.py        simulasi satu trade dan portofolio
tools/backtest/optimize.py      grid search, skor gabungan, OOS, CLI
tools/backtest/progress.py      progress bar rich + fallback log baris biasa
tools/backtest/README.md        dokumentasi cara pakai
tests/test_backtest_download.py
tests/test_backtest_signals.py
tests/test_backtest_engine.py
tests/test_backtest_optimize.py
tests/test_backtest_progress.py
```

Catatan: `tools/backtest/util.py` tidak ada di struktur yang Anda minta. Saya
menambahkannya karena pemuat config berpatch-env dan konverter CSV-ke-Candle
dipakai oleh ketiga modul lain; menaruhnya di salah satu modul akan membuat
import melingkar.

## File DIUBAH

```
README.md                          tambah satu tautan ke tools/backtest/README.md
requirements.txt                   tambah rich>=13.0 (opsional, untuk progress bar)
tests/test_download_top_symbols.py hapus kelas TestTopSymbols (stale),
                                   pertahankan TestGetUniverseChunking
```

## Pembaruan: efek loading (per 2026-09-19)

- Modul baru `tools/backtest/progress.py`: progress bar memakai `rich` dengan
  ETA, penghitung n/total, dan kecepatan per detik. Otomatis turun ke log
  baris biasa (garis kemajuan tiap 10 persen) bila output bukan terminal,
  dijalankan di CI, atau `rich` belum terpasang.
- Semua tahap diberi progres: download menampilkan satu bar keseluruhan plus
  bar per simbol (bagian yang sudah di-cache langsung dihitung maju),
  sedangkan optimize menampilkan bar untuk muat data, scan in sample, grid,
  scan OOS, dan validasi OOS top K.
- Kedua CLI menerima flag baru `--no-progress` untuk memaksa log polos.
- `rich` memang opsional: tanpa modul itu tools tetap berjalan normal.
- Mode paralel (`--workers`) ikut terlaporkan rapi: penghitung maju tiap
  simbol atau kombinasi selesai, dikerjakan di proses utama.

## File DIHAPUS (lakukan manual di repo Anda)

Tiga test ini mengimpor `tools.backtest` versi lama dan error saat collection:

```bash
git rm tests/test_backtest_parity.py
git rm tests/test_engine_max_hold.py
git rm tests/test_grid_cache.py
```

Selain itu, kelas `TestTopSymbols` di `tests/test_download_top_symbols.py`
sudah dibuang (menguji downloader lama berbasis SDK). Kelas
`TestGetUniverseChunking` di file yang sama menguji `BinanceGateway` dan
DIPERTAHANKAN utuh, sesuai permintaan Anda.

## Cara menerapkan

```bash
# dari root repo pumpbot
unzip -o pumpbot-backtest.zip
git rm tests/test_backtest_parity.py tests/test_engine_max_hold.py tests/test_grid_cache.py
pytest -q
```

Status terakhir di sandbox saya: 161 passed, 1 skipped. Pyflakes bersih,
ruff (E, F, W) bersih pada semua file baru.

## Cara pakai singkat

```bash
python -m tools.backtest.download --top 30 --days 30 --interval 1m
python -m tools.backtest.optimize --days 30 --top 10 --oos 0.3 --workers 4
```

Threshold entry perlu dikalibrasi ulang: skor di backtest hanya proksi candle
(price action + volume), skalanya berbeda dari skor live yang memakai lima
detector. Detail ada di `tools/backtest/README.md`.

## Catatan performa (hasil pengukuran)

Pada 30 simbol x 30 hari 1m (1.296.000 candle):

- `load_data` 10,5 detik, RSS sekitar 640 MB
- scanning entry sekitar 890 candle per detik per core (tahap paling berat)
- grid 1290 kombinasi sekitar 23 detik
- estimasi total: 1 core sekitar 24 menit, 4 core sekitar 7 menit

Dua hal yang perlu diperhatikan:

1. Pruning baru aktif bila `--threshold` di atas 60. Dengan bobot config
   sekarang (price_action 0.30, volume 0.20), batas atas skor adalah
   `60 + 0.4 x vol_score`. Pada threshold 70 scanning 6,5x lebih cepat
   dibanding threshold 35.
2. Pemakaian RAM kira-kira `640 MB x (workers + 1)` karena setiap proses
   pekerja memegang salinan data sendiri. Turunkan `--workers` bila RAM di
   bawah 4 GB.

## Pembaruan: optimasi scan 40x (per 2026-09-19)

Keluhan nyata: scan 365 hari x 10 simbol tidak selesai dalam 2 jam di Windows.

Akar masalah ada dua, semuanya di `tools/backtest/signals.py`:

1. `synthetic_ticker` menghitung ulang max/min/jumlah atas jendela 1440 candle
   untuk setiap candle. Digantikan `RollingTicker` (deque monoton + jumlah
   berjalan). Fungsi naif dipertahankan sebagai referensi pada test parity.
2. Buffer detector dibuat hampir tanpa batas (`max_candles = n + 1`), padahal
   setiap pemanggilan detector mengeksekusi `list(buf.candles)`. Akibatnya
   scan menjadi kuadratik pada data panjang. Buffer kini dibatasi oleh
   `_buffer_cap(cfg)` (sekitar 2x lookback maksimum detector).

Hasil terukur pada 100 ribu candle, threshold < 60: 390 -> 16.000 candle per
detik (40.6x) dengan daftar entry yang identik persis. Diuji oleh
`test_scan_hasil_sama_dengan_buffer_tanpa_batas`,
`test_rolling_ticker_identik_dengan_naif`, dan
`test_rolling_ticker_mulai_dari_tengah`, serta test scan parity baru.

## Pembaruan: grid parameter sinyal (per 2026-09-19)

Permintaan: tambahkan parameter optimasi lain yang berpengaruh selain exit.

- Grid kartesius dua dimensi di `optimize.py`: grid SINYAL (threshold,
  rasio bobot price action vs volume, ma_period, spike_scale,
  structure_candles, breakout_lookback, swing_neighbors, min_candles) kali
  grid EXIT lama. Tiap kombinasi sinyal di-scan sekali lewat `variant_cfg`
  (deepcopy config), lalu seluruh grid exit berjalan di atas entry-nya.
- `cooldown_after_exit_min` menjadi dimensi pada grid exit lewat bidang baru
  `Params.cooldown_min` di engine (None = ikut config).
- CSV hasil kini membawa kolom sinyal dan cooldown_min; tabel terminal
  menampilkan kolom ringkas THR/WPA/MA/SPK/STR/BRK/CD. Validasi OOS top K
  memakai entry dari kombinasi sinyal masing-masing baris.
- YAML tetap parameter exit saja (keputusan pengguna); parameter sinyal baris
  teratas dicetak sebagai laporan satu baris.
- Hemat biaya: dengan >1 kombo sinyal, grid exit berjalan serial di tiap kombo
  agar tidak membuat pool proses baru berkali-kali.
- Tes baru: kartesius/dedup grid sinyal, validasi w_pa, isolasi variant_cfg,
  dimensi cooldown pada grid dan engine, parser int, dan kolom CSV.

## Pembaruan: diagnostik edge sinyal (per 2026-09-19)

Permintaan: cara membuktikan apakah sinyal entry sendirian punya edge, tanpa
tercampur parameter exit. Hasil analisis 5 run grid (exit dan sinyal) semuanya
negatif, jadi inilah uji penutup untuk data klines.

- Modul baru `tools/backtest/signals_probe.py`: beli di open candle entry,
  jual persis H menit kemudian (tanpa exit), untuk tiap kombinasi threshold x
  rasio bobot. Setiap himpunan sinyal dibandingkan baseline entry acak
  berjumlah sama (seed tetap) lewat uji z.
- Statistik per horizon: n, rata rata, CI 95%, median, persen positif, rata
  rata acak, selisih, p-value, plus kesimpulan otomatis (EDGE MELEBIHI FEE /
  POSITIF TAPI DI BAWAH FEE / LEBIH BURUK DARI ACAK / TIDAK SIGNIFIKAN).
- Satu bug logika tertangkap saat smoke test: selisih vs acak sebelumnya
  hanya menuntut p < 0.05 (dua sisi), sehingga sinyal yang signifikan LEBIH
  BURUK dari acak bisa salah dibaca sebagai edge. Diperbaiki: selisih wajib
  positif; kasus negatif diberi label tersendiri.
- Tes baru: 19 kasus di tests/test_backtest_probe.py (forward_returns relatif
  indeks dan tail, random_entries deterministik dan batas indeks, summarize,
  compare, semua cabang verdict).

## Pembaruan: memangkas tahap grid exit (per 2026-09-19)

Motivasi: di run 9 kombo sinyal pengguna, tahap grid exit 4x lebih lambat dari
scan (1 jam 1 menit vs 27 menit). Penyebabnya: ratusan ribu entry di-sort
ulang di dalam simulate_portfolio untuk SETIAP simulasi.

- engine.simulate_portfolio kini menerima presorted=True (default False,
  perilaku lama utuh): pemanggil menjamin entries terurut (ts_entry, symbol)
  seperti keluaran scan_all, sehingga sort ulang dilewati.
- optimize meneruskannya di fase grid dan fase validasi OOS; run_grid juga
  meneruskan ke proses pekerja saat paralel.
- Terukur pada 299.900 entry: 0.08 -> 0.02 detik per simulasi (sekitar 4x),
  jumlah trade identik. Diuji oleh test_presorted_hasil_identik_dengan_
  sort_biasa.
