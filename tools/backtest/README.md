# Analisis Sensitivitas & Backtest Parameter (klines-only)

Tool ini menjawab: **"parameter mana yang stabil?"** — bukan mencari satu
set parameter ajaib, melainkan memetakan area robust (plateau) lewat
grid search + walk-forward, dengan prioritas drawdown terkendali.

> Baca batasan penting di bawah sebelum memakai hasilnya.

## Alur

```
download.py (atau synthetic.py)     engine.score_series()        grid.py
  klines 1m 20 pair × 6 bulan  ->   skor per candle memakai   -> 135 kombinasi
  data/{SYM}_1m.csv.gz             DETEKTOR ASLI bot             × walk-forward
                                                                   out/report.md
                                                                   out/heat_*.png
```

Skor dihitung SEKALI per simbol (bagian mahal ~1-2 menit/simbol); setiap
kombinasi parameter tinggal me-replay skor + simulasi posisi (murah).

## Pemakaian

```bash
cd pumpbot

# 1. SMOKE TEST dulu tanpa unduh apa pun (data sintetis, ±1 menit):
python tools/backtest/synthetic.py --selftest

# 2. Unduh data riil (endpoint publik, tanpa API key):
python tools/backtest/download.py --top 20 --months 6
#    atau simbol pilihan sendiri:
python tools/backtest/download.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --months 6
#
#    Default pakai https://data-api.binance.vision — mirror MARKET-DATA
#    PUBLIK resmi Binance (lebih ramah jaringan daripada api.binance.com).
#    Ganti dengan --base https://api.binance.com bila ingin.
#
#    Catatan --top: sejak Binance mewajibkan symbol/symbols pada
#    /api/v3/ticker/24hr, daftar pair diambil dari exchangeInfo lalu
#    statistik 24h diminta per kelompok 100 simbol (jalur terdokumentasi).
#    Listing dengan simbol non-ASCII (mis. "币安人生USDT") otomatis
#    disaring — satu simbol seperti itu membuat seluruh request
#    symbols=[...] ditolak HTTP 400 (-1100 Illegal characters).

# 3. Jalankan grid + walk-forward + laporan:
python tools/backtest/grid.py                        # default: train 42d/test 14d
python tools/backtest/grid.py --dd-limit 5 --min-trades 30
#    Alur dan batas default sama dengan live: 1 posisi, risk 1% balance,
#    BE +1R, dan trailing setelah BE. Grid menguji variasi SL, TP RR,
#    dan jarak trailing di sekitar nilai konfigurasi live.
#    Untuk menguji mode multi-posisi (maksimal 3):
python tools/backtest/grid.py --max-open 3
#
#    Studi batas umur posisi (time-stop): bandingkan beberapa nilai
#    max_hold sekaligus (0 = nonaktif). Laporan otomatis memuat bagian
#    "Analisis max_hold": % trade yang exit paksa, PnL-nya, dan efek ke
#    PF/DD — untuk memutuskan apakah fitur ini layak dipakai di live.
python tools/backtest/grid.py --focus --dd-limit 15 --min-trades 100 --max-hold 0,60,120,240
#
#    Estimasi durasi (patokan): scoring ~2 ms/candle (sekali per simbol)
#    + replay murah. 20 simbol × 6 bulan ≈ 5,2 juta candle ->
#    ±3 jam scoring + ±1,5 jam grid. Jalankan semalam, atau turunkan
#    cakupan (mis. --top 10 --months 4) untuk iterasi cepat.
#    PENTING: pastikan hanya data riil di tools/backtest/data/
#    (hapus SIM*USDT_*.csv.gz hasil selftest) agar grid tidak tercampur.

# (opsional) heatmap butuh matplotlib:
pip install matplotlib
```

Hasil ada di `tools/backtest/out/`:

| File | Isi |
|---|---|
| `report.md` | Ringkasan OOS, parameter terpilih per window, tabel sensitivitas per parameter |
| `wf_summary.csv` | Detail per window walk-forward |
| `results.csv` | Semua kombinasi × window (bisa dianalisis lanjut di Excel/pandas) |
| `heat_*.png` | Heatmap drawdown per pasangan parameter |

Untuk analisis cepat iterasi awal bisa juga: `python tools/backtest/synthetic.py --symbols 6 --days 60` lalu `grid.py --train-days 20 --test-days 7` (data sintetis hanya untuk menguji pipeline, BUKAN untuk menarik kesimpulan parameter). Gunakan `--out /tmp/bt-data` jika tidak ingin menulis data sintetis ke folder project.

## Metode

- **Walk-forward**: train N hari → pilih kombinasi terbaik (PF maksimum
  dengan syarat DD ≤ `--dd-limit` dan trade ≥ `--min-trades`) → uji di
  M hari berikutnya (out-of-sample) → geser window. Metrik yang dilaporkan
  hanya dari window TEST.
- **Risk % balance + saldo spot**: setiap entry dihitung dari balance pada
  harga modal, sehingga unrealized PnL tidak mengubah risk %. Setelah posisi
  tertutup, PnL direalisasikan ke quote balance untuk entry berikutnya. Default
  hanya satu posisi; gunakan `--max-open 2` atau `--max-open 3` untuk
  mensimulasikan mode multi-posisi.
- **Asumsi konservatif**: entry di open candle berikutnya + slippage;
  bila SL dan TP tersentuh di candle yang sama → SL dihitung dulua;
  fee 0.1% per sisi.
- **Alur exit**: satu TP penuh berbasis RR, BE aktif pada +1R, lalu
  trailing baru bergerak. Dengan default `SL 1%`, `TP RR 2`, dan trailing
  `0,5%`, serta buffer BE `0,25%`, simulasi sama dengan konfigurasi live.
- **Grid default** (di `grid.py`, silakan ubah): threshold 55–75,
  SL 1/1,5/2,5%, TP 1,5R/2R/2,5R, trailing 0,3/0,5/0,8% setelah BE.

## Batasan (WAJIB dipahami)

1. **Hanya 3 dari 6 detektor aktif**: trade_flow (proxy dari jumlah trade
   & taker-buy candle), volume, price_action, plus penalti manipulasi.
   Order book imbalance/wall dan whale/spoof **tidak bisa** diuji dari
   klines — bobotnya dinormalisasi ulang. Skor di sini ≠ skor live.
2. **Threshold hasil optimasi tidak langsung dipakai live** — karakter
   distribusi skornya beda. Gunakan temuan untuk memahami area robust
   (mis. "SL 1.5–2.5% konsisten lebih baik dari 4%" atau "threshold > 70
   membuat jumlah trade terlalu sedikit"), lalu validasi di testnet.
3. Resolusi 1 menit: entry live terjadi mid-candle, backtest tidak bisa
   meniru itu persis.

## Yang dibaca dari laporan

- **Sensitivitas per parameter**: nilai yang PnL/PF-nya datar terhadap
  pergeseran = robust. Puncak tajam di satu nilai = tanda overfit.
- **Parameter terpilih per window**: kalau window 1, 2, 3 memilih nilai
  yang mirup → parameter itu stabil antar periode. Kalau loncat-loncat →
  tidak ada sinyal yang bisa diandalkan dari parameter itu.
- **Window profit X/Y & DD**: ukuran sebenarnya dari kriteria Anda
  (drawdown terkendali).
