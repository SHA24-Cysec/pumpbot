# Backtest dan Optimasi Parameter Exit

Tools untuk mencari kombinasi Stop Loss, Take Profit, Breakeven, dan Trailing
Stop yang paling cocok, berdasarkan data klines historis Binance Spot.

Semua logika exit memakai fungsi murni milik bot
(`bot/risk_management/stops.py` dan `sizing.py`) supaya hasilnya sejajar dengan
`PositionManager`. Tidak ada perilaku bot live yang diubah.

Tools ini **tidak memakai API key** dan **tidak memakai BinanceGateway**.
Hanya endpoint market data publik.

> **Lebih suka tanpa terminal?** Seluruh alur di halaman ini tersedia lewat
> panel **Backtest & Optimasi** di dashboard web, lengkap dengan unduh data
> otomatis, progress bar, tabel peringkat, dan tombol untuk menulis hasil ke
> `config/config.yaml`. Lihat [README utama](../../README.md#backtest--optimasi-dari-dashboard).
> Jembatannya adalah `tools/backtest/service.py`, dijelaskan di bagian 7.

## 1. Unduh data

```bash
python -m tools.backtest.download --top 30 --days 30 --interval 1m
```

Opsi:

| Flag         | Default          | Keterangan                                   |
| ------------ | ---------------- | -------------------------------------------- |
| `--top`      | 30               | jumlah simbol dengan volume 24 jam tertinggi |
| `--days`     | 30               | panjang riwayat dalam hari                    |
| `--interval` | 1m               | `1m` `3m` `5m` `15m` `30m` `1h` `2h` `4h` `6h` `8h` `12h` `1d` |
| `--symbols`  | (kosong)         | daftar simbol manual, melewati filter volume  |
| `--data-dir` | data/backtest    | lokasi cache CSV                              |
| `--no-progress` | mati          | paksa log baris biasa (tanpa animasi)      |

Cache disimpan di `data/backtest/{SYMBOL}_{interval}.csv`, terurut naik, tanpa
duplikat, dan bisa dilanjutkan. Menjalankan ulang perintah yang sama hanya
mengunduh candle yang belum ada. Simbol yang datanya bolong ditandai di
ringkasan akhir.

Detail teknis:

- Host utama `data-api.binance.vision` (khusus market data), fallback
  `api.binance.com`.
- `GET /api/v3/ticker/24hr?type=MINI` satu kali untuk daftar pair, lalu disaring
  dengan aturan yang sama seperti bot (buang stablecoin dan leveraged token,
  hormati `universe.min_quote_volume_24h` dan `universe.exclude_symbols`).
- `GET /api/v3/klines` dengan `limit=1000` (maksimum menurut dokumentasi resmi,
  bobot 2 per request). Paging memakai `startTime = open_time terakhir + 1`.
- Header `x-mbx-used-weight-1m` dipantau; bila mendekati batas 6000 per menit
  proses tidur sejenak. HTTP 429 dan 418 menghormati `Retry-After`, error
  jaringan diulang dengan exponential backoff, HTTP 400 gagal cepat.
- Timestamp di atas 1e14 dianggap mikrodetik lalu dibagi 1000. Candle terakhir
  yang belum close selalu dibuang.

## 2. Optimasi

```bash
python -m tools.backtest.optimize --days 30 --top 10 --oos 0.3 --workers 4
```

Opsi penting:

| Flag                        | Default | Keterangan                                        |
| --------------------------- | ------- | ------------------------------------------------- |
| `--oos`                     | 0.3     | porsi data akhir untuk validasi out of sample      |
| `--min-trades`              | 30      | kombinasi dengan trade lebih sedikit didiskualifikasi |
| `--threshold`               | config  | ambang skor entry proksi                           |
| `--risk-pct`                | config  | override `risk.risk_per_trade_pct`                 |
| `--workers`                 | 1       | jumlah proses paralel                              |
| `--thr`                     | 40,50,60 | daftar threshold sinyal, dipisah koma             |
| `--wpa`                     | 0.5,0.6,0.7 | daftar rasio bobot price action vs volume      |
| `--ma-period` `--spike-scale` | config | daftar parameter VolumeDetector                 |
| `--structure` `--breakout` `--swing` | config | daftar parameter PriceActionDetector |
| `--min-candles`             | config  | daftar signal.min_candles                          |
| `--cooldown`                | config  | daftar cooldown sesudah exit (menit)               |
| `--no-progress`             | mati    | paksa log baris biasa (tanpa animasi)              |
| `--w-pnl` `--w-pf` `--w-dd` | 0.4 / 0.3 / 0.3 | bobot skor gabungan                        |
| `--sl` `--tp` `--be` `--be-buffer` `--trail` | grid default | override daftar nilai, dipisah koma |

Semua parameter daftar (`--sl`, `--thr`, dll.) juga menerima rentang otomatis
`awal..akhir` atau `awal..akhir:langkah`, contoh `--sl 0.5..2.0:0.25` menjadi
0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0. Rentang menurun seperti `2.0..0.5:0.5`
sah, dan satu argumen boleh mencampur keduanya: `--thr 35,40..60:5,70`.

Grid default:

- `sl_pct`: 0.5, 0.75, 1.0, 1.5, 2.0, 3.0 (nilai di luar
  `stops.min_stop_pct`..`max_stop_pct` dibuang dengan peringatan)
- `tp_rr`: 1.0, 1.5, 2.0, 3.0, 4.0
- `be_rr`: 0, 0.5, 0.75, 1.0 (0 berarti breakeven mati)
- `be_buffer_pct`: 0.2, 0.3, 0.5
- `trail_pct`: 0, 0.3, 0.5, 0.75, 1.0 (0 berarti trailing mati)
- `trail_step_pct` selalu diambil dari config

Grid di atas adalah grid EXIT. Sejak versi ini ada juga grid SINYAL yang
di-kartesius dengan grid exit (satu grid gabungan): threshold, rasio bobot
price action vs volume, ma_period, spike_scale, structure_candles,
breakout_lookback, swing_neighbors, dan min_candles. Prinsip kerjanya hemat:
tiap kombinasi sinyal di-scan sekali (bagian termahal), lalu seluruh grid exit
dijalankan di atas entry yang sama. `cooldown_after_exit_min` masuk ke grid
exit karena diterapkan saat simulasi, bukan saat scan.

Perkiraan biaya: tiap kombinasi sinyal = satu scan penuh atas data in sample
(dan out of sample bila aktif). Dengan data 365 hari x 10 simbol, satu scan
memakan menitan, jadi jaga jumlah kombinasi sinyal tetap puluhan, bukan
ratusan. Defaultnya 3 threshold x 3 rasio bobot = 9 scan.

Cuplikan YAML di akhir output hanya memuat parameter exit; parameter sinyal
baris teratas ditampilkan sebagai laporan satu baris di atas cuplikan dan
tercatat lengkap di kolom-kolom awal CSV hasil.

Kanonikalisasi mencegah kombinasi duplikat: `be_rr <= 0` atau `be_rr >= tp_rr`
berarti breakeven mati, buffer tidak relevan, dan trailing dipaksa mati karena
di bot trailing hanya aktif setelah breakeven.

Skor gabungan memakai min-max normalisasi lintas kombinasi:

```
skor = w_pnl x N(net_return) + w_pf x N(min(PF, 5)) + w_dd x (1 - N(max_dd))
```

Output: tabel top K di terminal, CSV lengkap di `data/backtest/results.csv`, dan
cuplikan YAML siap tempel untuk `stops`, `take_profit`, `breakeven`, dan
`trailing`.

## Progres dan efek loading

Kedua CLI menampilkan progress bar beranimasi lengkap dengan ETA, penghitung
`n/total`, dan kecepatan proses per detik. Tampilannya memakai modul `rich`
dan aktif otomatis bila dua syarat terpenuhi: `rich` terpasang (`pip install
rich`, sudah tercatat di `requirements.txt`) dan output mengarah ke terminal.

Bila output dialihkan ke file, dijalankan lewat cron/CI, atau `rich` belum
terpasang, tools otomatis turun ke log baris biasa yang tetap informatif:

```
[ mulai ] Memindai sinyal (in sample) (total 50)
[ 10%  ] Memindai sinyal (in sample) (5/50)
[ 50%  ] Memindai sinyal (in sample) (25/50)
[100%  ] Memindai sinyal (in sample) (50/50)
[selesai] Memindai sinyal (in sample) dalam 2m 31d
```

Tahap yang punya progress bar:

- `download`: satu bar keseluruhan per jumlah simbol, plus satu bar per simbol
  berdasarkan estimasi jumlah candle (bagian yang sudah ada di cache langsung
  dihitung maju).
- `optimize`: memuat cache candle, memindai sinyal in sample, menjalankan
  grid, memindai sinyal OOS, dan validasi OOS top K.

Untuk mematikan animasi secara manual (misalnya agar log pipa tetap polos),
tambahkan `--no-progress` pada perintah apa pun.

## 3. Aturan simulasi

Entry terisi di **OPEN candle i+1**, bukan close candle sinyal.

Intrabar pesimistis, urutan per candle:

1. Bila `low <= stop`, exit di `min(stop, open)` dikurangi slippage. Gap turun
   otomatis terisi di open yang lebih buruk. SL selalu dicek lebih dulu dari TP.
2. Bila `high >= TP`, exit di `max(TP, open)` tanpa slippage (limit order).
3. Baru setelah itu highest, breakeven, dan trailing diperbarui, dan hasilnya
   hanya berlaku untuk candle berikutnya.

Urutan breakeven lalu trailing meniru `PositionManager` persis: trailing hanya
aktif setelah breakeven terpicu, monoton naik, dan stop baru hanya dipakai bila
kenaikannya memenuhi `update_step_pct`.

Portofolio mengikuti `risk.allow_multiple_positions: false`: entry diurutkan
menurut waktu lintas simbol, hanya satu posisi terbuka, entry yang jatuh saat
posisi lain berjalan dilewati, dan `signal.cooldown_after_exit_min` berlaku per
simbol. Sizing compounding memakai `compute_raw_qty` dan
`clamp_to_available_balance` milik bot.

## 4. Skor entry adalah PROKSI CANDLE ONLY

Klines tidak memuat order book, aliran trade, maupun aktivitas whale. Karena itu
skor entry hanya memakai dua detector asli bot yang bisa jalan dengan candle:
`PriceActionDetector` dan `VolumeDetector`. Bobot keduanya diambil dari config
lalu dinormalisasi ulang hanya di antara detector yang tersedia. Penalti dan
veto `ManipulationDetector` tetap diterapkan persis seperti
`SignalEngine.evaluate`, dengan `Ticker24h` sintetis yang dihitung dari candle 24
jam sebelumnya.

Konsekuensinya: **skala skor di sini berbeda dari skor live** yang memakai lima
detector. Threshold wajib dikalibrasi ulang, jangan langsung memakai angka 70
dari config.

## 4b. Filter Anchored VWAP di backtest

`scan_symbol` memanggil `VWAPFilter` tepat setelah pengecekan veto dan
threshold, sebelum `Entry` dibuat, bila `signal.vwap.enabled` bernilai true.
Candle yang ditolak filter dilewati; logika prune tidak berubah, dan
`_buffer_cap()` otomatis diperluas sebesar
`anchor_lookback_candles + pump_baseline_candles` supaya buffer backtest tidak
lebih pendek daripada jalur live.

Flag optimizer:

```bash
python -m tools.backtest.optimize --vwap off   # filter dimatikan
python -m tools.backtest.optimize --vwap on    # filter dipaksa aktif
python -m tools.backtest.optimize --vwap config  # default, ikut config.yaml
```

Status filter dicetak di header hasil.

**Interval.** Default `--interval` backtest adalah `1m`, sama dengan default
live, jadi `anchor_lookback_candles: 60` mewakili rentang waktu yang sama
(60 menit). Tools download hanya mendukung `1m` dan `5m`. Bila backtest
dijalankan dengan `--interval 5m`, semua parameter VWAP (yang satuannya CANDLE)
harus disesuaikan: `anchor_lookback_candles: 60` pada 5m berarti 5 jam, bukan
1 jam. Perkecil nilainya (mis. 12 candle = 1 jam) agar setara.

**Kalibrasi.** Karena skor di sini proksi candle only (hanya price_action dan
volume), pasangan `score_threshold` dan `vwap.max_above_pct` yang optimal di
backtest tidak otomatis optimal di live. Kalibrasi ulang keduanya, dan bandingkan
selalu run `--vwap off` dengan `--vwap on` pada data yang sama.

## Diagnostik edge sinyal (tanpa exit)

Pertanyaan paling mendasar sebelum optimasi apa pun: "setelah sinyal bunyi,
harga rata rata naik melebihi fee atau tidak?" Modul `signals_probe`
menjawabnya secara langsung: beli di open candle entry, jual persis H menit
kemudian, tanpa SL/TP/breakeven/trailing sama sekali, lalu bandingkan dengan
baseline entry pada waktu acak.

```bash
python -m tools.backtest.signals_probe --days 365 --thr 40,50,60 \
    --horizons 15,30,60,240 --workers 4
```

Opsi penting: `--thr` dan `--wpa` (daftar kombinasi sinyal, tiap kombinasi
satu scan), `--horizons` (menit), `--fee` (round trip, default 2x fee_pct),
`--random-mult` (kelipatan jumlah baseline acak), `--seed`, `--workers`,
`--no-progress`.

Hasil tiap sel dinilai dengan dua syarat sekaligus: batas bawah CI 95% rata
rata return melewati fee, DAN sinyal signifikan lebih baik dari acak
(p < 0.05 dengan selisih positif). Empat kesimpulan mungkin: `EDGE MELEBIHI
FEE` (satu satunya yang layak dioptimasi exit-nya), `POSITIF TAPI DI BAWAH
FEE`, `LEBIH BURUK DARI ACAK`, atau `TIDAK SIGNIFIKAN / NOL`. Catatan: sinyal
berdekatan waktunya berkorelasi, jadi p-value bersifat sedikit optimis.

## Kinerja scan

Dua optimasi di `signals.py` membuat scan berjalan dalam waktu linear (O(n))
terhadap panjang riwayat, dengan hasil yang identik persis (diuji):

- `RollingTicker`: maksimum, minimum, dan total volume pada jendela 24 jam
  dihitung inkremental (deque monoton + jumlah berjalan), bukan dihitung
  ulang untuk setiap candle. Fungsi naif `synthetic_ticker` tetap ada sebagai
  referensi parity pada test.
- Buffer candle detector dibatasi pada kebutuhan riwayat nyata
  (`_buffer_cap`, sekitar 2x lookback maksimum). Tanpa pembatasan ini,
  `list(buf.candles)` di dalam detector menelan biaya O(riwayat) per candle
  sehingga scan menjadi kuadratik pada data panjang.

Patokan terukur (satu core): sekitar 16 ribu candle per detik pada data 100
ribu candle dengan threshold di bawah 60 (pruning tidak aktif), kurang lebih
40x lebih cepat dibanding implementasi awal, pada entri yang sama.

## 5. Batasan yang diketahui

- Skor entry hanya proksi candle, bukan replika skor live.
- Resolusi intrabar terbatas pada candle 1 menit; urutan tick di dalam candle
  tidak diketahui sehingga dipakai asumsi pesimistis.
- Take profit multi target tidak dioptimasi. (Trailing ATR sudah dihapus
  dari bot; trailing yang diuji hanya mode persen.)
- Filter LOT_SIZE dan MIN_NOTIONAL diabaikan (qty dianggap pecahan bebas).
- Tidak ada model partial fill maupun dampak likuiditas.
- Hasil backtest bukan jaminan hasil live.

## 6. Test

```bash
pytest -q
```

Seluruh test backtest berjalan tanpa jaringan; downloader diuji dengan HTTP
palsu (`urllib` di-patch).

## 7. `service.py`: penggerak panel dashboard

`tools/backtest/service.py` adalah pembungkus yang dipakai dashboard. Ia
menjalankan alur yang sama dengan `optimize`, tetapi melaporkan kemajuannya
sebagai **NDJSON** (satu objek JSON per baris) di stdout supaya bisa
ditampilkan sebagai progress bar.

```bash
# job dibaca dari stdin sebagai satu objek JSON
echo '{"symbols":["BTCUSDT"],"days":7,"sl":[1.0,1.5],"tp":[2.0,3.0]}' \
  | python -u -m tools.backtest.service --job -
```

Jenis baris yang dikeluarkan:

| Tipe | Isi |
|---|---|
| `plan` | jumlah kombinasi sinyal, kombinasi exit, total baris, daftar simbol |
| `phase` | fase yang mulai berjalan: `persiapan`, `unduh`, `muat`, `scan`, `grid`, `oos`, `selesai` |
| `progress` | `current` dari `total` untuk fase berjalan |
| `log` | pesan teks untuk panel log |
| `done` | seluruh baris hasil, path CSV, dan metadata ringkasan |
| `error` | pesan kegagalan |

Tiga fase inti:

1. **Scan entry.** Sinyal dipindai sekali per kombinasi parameter entry, hasilnya
   dipakai ulang oleh semua kombinasi exit. Inilah alasan menambah nilai SL atau
   TP jauh lebih murah daripada menambah nilai threshold.
2. **Grid exit.** Setiap kombinasi exit disimulasikan pada daftar sinyal tadi.
3. **Uji out-of-sample.** Hanya peringkat teratas yang diuji ulang pada potongan
   data terakhir.

Setiap baris hasil membawa cuplikan `yaml` siap tempel dan objek `apply` berisi
nilai per grup config, yang dipakai tombol *Terapkan ke config* di dashboard.
