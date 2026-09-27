# 🤖 PumpBot - Binance Spot Pump Detector & Auto Trading Bot

Bot trading otomatis untuk **Binance Spot** yang:

1. **Mencari koin berpotensi pump** lewat 6 detector paralel (order book, frekuensi
   trade, volume, whale/bandar, manipulasi, price action + fibonacci).
2. **Masuk posisi dengan manajemen risiko ketat** - ukuran posisi dihitung dari
   jarak stop loss sehingga rugi maksimum per trade selalu sesuai persen yang
   Anda tentukan.
3. **Mengelola posisi otomatis**: stop loss, take profit (single/multi/partial),
   breakeven, dan trailing stop (percent / ATR).
4. **Dashboard web real-time** (FastAPI + WebSocket): saldo, posisi, histori,
   statistik performa, equity curve, panel skor sinyal, dan kontrol manual
   (pause/resume, tutup posisi, ubah parameter risiko tanpa restart).

Dibangun di atas **SDK resmi Binance `binance-sdk-spot`** (REST API + WebSocket
Streams). Library lama `binance-connector` sudah deprecated oleh Binance dan
tidak dipakai di sini.

> ## ⚠️ DISCLAIMER - BACA DULU
> **Bot ini menyangkut uang sungguhan.**
> - Wajib diuji di **mode `paper` (akun demo)** sampai hasilnya konsisten
>   sebelum menyentuh mode `live`.
> - **Tidak ada strategi trading yang pasti profit.** Deteksi pump bukan
>   jaminan; pump sering diikuti dump. Forward test dulu dengan
>   modal kecil.
> - Risiko sepenuhnya tanggung jawab Anda. Gunakan modal yang siap Anda rugikan.
> - Author tidak bertanggung jawab atas kerugian apa pun.

---

## Daftar Isi

1. [Arsitektur & Alur Kerja](#arsitektur--alur-kerja)
2. [Struktur Folder](#struktur-folder)
3. [Persyaratan](#persyaratan)
4. [Instalasi](#instalasi)
5. [Mode Paper: Akun Demo](#mode-paper-akun-demo)
6. [Menjalankan Bot](#menjalankan-bot)
7. [Dashboard Web](#dashboard-web)
8. [Backtest & Optimasi dari Dashboard](#backtest--optimasi-dari-dashboard)
9. [Cara Kerja Strategi](#cara-kerja-strategi)
10. [Manajemen Risiko (Revisi)](#manajemen-risiko-revisi)
11. [Testing](#testing)
12. [Dust Sweep](#dust-sweep-konversi-sisa-koin-kecil-ke-bnb)
13. [Pemulihan Setelah Restart](#pemulihan-setelah-restart)
14. [Upgrade ke PostgreSQL](#upgrade-ke-postgresql)
15. [Troubleshooting / FAQ](#troubleshooting--faq)

---

## Arsitektur & Alur Kerja

```
                    ┌─────────────────────────────────────────────┐
                    │              BINANCE (SDK resmi)             │
                    │  REST: order, saldo, exchangeInfo, klines    │
                    │  WS Streams: kline, aggTrade, depth, ticker  │
                    └───────────────┬─────────────────────────────┘
                                    │
        ┌───────────────────────────▼───────────────────────────┐
        │  DATA COLLECTOR                                          │
        │  • watchlist otomatis (filter volume 24 jam)            │
        │  • buffer memori rolling (candle/trade/book/ticker)     │
        └───────────────────────────┬───────────────────────────┘
                                    │ tiap reevaluate_sec
        ┌───────────────────────────▼───────────────────────────┐
        │  SIGNAL ENGINE - 6 detector, skor gabungan 0-100        │
        │  orderbook • trade_flow • volume • whale • manipulasi   │
        │  (penalti/veto) • price_action (breakout+fib)           │
        └───────────────────────────┬───────────────────────────┘
                                    │
        ┌───────────────────────────▼───────────────────────────┐
        │  FILTER ANCHORED VWAP (gate, opsional)                  │
        │  • VWAP dari awal pump sampai candle closed terakhir    │
        │  • entry hanya bila jarak harga ke VWAP di zona sehat   │
        │  • tidak menyumbang skor, hanya boleh membatalkan       │
        └───────────────────────────┬───────────────────────────┘
                    skor ≥ threshold & lolos filter
                                    │
        ┌───────────────────────────▼───────────────────────────┐
        │  RISK MANAGEMENT                                         │
        │  • position sizing dari jarak SL (risk % tetap)         │
        │  • risk % balance • mode posisi • rugi harian opsional │
        └───────────────────────────┬───────────────────────────┘
                                    │
        ┌───────────────────────────▼───────────────────────────┐
        │  EXECUTION (REST)                                        │
        │  • entry market/limit • OCO (TP limit + SL stop-limit)  │
        │  • partial TP per chunk • auto-close bila OCO gagal     │
        └───────────────────────────┬───────────────────────────┘
                                    │
        ┌───────────────────────────▼───────────────────────────┐
        │  POSITION MANAGER (loop 1 detik)                        │
        │  • breakeven → trailing stop (percent/ATR)              │
        │  • eksekusi TP/SL manual • rekonsiliasi status OCO      │
        │  • watchdog darurat                                      │
        └───────────────┬─────────────────────────┬───────────────┘
                        │                         │
        ┌───────────────▼──────────┐  ┌───────────▼───────────────┐
        │  DATABASE (SQLite/WAL)   │  │  DASHBOARD (FastAPI+WS)   │
        │  trades, events, sinyal, │  │  real-time + kontrol      │
        │  equity snapshot, kv     │  │  manual via browser       │
        └──────────────────────────┘  └───────────────────────────┘
```

## Struktur Folder

```
pumpbot/
├── run.py                      # entry point CLI
├── requirements.txt
├── .env.example                # template API key & env (copy ke .env)
├── conftest.py
├── config/
│   └── config.yaml             # satu-satunya konfigurasi bot & strategi
├── bot/
│   ├── main.py                 # BotApp: orkestrasi semua modul
│   ├── config.py               # loader + VALIDASI konfigurasi
│   ├── models.py               # dataclass inti (Candle, Position, dll)
│   ├── portfolio.py            # pelacak saldo & equity
│   ├── position_manager.py     # loop BE/trailing/SL/TP/rekonsiliasi
│   ├── utils.py                # logging, notifikasi Telegram, helper
│   ├── exchange/
│   │   ├── gateway.py          # interface exchange (abstrak)
│   │   ├── binance_gateway.py  # implementasi binance-sdk-spot (REST+WS)
│   │   ├── paper_gateway.py    # AKUN DEMO: data pasar nyata, dana virtual
│   │   └── simulated_gateway.py# simulator offline (khusus unit test)
│   ├── data_collector/
│   │   ├── buffers.py          # rolling buffer per simbol
│   │   └── collector.py        # watchlist + langganan stream + seed REST
│   ├── signal_engine/
│   │   ├── detectors.py        # 6 detector pump
│   │   ├── vwap.py             # filter Anchored VWAP (gate entry, PURE)
│   │   └── engine.py           # penggabung skor, threshold, cooldown
│   ├── risk_management/
│   │   ├── sizing.py           # position sizing (PURE, unit-test)
│   │   ├── stops.py            # SL/TP/BE/trailing (PURE, unit-test)
│   │   ├── stats.py            # win rate, PF, drawdown (PURE)
│   │   └── manager.py          # batas risiko + parameter runtime
│   ├── execution/
│   │   └── executor.py         # order entry/exit/OCO/partial
│   ├── database/
│   │   └── db.py               # SQLite (siap di-upgrade PostgreSQL)
│   └── dashboard/
│       ├── server.py           # FastAPI + WebSocket + REST kontrol
│       └── static/index.html   # UI (vanilla JS, tanpa CDN)
├── tests/
│   ├── test_sizing.py          # verifikasi ukuran posisi & pembatasnya
│   ├── test_stops.py           # verifikasi SL/TP/breakeven/trailing
│   ├── test_stats.py           # verifikasi statistik performa
│   ├── test_detectors.py       # verifikasi logika deteksi
│   ├── test_vwap.py            # verifikasi filter Anchored VWAP
│   ├── test_config.py          # verifikasi validasi konfigurasi
│   ├── test_executor_race.py   # regresi race re-place OCO vs tutup manual
│   ├── test_executor_sl.py     # jalur stop-loss end-to-end (simulator)
│   └── test_paper_gateway.py   # akuntansi & OCO akun demo
├── data/                       # database SQLite (dibuat otomatis)
└── logs/                       # file log rotasi (dibuat otomatis)
```

## Persyaratan

- Python **3.10+** (diuji pada 3.13)
- Koneksi internet ke Binance
- Untuk mode `paper` (akun demo): **tidak perlu API key sama sekali**
- Untuk mode `live`: API key Binance dengan izin Spot Trading

## Instalasi

```bash
# 1. masuk folder project
cd pumpbot

# 2. (disarankan) virtual environment
python -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# 3. install dependency
pip install -r requirements.txt

# 4. (opsional) siapkan .env - hanya perlu diisi kalau nanti pakai mode live
cp .env.example .env
```

PumpBot hanya memakai **`config/config.yaml`**. File tersebut sudah tersedia
dan default-nya `mode: paper`. Ubah seluruh parameter strategi dan mode di
file itu; tidak ada profile konfigurasi lain maupun override mode dari CLI/.env.

### Isi `.env`

| Variable | Keterangan |
|---|---|
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | **hanya** wajib jika `mode` di `config/config.yaml` adalah `live`. Mode `paper` tidak memakainya sama sekali |
| `DASHBOARD_HOST` / `DASHBOARD_PORT` | opsional; menimpa host/port dashboard di YAML |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | opsional, notifikasi start/stop/halt harian |
| `LOG_LEVEL` | opsional; menimpa level logging YAML |

## Mode Paper: Akun Demo

Mode `paper` adalah **akun demo**: prinsipnya *data pasar nyata, uang palsu*.

| Bagian | Di mode `paper` |
|---|---|
| Harga, volume, order book, trade | **NYATA**, langsung dari Binance Spot |
| Watchlist & filter simbol | **NYATA**, dari `exchangeInfo` dan ticker 24 jam |
| Aturan LOT_SIZE / MIN_NOTIONAL | **NYATA**, diambil dari exchange |
| Saldo, order, posisi, PnL | **VIRTUAL**, hanya di komputer Anda |
| API key | **Tidak dipakai, tidak dibutuhkan** |

### Dari mana datanya

Bot memakai dua domain market data **publik resmi** Binance:

| Keperluan | Endpoint |
|---|---|
| REST (exchangeInfo, klines, ticker) | `https://data-api.binance.vision` |
| WebSocket (kline, aggTrade, depth, ticker) | `wss://data-stream.binance.vision` |

Menurut dokumentasi resmi Binance, kedua domain ini **tidak memerlukan
autentikasi apa pun dan hanya melayani data pasar publik** - user data stream
dan endpoint order tidak tersedia di sana.
Rujukan: [Binance Spot API - Market Data Only](https://developers.binance.com/docs/binance-spot-api-docs/faqs/market_data_only).

Konsekuensinya penting untuk keamanan: saat mode `paper`, bot terhubung ke
domain yang **secara teknis tidak bisa mengeksekusi order**. Ditambah tidak
adanya API key, tidak ada jalur apa pun yang membuat bot mengirim order
sungguhan.

### Kenapa akun demo, bukan testnet

Binance Testnet sudah **dihapus** dari project ini karena tiga alasan nyata:

1. **Harga testnet bukan harga pasar.** Buku order testnet sepi dan harganya
   menyimpang jauh dari pasar asli, sehingga sinyal pump yang terdeteksi di
   sana tidak pernah mencerminkan pump sungguhan. Untuk bot pemburu pump,
   ini membuat hasil ujinya tidak bisa dipercaya.
2. **Data testnet direset berkala**, sehingga saldo, order, dan histori
   hilang di tengah pengujian.
3. **Testnet tetap menuntut API key**, padahal akun demo tidak butuh.

Akun demo membaca pergerakan harga yang benar-benar terjadi, jadi statistik
win rate, profit factor, dan drawdown yang muncul di dashboard punya arti.

### Pengaturan akun demo

Semua di `config/config.yaml`:

```yaml
paper:
  start_balance: 10000.0   # modal virtual saat akun demo pertama dibuat
  slippage_bps: 2.0        # slippage taker per sisi (10 bps = 0,1%)
  reset_on_start: false    # true -> saldo dikosongkan ulang tiap bot start
```

Yang ditiru supaya hasil demo tidak lebih optimistis dari kenyataan:

- **Beli di best ask, jual di best bid** (bukan harga tengah).
- **Slippage** ditambahkan ke arah yang merugikan di kedua sisi.
- **Fee taker** dari `risk.fee_pct`: beli dipotong di aset base, jual dipotong
  di quote - sama seperti Binance Spot tanpa diskon BNB.
- **Locked balance**: aset dikunci saat OCO terpasang, dilepas saat dibatalkan.
- **OCO hanya terisi** bila harga pasar sungguhan menyentuh TP atau stop. SL
  terisi di harga stop-limit (lebih buruk dari trigger), dan bila dalam satu
  tick TP dan SL sama-sama tersentuh, **SL yang dimenangkan** - asumsi paling
  konservatif.

Saldo demo **bertahan lintas restart**: disimpan di `data/pumpbot-paper.db`,
terpisah total dari database mode live. Set `reset_on_start: true` bila ingin
mengulang eksperimen dari modal awal yang sama.

### Menyiapkan mode live (nanti)

Buat key di Binance → **API Management**, aktifkan **Enable Spot & Margin
Trading**, **JANGAN** aktifkan withdrawal, batasi ke IP tetap Anda, lalu isi
`.env`:

```
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
```

## Menjalankan Bot

```bash
# Jalankan konfigurasi tunggal (default mode: paper / akun demo)
python run.py
```

> ⏸ **Bot tidak auto-start trading.** Setelah `python run.py` dijalankan,
> bot berada dalam kondisi **PAUSE**: dashboard, data pasar, dan pengelolaan
> posisi lama tetap berjalan, tetapi bot **tidak membuka posisi baru** sampai
> Anda menekan tombol **▶ Resume Bot** di dashboard. Ini berlaku untuk mode
> `paper` maupun `live`.

Untuk mengganti mode, buka **`config/config.yaml`** lalu ubah satu baris ini:

```yaml
mode: paper    # pilihan: paper | live
```

- `paper` - **akun demo**: harga pasar Binance nyata, dana virtual, tanpa API
  key. Mulai dari sini dan bertahanlah di sini sampai hasilnya konsisten.
- `live` - **uang sungguhan**: butuh API key di `.env`.

Badge di pojok kiri dashboard menunjukkan mode aktif: **hijau `PAPER · AKUN
DEMO`** berarti aman, **merah `LIVE`** berarti setiap order memakai uang Anda.

Berhenti: `Ctrl+C` (shutdown rapi; posisi & OCO yang masih terbuka **tetap hidup
di exchange** dan dipulihkan otomatis saat bot dinyalakan lagi).

Dashboard: buka **http://localhost:8000** (atau `DASHBOARD_PORT` Anda).

## Dashboard Web

Semua panel update real-time via WebSocket (`/ws`), tanpa refresh manual:

| Panel | Isi |
|---|---|
| **Kartu ringkasan** | equity, saldo tersedia/terkunci, PnL harian, expectancy, dan total PnL tetap dalam quote asset + estimasi Rupiah dari market Binance |
| **Equity curve** | grafik perkembangan modal (snapshot tiap 30 detik) |
| **Posisi terbuka** | pair, entry, harga kini, qty, nilai, SL, TP, status BE & trailing, PnL% & nominal, tombol **Tutup** |
| **Histori transaksi** | waktu entry/exit, harga, PnL, alasan exit (SL/TP/trailing/manual/OCO gagal) |
| **Peringatan/Event** | log penting dashboard termasuk penyebab OCO gagal dan auto-close |
| **Panel sinyal** | semua koin dipantau + skor terbaru per detector, flag GATE/VETO |
| **Parameter risiko (live)** | ubah risk % balance, aktifkan mode multi-posisi (maks. 3 pair), pilih batas rugi harian opsional, threshold skor, serta on/off trailing & breakeven - **tanpa restart** |
| **Kontrol** | tombol **Pause/Resume Bot** (pause menghentikan entry baru; posisi tetap dikelola) |
| **Backtest & Optimasi** | grid search SL/TP/BE/trailing dari data historis Binance, tabel peringkat, dan tombol untuk menulis kombinasi terbaik ke `config.yaml` (lihat bab berikut) |

Endpoint REST: `GET /api/health`, `GET /api/trades`, `GET|POST /api/params`,
`POST /api/control/pause`, `POST /api/control/close`, plus enam endpoint
backtest yang dirinci di bab [Backtest & Optimasi dari Dashboard](#backtest--optimasi-dari-dashboard).

## Backtest & Optimasi dari Dashboard

Panel **Backtest & Optimasi** di dashboard menjalankan *grid search*: ia mencoba
banyak kombinasi parameter exit pada data harga historis yang sungguhan, lalu
mengurutkan hasilnya. Tidak perlu membuka terminal, dan tidak perlu mengunduh
data lebih dulu secara manual.

### Alur pemakaian

1. **Pause bot.** Tombol *Jalankan* baru aktif setelah bot di-pause. Ini
   disengaja: backtest memakai beberapa inti CPU, dan bot yang sedang memantau
   pasar tidak boleh kalah cepat gara-gara CPU terpakai habis.
2. **Isi form.** Simbol, jumlah hari, interval candle, porsi data
   *out-of-sample*, jumlah worker, lalu daftar nilai yang ingin dicoba untuk
   SL, TP (dalam R), breakeven, buffer breakeven, trailing, dan cooldown.
   Semua daftar dipisah koma, contoh `0.5, 0.75, 1.0, 1.5`.
3. **Perhatikan estimasi.** Di bawah form ada penghitung yang menampilkan
   jumlah kombinasi yang akan diuji, dihitung ulang setiap kali Anda mengetik.
   Kombinasi kembar otomatis dibuang (misalnya saat breakeven mati, nilai
   buffer tidak lagi berpengaruh sehingga tidak diuji berulang).
4. **Jalankan.** Progress bar menampilkan fase yang sedang berjalan:
   persiapan, unduh, muat data, pindai sinyal, grid exit, uji out-of-sample,
   selesai. Log mentah bisa dibuka lewat *Lihat log*. Tombol *Batalkan*
   menghentikan job kapan saja.
5. **Baca tabel peringkat.** Setiap baris berisi metrik in-sample dan
   out-of-sample: jumlah trade, win rate, return bersih, profit factor, max
   drawdown, rata-rata R, dan expectancy. Kombinasi dengan jumlah trade di
   bawah *Minimal trade* ditandai diskualifikasi supaya hasil kebetulan dari
   dua-tiga trade tidak naik ke puncak.
6. **Terapkan.** Tombol *Terapkan ke config* pada baris pilihan Anda menulis
   parameter tersebut ke `config/config.yaml`. Ada pratinjau dulu sebelum
   file benar-benar diubah.

### Sumber data

Candle diunduh otomatis dari `https://data-api.binance.vision`, domain publik
Binance khusus market data yang tidak memerlukan API key. Hasil unduhan
di-cache ke `data/backtest/` sehingga percobaan berikutnya pada rentang yang
sama langsung memakai cache. Centang *Unduh data* dimatikan bila Anda ingin
memakai cache saja.

### In-sample dan out-of-sample

Rentang data dibelah dua. Bagian awal (in-sample) dipakai mencari kombinasi
terbaik, bagian akhir (out-of-sample, default 30 persen) dipakai menguji
apakah kombinasi itu masih bekerja pada data yang belum pernah dilihat. Hanya
peringkat teratas yang diuji out-of-sample supaya waktu proses tidak meledak.
Kombinasi yang bagus di in-sample tetapi hancur di out-of-sample adalah tanda
*overfitting*: parameternya cuma menghafal masa lalu.

### Apa yang ditulis ke config

Hasil dikelompokkan menjadi empat grup yang bisa dicentang terpisah:

| Grup | Kunci yang ditulis | Default |
|---|---|---|
| **Exit** | `stops.mode`, `stops.percent_pct`, `take_profit.mode`, `take_profit.rr`, `breakeven.enabled`, `breakeven.trigger_rr`, `breakeven.buffer_pct`, `trailing.enabled`, `trailing.mode`, `trailing.percent_pct`, `trailing.update_step_pct` | aktif |
| **Lookback sinyal** | `signal.min_candles`, `signal.volume.ma_period`, `signal.volume.spike_scale`, `signal.price_action.structure_candles`, `signal.price_action.breakout_lookback`, `signal.price_action.swing_neighbors` | nonaktif |
| **Skoring** | `signal.score_threshold`, `signal.weights.price_action`, `signal.weights.volume` | nonaktif |
| **Cooldown** | `signal.cooldown_after_exit_min` | ikut hasil |

Grup **Skoring** sengaja dimatikan secara default dan diberi peringatan di UI.
Alasannya jujur saja: backtest hanya melihat candle, sehingga skornya cuma
memakai dua detector (price action dan volume). Bot yang berjalan memakai enam
detector termasuk orderbook, trade flow, dan whale yang datanya tidak ada di
data historis klines. Threshold yang optimal di backtest belum tentu optimal
di live. Terapkan grup ini hanya kalau Anda paham konsekuensinya.

Parameter yang **tidak** ikut dioptimasi dilaporkan di ringkasan hasil, antara
lain: stop berbasis struktur atau ATR, trailing mode ATR, multi-level TP dengan
jual parsial, serta pembulatan LOT_SIZE dan MIN_NOTIONAL.

### Keamanan penulisan config

- Setiap penerapan membuat **backup otomatis** di `config/backup/` dengan nama
  bertanggal, maksimal 10 file terakhir disimpan.
- Penulisan dilakukan **per baris**, bukan dengan memuat lalu menulis ulang
  YAML. Semua komentar, urutan kunci, dan format asli file tetap utuh. Hanya
  baris yang nilainya berubah yang disentuh.
- Ada **daftar putih** kunci. Permintaan menulis kunci di luar daftar itu
  ditolak, jadi endpoint ini tidak bisa dipakai mengubah `mode`, path
  database, host dashboard, atau parameter risiko.
- File ditulis lewat file sementara lalu di-*rename* (atomik), dan hasilnya
  divalidasi ulang dengan parser config. Kalau hasilnya tidak sah, file
  dikembalikan ke isi semula.
- Config hanya dibaca saat bot start, jadi **bot perlu direstart** agar
  parameter baru berlaku. Dashboard mengingatkan ini setelah apply berhasil.

### Batas pengaman

`MAX_SIGNAL_COMBOS` 64, `MAX_TOTAL_ROWS` 200.000, maksimal 60 simbol, maksimal
180 hari, maksimal 200 baris hasil, worker maksimal dua kali jumlah inti CPU.
Hanya satu job boleh berjalan pada satu waktu. Job berjalan sebagai proses
terpisah dan ikut dimatikan saat bot dihentikan, termasuk bila bot dimatikan
paksa di tengah job.

### Endpoint

| Method | Path | Fungsi |
|---|---|---|
| GET | `/api/backtest/defaults` | nilai awal form, diambil dari config aktif |
| POST | `/api/backtest/start` | mulai job (409 jika bot belum pause atau job lain jalan) |
| GET | `/api/backtest/state?since=N` | status, progress, hasil, dan log sejak baris ke-N |
| POST | `/api/backtest/cancel` | hentikan job yang sedang berjalan |
| POST | `/api/backtest/apply` | tulis satu baris hasil ke `config/config.yaml` |
| GET | `/api/backtest/csv` | unduh hasil lengkap sebagai CSV |

### Versi CLI

Untuk pemakaian lewat terminal, skrip lama tetap ada:
`python -m tools.backtest.download`, `python -m tools.backtest.optimize`, dan
`python -m tools.backtest.service`. Rinciannya di
[tools/backtest/README.md](tools/backtest/README.md).

## Cara Kerja Strategi

### 6 Detector (bobot & parameter via config)

| Detector | Apa yang dilihat | Skor tinggi ketika… |
|---|---|---|
| **Order book** | rasio bid/ask berbobot kedekatan level; wall | bid mendominasi ≥75%; ada bid wall (support) dekat harga; ask wall = penalti |
| **Trade flow** | jumlah trade/menit vs baseline 1 jam; tekanan taker beli vs jual; **gate spread** | lonjakan ≥4x baseline; agresor beli dominan; spread ≤ 15 bps (wajib) |
| **Volume** | volume candle terakhir & volume menit berjalan vs MA20 | spike ≥ 4x rata-rata |
| **Whale/bandar** | transaksi tunggal ≥ 8x rata-rata (dan ≥ $10rb); **deteksi spoofing** (wall muncul-lalu-hilang) | net notional beli whale besar; ask wall palsu menghilang (bullish) |
| **Manipulasi** *(penalti/veto)* | naik >20%/24 jam; pola pump-and-dump (naik ≥8% lalu pullback ≥40%); overextended 5 menit; wash trading (volume tinggi, harga diam) | skor manipulasi ≥70 → **VETO**, bot tidak akan entry |
| **Price action** *(bobot terbesar)* | struktur HH/HL; **breakout resistance 20 candle + konfirmasi volume**; retracement fibonacci 38.2-61.8% dari kaki impulsif | breakout terkonfirmasi volume, di golden zone, struktur uptrend |

**Skor akhir** = rata-rata tertimbang detector positif × (1 − bobot_manipulasi ×
skor_manipulasi). Entry bila skor ≥ `score_threshold` (default 70) DAN lolos
gate spread DAN tidak di-veto DAN simbol tidak sedang cooldown DAN (bila aktif)
lolos filter Anchored VWAP di bawah.

### Filter Anchored VWAP (gate entry, bukan skor)

VWAP berlabuh dihitung dari satu candle ANCHOR sampai candle closed terakhir:

    VWAP = Σ nilai_transaksi / Σ volume

Nilai per candle memakai `quote_volume` bila > 0 (elemen indeks 7 pada respons
kline Binance Spot = Quote asset volume), selain itu jatuh ke typical price
`((high + low + close) / 3) × volume`. Hanya candle CLOSED dengan volume > 0
yang dijumlahkan; duplikat dan `open_time` yang tidak naik (akibat reconnect
WebSocket) dibuang. Karena buffer hanya berisi candle closed, tidak ada
look-ahead.

Keputusan: `dist_pct = (last_price − vwap) / vwap × 100`, lolos bila
`min_above_pct ≤ dist_pct ≤ max_above_pct` (inklusif). Harga di bawah VWAP
berarti penjual menguasai pasar sejak pump dimulai; harga terlalu jauh di atas
berarti kita mengejar puncak. Keduanya ditolak.

| Parameter (`signal.vwap`) | Default | Arti |
|---|---|---|
| `enabled` | `true` di YAML, `false` di dataclass | matikan untuk perilaku persis seperti sebelum filter ada |
| `anchor_mode` | `pump_start` | `pump_start` / `impulse_low` / `manual` |
| `anchor_lookback_candles` | 60 | window pencarian anchor (60 candle = 60 menit pada 1m) |
| `pump_volume_mult` | 3.0 | candle "berlonjak" bila volume ≥ 3× rata-rata baseline dan bullish |
| `pump_baseline_candles` | 20 | panjang baseline rata-rata volume sebelum candle kandidat |
| `pump_max_gap_candles` | 3 | dua lonjakan masuk satu rantai bila jaraknya ≤ 3 candle |
| `no_anchor_action` | `impulse_low` | bila tidak ada lonjakan: pakai low terendah, atau `block` |
| `manual_anchors` | `{}` | `{SIMBOL: epoch_ms UTC}`; simbol tak terdaftar / di luar window ikut `no_anchor_action` |
| `min_above_pct` | 0.0 | batas bawah zona entry (persen terhadap VWAP) |
| `max_above_pct` | 8.0 | batas atas zona entry, **nilai awal, wajib dikalibrasi lewat backtest** |
| `min_anchor_candles` | 3 | minimal candle sejak anchor sebelum VWAP dipercaya |
| `on_insufficient_data` | `block` | data kurang / volume 0 / harga tidak valid: `block` atau `allow` |

Semua parameter berjumlah CANDLE, jadi pada `data.kline_interval: 1m` satu
candle sama dengan satu menit.

**Anchor `pump_start`:** dalam window, cari candle berlonjak TERBARU lalu
telusuri mundur selama jarak antar lonjakan ≤ `pump_max_gap_candles`; anchor =
candle paling awal dalam rantai itu. Lonjakan lama yang terpisah diabaikan.

**Anchor `impulse_low` vs fibonacci PriceActionDetector:** keduanya memakai
konsep kaki impulsif (low terendah lalu diukur ke atas), tetapi windownya
BERBEDA. PriceActionDetector memakai `price_action.structure_candles` (30),
sedangkan filter VWAP memakai `vwap.anchor_lookback_candles` (60) sendiri.
Jadi anchor VWAP bisa berada lebih jauh ke belakang daripada kaki impulsif
yang dipakai skor fibonacci; ini disengaja supaya satu pump penuh tercakup.

### Kenapa menunggu retracement / breakout, bukan mengejar kenaikan?

Detector manipulasi memberi penalti berat pada koin yang sudah naik terlalu
tajam dalam 5 menit (overextended) atau sedang dalam pola pump-and-dump - bot sengaja **tidak membeli di puncak pump palsu**. Sinyal terbaik justru
breakout resistance yang dikonfirmasi volume, atau entry di golden zone
fibonacci setelah kaki impulsif sehat.

## Manajemen Risiko (Revisi)

### 1. Risk hanya dalam persen balance

Ukuran posisi ditentukan oleh satu input: `risk.risk_per_trade_pct`. Dasarnya
adalah **balance pada harga modal**, bukan equity mark-to-market; profit/loss
mengambang pada posisi lain tidak mengubah risk % entry baru.

```text
risk_amount = balance × risk_per_trade_pct
qty         = risk_amount / (entry − stop)          # spot long
```

Contoh balance 10.000 USDT, risk 1%, entry 1,00 dan SL 0,98: target rugi adalah
100 USDT, maka qty = 100 / 0,02 = **5.000 unit**. Jika SL tersentuh, rugi
sebelum fee/slippage sekitar 1% balance. Pada Binance Spot, order tetap tidak
bisa melebihi quote balance yang tersedia (dengan buffer fee 0,5%); bila saldo
fisik tidak cukup, qty - dan risiko efektif - hanya dapat menjadi lebih kecil,
tidak pernah lebih besar.

Tidak ada batas atas software untuk `risk_per_trade_pct`: nilainya fleksibel dan
bisa diatur ke angka positif berapa pun dari YAML atau dashboard. Batas yang
masih berlaku hanyalah batas fisik/teknis, yaitu saldo quote tersedia,
pembulatan `LOT_SIZE`, pengecekan `MIN_NOTIONAL`, dan batas exchange lain.

### 2. Batas rugi harian opsional

`risk.daily_loss_enabled` default-nya `false`. Jika diaktifkan, equity (termasuk
unrealized PnL) dibandingkan dengan equity awal hari. Ketika turun sebesar
`risk.daily_loss_limit_pct`, bot **hanya menghentikan entry baru**; SL, TP, BE,
dan trailing posisi yang sudah terbuka tetap dikelola. Baseline dan resetnya
mengikuti **00:00 UTC**.

### 3. Satu posisi default, multi-posisi sebagai opsi

Default `risk.allow_multiple_positions: false`, sehingga bot menunggu posisi
yang ada selesai sebelum entry berikutnya. Jika diaktifkan, `max_open_positions`
dapat diatur dari 1 sampai **3**. Setiap pair tetap hanya boleh memiliki satu
posisi aktif - tidak ada pyramiding pada simbol yang sama.

### 4. SL/TP 1:2, BE, lalu trailing

Konfigurasi default memakai SL persen dan satu TP berbasis risk:reward:

```yaml
stops:
  mode: percent
  percent_pct: 1.0

take_profit:
  mode: rr
  rr: 2.0
```

Jadi SL 1% menghasilkan TP 2% (1:2). **Sebelum TP**, saat harga mencapai
**+1R** (jarak dari entry ke SL awal), bot memindahkan SL ke BE. Harga BE
memakai buffer minimum 2× estimasi fee agar benar-benar tidak rugi karena biaya.
Segera sesudah BE aktif, trailing mulai bekerja dari harga tertinggi, dengan
jarak default **0,5%**. SL hanya bergerak naik, tidak pernah diturunkan.

Urutan ringkas untuk entry 100, SL 99 dan TP 102:

1. Harga mencapai 101 (+1R) → SL pindah ke sekitar BE dan trailing diaktifkan.
2. Harga mencetak high baru → SL mengikuti `highest × (1 − 0,5%)`.
3. Harga mencapai 102 → TP terisi; jika harga berbalik lebih dahulu, SL BE atau
   trailing yang menutup posisi.

> Tetap uji dalam mode `paper` sebelum memakai dana riil. Gap,
> slippage, dan kegagalan eksekusi stop-limit dapat membuat hasil aktual
> berbeda dari risk teoritis.

## Testing

```bash
python -m pytest tests/ -v
```

467 unit test lulus pada perintah baseline (`python -m pytest -q
--ignore=tests/test_backtest_parity.py --ignore=tests/test_engine_max_hold.py
--ignore=tests/test_grid_cache.py`), mencakup: position sizing (risiko tidak pernah melebihi target),
pembulatan LOT_SIZE/MIN_NOTIONAL, SL awal struktur/persen, level TP,
trigger & harga breakeven, trailing monoton, ATR, statistik (win rate, profit
factor, max drawdown), seluruh detector (skor/gate/veto), validasi
konfigurasi, filter Anchored VWAP (rumus, anchor, gate, cache, integrasi
engine dan backtest), plus dua test end-to-end executor:
race-condition "re-place OCO vs tutup posisi manual" (anti OCO yatim/penjualan
ganda) dan jalur exit stop-loss beserta konsistensi akuntansi dana.

Fitur backtest dashboard ditutup 165 test tambahan:
`tests/test_backtest_service.py` (68) untuk pembentukan grid, dedup kombinasi,
pembatas keamanan, peringkat, diskualifikasi, dan format NDJSON;
`tests/test_config_writer.py` (50) untuk penulisan YAML per baris, keutuhan
komentar, daftar putih kunci, rotasi backup, penulisan atomik, dan rollback
saat hasil tidak sah; `tests/test_dashboard_backtest.py` (53) untuk keenam
endpoint, guard wajib pause, mesin status job, pemangkasan log, dan jaring
pengaman agar proses anak tidak jadi yatim.

Khusus akun demo, `tests/test_paper_gateway.py` (39 test, seluruhnya offline)
memverifikasi: tidak ada API key yang pernah dikirim, URL yang dipakai adalah
domain market-data-only Binance, mode `testnet` benar-benar ditolak, akuntansi
beli/jual/fee/slippage, kekekalan dana (beli lalu jual di harga sama tanpa fee
mengembalikan modal utuh persis), locked balance OCO, pengisian OCO hanya saat
harga pasar menyentuhnya, prioritas SL atas TP dalam satu tick, serta saldo
demo yang bertahan lintas restart termasuk saat state tersimpan rusak.

## Pemulihan Setelah Restart

Bot mencatat semua posisi ke SQLite. Saat dinyalakan ulang:
1. Filter exchange diambil lebih awal, lalu posisi `OPEN` dimuat dari database.
2. Order terbuka lama di simbol tersebut dibatalkan, OCO dipasang ulang dengan
   SL/TP terkini.
3. Jika OCO restore gagal, penyebabnya dicatat di dashboard/log dan posisi
   ditutup market otomatis agar tidak berjalan tanpa proteksi exchange.
4. Trading lanjut seperti biasa untuk posisi yang berhasil dipulihkan.

## Dust Sweep (Konversi Sisa Koin Kecil ke BNB)

Sisa qty pembulatan LOT_SIZE setelah semua TP terjual (nilainya di bawah
`MIN_NOTIONAL`, tidak bisa dijual biasa) dapat dikonversi otomatis menjadi
BNB lewat endpoint resmi Binance *Convert Dust to BNB* (SDK resmi
`binance-sdk-wallet`, endpoint SAPI `/sapi/v1/asset/dust`).

```yaml
dust_sweep:
  enabled: true            # aktifkan sweep berkala
  interval_minutes: 360    # tiap 6 jam (minimum 30 menit)
  min_value_usd: 1.0       # aset bernilai < 1 USDT dianggap dust
```

Catatan:
- **Hanya berjalan di mode LIVE** - endpoint SAPI butuh API key bertanda
  tangan dan akun demo tidak memegang BNB sungguhan (bot mencatat warning
  lalu melewati fitur ini).
- Aset quote (USDT), BNB, dan aset bernilai >= `min_value_usd` tidak ikut.
- Maksimal 10 aset per request (batas API); hasil (jumlah BNB diterima)
  dicatat di log dan tabel `events` (type `DUST_SWEEP`).
- BNB hasil konversi tidak dipakai bot (trading tetap pakai quote USDT);
  BNB hanya terkumpul di wallet.

## Upgrade ke PostgreSQL

Semua query di `bot/database/db.py` memakai SQL standar berparameter. Untuk
migrasi:
1. Ganti `sqlite3.connect()` → `psycopg.connect()`.
2. Ubah placeholder `?` → `%s`.
3. Sesuaikan DDL: `INTEGER PRIMARY KEY AUTOINCREMENT` →
   `BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`.
4. Isolasi perubahan cukup di file `db.py` saja (modul lain memakai method
   `Database`, bukan SQL langsung).

## Troubleshooting / FAQ

**Order ditolak: `LOT_SIZE` / `PRICE_FILTER` / `NOTIONAL`**
Bot sudah membulatkan otomatis dari exchangeInfo. Jika masih terjadi biasanya
saldo tidak cukup untuk `MIN_NOTIONAL` setelah fee - cek kartu saldo di
dashboard. Log penolakan entry ada di tabel `events` (kolom `type` =
`ENTRY_REJECTED`) beserta alasannya.

**OCO gagal dipasang**
Bot mengklasifikasikan penyebab OCO gagal di log dan dashboard dengan kode
berbeda, misalnya `OCO_FAIL_PRICE_FILTER`, `OCO_FAIL_LOT_SIZE`,
`OCO_FAIL_MIN_NOTIONAL`, `OCO_FAIL_INSUFFICIENT_BALANCE`,
`OCO_FAIL_IMMEDIATE_TRIGGER`, `OCO_FAIL_RATE_LIMIT`, atau `OCO_FAIL_NETWORK`.
Jika OCO gagal, posisi langsung ditutup market otomatis demi keamanan agar
tidak berjalan tanpa proteksi order di exchange. Event penutupan tercatat
sebagai `OCO_FAILED_POSITION_CLOSED`.

**WebSocket sering putus**
SDK resmi otomatis reconnect (10 percobaan, jeda 5 detik) + watchdog kami
me-langgan ulang bila benar-benar mati. Cek `logs/bot.log`.

**Rate limit (HTTP 429 / ban)**
REST wrapper menunggu `Retry-After` secara otomatis. Kurangi `max_symbols`
atau naikkan `reevaluate_sec` bila sering terjadi.

**`Konfigurasi TIDAK VALID: ...`**
Validasi mencegah parameter berbahaya (risk ≤ 0, SL/TP tak valid, dll).
Perbaiki sesuai pesan error - daftar lengkapnya dalam pesan tersebut.

**Ingin mulai statistik dari nol**
Hentikan bot lalu hapus file DB mode yang bersangkutan - tiap mode punya file
sendiri: `data/pumpbot-paper.db` dan `data/pumpbot-live.db` (path default
`data/pumpbot.db` di config otomatis diarahkan ke sana). Untuk mode paper ada
jalan yang lebih ringan: set `paper.reset_on_start: true` agar saldo virtual
kembali ke `start_balance` tanpa menghapus histori trade.

Histori lama di file `data/pumpbot.db` peninggalan versi sebelumnya bisa
dipakai ulang sebagai akun demo dengan `mv data/pumpbot.db data/pumpbot-paper.db`.

**Pindah dari mode testnet versi lama**
Mode `testnet` sudah tidak ada. Kalau `config/config.yaml` lama Anda masih
berisi `mode: testnet`, bot akan menolak start dengan pesan
`mode harus 'paper' (akun demo) | 'live' (uang sungguhan)`. Ubah baris itu
menjadi `mode: paper`. Database `data/pumpbot-testnet.db` lama boleh dihapus
atau di-rename menjadi `data/pumpbot-paper.db` bila ingin dipertahankan - tapi ingat statistiknya berasal dari harga testnet yang tidak realistis.

---

**Terakhir**: ujilah di akun demo minimal beberapa minggu, pahami setiap
parameter sebelum mengubahnya, dan jangan pernah masuk mode `live` dengan
parameter yang belum Anda pahami sepenuhnya. Selamat menguji! 🚀
