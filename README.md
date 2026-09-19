# 🤖 PumpBot — Binance Spot Pump Detector & Auto Trading Bot

Bot trading otomatis untuk **Binance Spot** yang:

1. **Mencari koin berpotensi pump** lewat 6 detector paralel (order book, frekuensi
   trade, volume, whale/bandar, manipulasi, price action + fibonacci).
2. **Masuk posisi dengan manajemen risiko ketat** — ukuran posisi dihitung dari
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

> ## ⚠️ DISCLAIMER — BACA DULU
> **Bot ini menyangkut uang sungguhan.**
> - Wajib diuji di **testnet** (atau mode `paper`) sampai stabil sebelum live.
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
5. [Membuat API Key Binance Testnet](#membuat-api-key-binance-testnet)
6. [Menjalankan Bot](#menjalankan-bot)
7. [Dashboard Web](#dashboard-web)
8. [Cara Kerja Strategi](#cara-kerja-strategi)
9. [Manajemen Risiko (Revisi)](#manajemen-risiko-revisi)
10. [Testing](#testing)
11. [Dust Sweep](#dust-sweep-konversi-sisa-koin-kecil-ke-bnb)
12. [Pemulihan Setelah Restart](#pemulihan-setelah-restart)
13. [Upgrade ke PostgreSQL](#upgrade-ke-postgresql)
14. [Troubleshooting / FAQ](#troubleshooting--faq)

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
        │  SIGNAL ENGINE — 6 detector, skor gabungan 0-100        │
        │  orderbook • trade_flow • volume • whale • manipulasi   │
        │  (penalti/veto) • price_action (breakout+fib)           │
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
│   ├── data_collector/
│   │   ├── buffers.py          # rolling buffer per simbol
│   │   └── collector.py        # watchlist + langganan stream + seed REST
│   ├── signal_engine/
│   │   ├── detectors.py        # 6 detector pump
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
│   ├── test_config.py          # verifikasi validasi konfigurasi
│   ├── test_executor_race.py   # regresi race re-place OCO vs tutup manual
│   └── test_executor_sl.py     # jalur stop-loss end-to-end (paper)
├── data/                       # database SQLite (dibuat otomatis)
└── logs/                       # file log rotasi (dibuat otomatis)
```

## Persyaratan

- Python **3.10+** (diuji pada 3.13)
- pip + akses internet ke Binance (testnet/live)
- Untuk mode `paper`: tidak perlu apa pun selain Python

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

# 4. siapkan API key bila nanti memakai mode testnet/live
cp .env.example .env
```

PumpBot hanya memakai **`config/config.yaml`**. File tersebut sudah tersedia
dan default-nya `mode: paper`. Ubah seluruh parameter strategi dan mode di
file itu; tidak ada profile konfigurasi lain maupun override mode dari CLI/.env.

### Isi `.env`

| Variable | Keterangan |
|---|---|
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | wajib jika `mode` di `config/config.yaml` adalah `testnet` atau `live` |
| `DASHBOARD_HOST` / `DASHBOARD_PORT` | opsional; menimpa host/port dashboard di YAML |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | opsional, notifikasi start/stop/halt harian |
| `LOG_LEVEL` | opsional; menimpa level logging YAML |

## Membuat API Key Binance Testnet

1. Buka **https://testnet.binance.vision**
2. Login dengan akun **GitHub** (testnet gratis, saldo virtual otomatis diberikan).
3. Klik tab **API Key** → **Generate HMAC Key**.
4. Salin **API Key** dan **Secret Key** ke file `.env`:
   ```
   BINANCE_API_KEY=xxxxxxxxxxxxxxxx
   BINANCE_API_SECRET=xxxxxxxxxxxxxxxx
   ```
5. Testnet Binance reset data berkala (beberapa hari) — saldo & order bisa hilang,
   wajar.

> Untuk live: buat key di Binance → API Management, aktifkan **Enable Spot &
> Margin Trading**, **JANGAN** aktifkan withdrawal, dan batasi IP jika memungkinkan.

## Menjalankan Bot

```bash
# Jalankan konfigurasi tunggal (default mode: paper)
python run.py
```

Untuk mengganti mode, buka **`config/config.yaml`** lalu ubah satu baris ini:

```yaml
mode: paper    # pilihan: paper | testnet | live
```

- `paper`: simulasi internal, tanpa API key — mulai dari sini.
- `testnet`: memerlukan API key testnet pada `.env`.
- `live`: uang sungguhan; gunakan hanya sesudah testnet stabil.

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
| **Parameter risiko (live)** | ubah risk % balance, aktifkan mode multi-posisi (maks. 3 pair), pilih batas rugi harian opsional, threshold skor, serta on/off trailing & breakeven — **tanpa restart** |
| **Kontrol** | tombol **Pause/Resume Bot** (pause menghentikan entry baru; posisi tetap dikelola) |

Endpoint REST: `GET /api/health`, `GET /api/trades`, `GET|POST /api/params`,
`POST /api/control/pause`, `POST /api/control/close`.

## Cara Kerja Strategi

### 6 Detector (bobot & parameter via config)

| Detector | Apa yang dilihat | Skor tinggi ketika… |
|---|---|---|
| **Order book** | rasio bid/ask berbobot kedekatan level; wall | bid mendominasi ≥75%; ada bid wall (support) dekat harga; ask wall = penalti |
| **Trade flow** | jumlah trade/menit vs baseline 1 jam; tekanan taker beli vs jual; **gate spread** | lonjakan ≥4x baseline; agresor beli dominan; spread ≤ 15 bps (wajib) |
| **Volume** | volume candle terakhir & volume menit berjalan vs MA20 | spike ≥ 4x rata-rata |
| **Whale/bandar** | transaksi tunggal ≥ 8x rata-rata (dan ≥ $10rb); **deteksi spoofing** (wall muncul-lalu-hilang) | net notional beli whale besar; ask wall palsu menghilang (bullish) |
| **Manipulasi** *(penalti/veto)* | naik >20%/24 jam; pola pump-and-dump (naik ≥8% lalu pullback ≥40%); overextended 5 menit; wash trading (volume tinggi, harga diam) | skor manipulasi ≥70 → **VETO**, bot tidak akan entry |
| **Price action** *(bobot terbesar)* | struktur HH/HL; **breakout resistance 20 candle + konfirmasi volume**; retracement fibonacci 38.2–61.8% dari kaki impulsif | breakout terkonfirmasi volume, di golden zone, struktur uptrend |

**Skor akhir** = rata-rata tertimbang detector positif × (1 − bobot_manipulasi ×
skor_manipulasi). Entry bila skor ≥ `score_threshold` (default 70) DAN lolos
gate spread DAN tidak di-veto DAN simbol tidak sedang cooldown.

### Kenapa menunggu retracement / breakout, bukan mengejar kenaikan?

Detector manipulasi memberi penalti berat pada koin yang sudah naik terlalu
tajam dalam 5 menit (overextended) atau sedang dalam pola pump-and-dump —
bot sengaja **tidak membeli di puncak pump palsu**. Sinyal terbaik justru
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
fisik tidak cukup, qty — dan risiko efektif — hanya dapat menjadi lebih kecil,
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
posisi aktif — tidak ada pyramiding pada simbol yang sama.

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

> Tetap uji dalam mode `paper` atau testnet sebelum memakai dana riil. Gap,
> slippage, dan kegagalan eksekusi stop-limit dapat membuat hasil aktual
> berbeda dari risk teoritis.

## Testing

```bash
python -m pytest tests/ -v
```

113 unit test mencakup: position sizing (risiko tidak pernah melebihi target),
pembulatan LOT_SIZE/MIN_NOTIONAL, SL awal struktur/persen, level TP,
trigger & harga breakeven, trailing monoton, ATR, statistik (win rate, profit
factor, max drawdown), seluruh detector (skor/gate/veto), validasi
konfigurasi, plus dua test end-to-end executor:
race-condition "re-place OCO vs tutup posisi manual" (anti OCO yatim/penjualan
ganda) dan jalur exit stop-loss beserta konsistensi akuntansi dana.

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
- **Hanya berjalan di mode LIVE** — endpoint SAPI tidak tersedia di testnet
  dan simulator paper tidak punya BNB (bot mencatat warning dan melewati).
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
saldo tidak cukup untuk `MIN_NOTIONAL` setelah fee — cek kartu saldo di
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
Perbaiki sesuai pesan error — daftar lengkapnya dalam pesan tersebut.

**Ingin mulai statistik dari nol**
Hentikan bot lalu hapus file DB mode yang bersangkutan — sejak pemisahan
otomatis, tiap mode punya file sendiri: `data/pumpbot-testnet.db`,
`data/pumpbot-live.db` (path default
`data/pumpbot.db` di config otomatis diarahkan ke sana; histori lama di
file `data/pumpbot.db` peninggalan versi sebelumnya bisa dipertahankan
untuk testnet dengan `mv data/pumpbot.db data/pumpbot-testnet.db`).

---

**Terakhir**: ujilah di testnet minimal beberapa minggu, pahami setiap
parameter sebelum mengubahnya, dan jangan pernah masuk mode `live` dengan
parameter yang belum Anda pahami sepenuhnya. Selamat menguji! 🚀
