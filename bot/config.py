"""
Loader & validasi konfigurasi.

Prioritas nilai:
  1. File YAML tunggal (config/config.yaml)
  2. Environment variable non-strategi (DASHBOARD_HOST/PORT, LOG_LEVEL)
  3. Default di dataclass di bawah ini

Mode bot hanya dibaca dari field ``mode`` dalam config/config.yaml; tidak ada
profile YAML atau override mode environment.

Semua parameter strategi tervalidasi SEBELUM bot berjalan
(risk % tidak boleh <= 0, dsb.) supaya salah ketik tidak berujung
pada ukuran posisi yang berbahaya.
"""

from __future__ import annotations

import math
import os
import warnings
from dataclasses import dataclass, field
from typing import Optional
from bot.risk_management.atr import (MAX_PERIOD as ATR_MAX_PERIOD,
                                     METHODS as ATR_METHODS,
                                     MIN_PERIOD as ATR_MIN_PERIOD)

import yaml


class ConfigError(Exception):
    """Dilempar saat konfigurasi tidak valid."""


# ---------------------------------------------------------------------------
# Skema konfigurasi (default konservatif)
# ---------------------------------------------------------------------------

@dataclass
class UniverseCfg:
    max_symbols: int = 24
    min_quote_volume_24h: float = 5_000_000
    include_symbols: list = field(default_factory=list)
    exclude_symbols: list = field(default_factory=list)
    exclude_stable_pairs: bool = True
    exclude_leveraged_tokens: bool = True
    refresh_minutes: int = 60


@dataclass
class DataCfg:
    kline_interval: str = "1m"
    history_candles: int = 120
    depth_levels: int = 20
    depth_speed_ms: int = 1000
    trade_buffer_max: int = 5000
    trade_window_sec: int = 600
    book_history_sec: int = 60


@dataclass
class ManipulationCfg:
    weight: float = 0.60
    veto_threshold: float = 0.70
    # Ambang kenaikan 24 jam (persen, dari ticker 24 jam Binance).
    # Kenaikan di bawah ambang ini hanya mengurangi skor secara proporsional,
    # sedangkan kenaikan yang MENYENTUH ambang langsung mendapat komponen
    # penalti penuh (c1 = 100). Dipilih 10% karena koin yang sudah naik
    # segitu dalam sehari berisiko dump, sementara koin di kisaran 6-10%
    # masih punya ruang naik dan tetap bisa lolos bila skor dasarnya kuat.
    max_change_24h_pct: float = 10.0
    pump_dump_gain_pct: float = 8.0
    pump_dump_pullback_pct: float = 40.0
    pump_dump_window_min: int = 15
    overextended_5m_pct: float = 5.0
    wash_volume_spike: float = 2.0
    wash_range_pct: float = 0.4


@dataclass
class Change24hCfg:
    """
    Band perubahan 24 jam sebagai GERBANG ENTRY (bukan penalti skor).

    Hanya koin yang BESAR perubahan 24 jam-nya berada di dalam [min_pct,
    max_pct] yang boleh masuk. Nilai di bawah min_pct berarti koin terlalu
    tenang untuk dikejar, sedangkan di atas max_pct berarti pergerakannya
    terlalu ekstrem: kalau naik berisiko beli di puncak, kalau turun itu pisau
    jatuh. Jadi koin yang turun 6 sampai 10% ikut lolos, koin yang turun lebih
    dalam dari max_pct ditolak.

    Angka dibaca dari ticker 24 jam Binance (priceChangePercent) dan diuji
    dengan NILAI ABSOLUT, sama seperti ManipulationDetector. Batas kedua ujung
    inklusif.
    """
    enabled: bool = True
    min_pct: float = 6.0
    max_pct: float = 10.0


@dataclass
class OrderBookCfg:
    imbalance_levels: int = 10
    imbalance_scale: float = 0.25
    wall_ratio: float = 10.0
    wall_near_pct: float = 1.0


@dataclass
class TradeFlowCfg:
    spike_scale: float = 3.0
    baseline_minutes: int = 60
    max_spread_bps: float = 15.0


@dataclass
class VolumeCfg:
    spike_scale: float = 3.0
    ma_period: int = 20


@dataclass
class WhaleCfg:
    multiplier: float = 8.0
    min_notional_usd: float = 10_000
    net_normalize_usd: float = 50_000
    window_min: int = 3
    spoof_wall_ratio: float = 8.0
    spoof_drop_pct: float = 0.8


@dataclass
class PriceActionCfg:
    structure_candles: int = 30
    swing_neighbors: int = 2
    breakout_lookback: int = 20
    breakout_vol_confirm: float = 1.5
    fib_zone: list = field(default_factory=lambda: [0.382, 0.618])
    near_top_pct: float = 0.9


@dataclass
class VWAPCfg:
    """
    Filter entry Anchored VWAP (bukan sumber skor).

    Semua parameter berjumlah CANDLE, jadi pada kline_interval 1m satu candle
    sama dengan satu menit. Default dataclass sengaja enabled=False supaya
    perilaku tanpa blok YAML identik dengan versi sebelum filter ada.
    """
    enabled: bool = False
    anchor_mode: str = "pump_start"       # pump_start | impulse_low | manual
    anchor_lookback_candles: int = 60
    pump_volume_mult: float = 3.0
    pump_baseline_candles: int = 20
    pump_max_gap_candles: int = 3
    no_anchor_action: str = "impulse_low"  # impulse_low | block
    manual_anchors: dict = field(default_factory=dict)
    min_above_pct: float = 0.0
    max_above_pct: float = 8.0
    min_anchor_candles: int = 3
    on_insufficient_data: str = "block"    # block | allow


@dataclass
class SignalCfg:
    score_threshold: float = 70.0
    cooldown_after_exit_min: int = 15
    reevaluate_sec: float = 3.0
    min_candles: int = 30
    weights: dict = field(default_factory=lambda: {
        "orderbook": 0.20, "trade_flow": 0.15, "volume": 0.20,
        "whale": 0.15, "price_action": 0.30,
    })
    manipulation: ManipulationCfg = field(default_factory=ManipulationCfg)
    orderbook: OrderBookCfg = field(default_factory=OrderBookCfg)
    trade_flow: TradeFlowCfg = field(default_factory=TradeFlowCfg)
    volume: VolumeCfg = field(default_factory=VolumeCfg)
    whale: WhaleCfg = field(default_factory=WhaleCfg)
    price_action: PriceActionCfg = field(default_factory=PriceActionCfg)
    vwap: VWAPCfg = field(default_factory=VWAPCfg)
    change_24h: Change24hCfg = field(default_factory=Change24hCfg)


@dataclass
class RiskCfg:
    # Satu input risiko per entry: % dari balance (bukan equity/PnL berjalan).
    # Tidak ada batas atas software; tetap harus angka positif dan finite.
    risk_per_trade_pct: float = 1.0
    # Default aman: hanya satu posisi. Multi-posisi perlu opt-in eksplisit.
    allow_multiple_positions: bool = False
    max_open_positions: int = 3
    # Fitur opsional; saat false, limit tidak memblokir entry.
    daily_loss_enabled: bool = False
    daily_loss_limit_pct: float = 3.0
    fee_pct: float = 0.1


@dataclass
class AtrCfg:
    """
    Parameter Average True Range (ATR) yang dipakai bersama SL, TP, BE, dan
    trailing.

    ATR dihitung dari candle TERTUTUP pada interval `data.kline_interval`,
    memakai rumus Wilder (lihat bot/risk_management/atr.py). Satu nilai ATR
    diambil saat entry dan DIBEKUKAN untuk seluruh umur posisi, jadi SL/TP/BE
    sebuah posisi tidak berubah hanya karena volatilitas bergerak.

    `period` juga menentukan jumlah candle minimum: ATR butuh period + 1
    candle tertutup. Dengan period 14 dan data.history_candles 120, syarat
    itu selalu terpenuhi setelah seed REST.
    """
    period: int = 14
    method: str = "wilder"          # wilder (RMA) | sma


@dataclass
class StopsCfg:
    mode: str = "atr"               # structure | percent | atr
    percent_pct: float = 1.0
    # Jarak SL saat mode 'atr': entry - (atr_multiplier x ATR). Nilai ini
    # juga menjadi pembanding saat mode 'atr' jatuh ke fallback percent.
    atr_multiplier: float = 1.5
    # Pengaman khusus mode atr, dalam satuan ATR (bukan persen). Kelipatan
    # efektif dijepit ke rentang ini. Sengaja tidak memakai min_stop_pct:
    # pada koin bervolatilitas rendah, batas persen 0,5% menimpa jarak ATR
    # yang sah (mis. 0,077% pada BTC) sehingga TP berbasis ATR bisa berakhir
    # lebih dekat daripada SL.
    atr_min_multiplier: float = 0.5
    atr_max_multiplier: float = 4.0
    structure_buffer_pct: float = 0.15
    # Batas atas mutlak: tetap berlaku di mode atr sebagai pengaman risiko.
    max_stop_pct: float = 4.0
    # min_stop_pct hanya berlaku untuk mode percent/structure.
    min_stop_pct: float = 0.5


@dataclass
class TPTarget:
    gain_pct: float = 1.2
    sell_pct: float = 50.0


@dataclass
class TakeProfitCfg:
    # Default satu TP 2R: SL 1% menghasilkan TP 2%.
    mode: str = "atr"              # rr | multi | single | atr
    rr: float = 2.0
    # Jarak TP saat mode 'atr': entry + (atr_multiplier x ATR). Dengan SL
    # 1.5 x ATR dan TP 3 x ATR, imbal rasio 1:2 terjaga berapa pun ATR-nya.
    atr_multiplier: float = 3.0
    targets: list = field(default_factory=lambda: [TPTarget(2.0, 100)])


@dataclass
class BreakevenCfg:
    enabled: bool = True
    # auto -> ikut basis SL (stops.mode): SL ATR maka BE ikut ATR.
    # rr   -> pemicu entry + trigger_rr x jarak SL awal (1R).
    # atr  -> pemicu entry + trigger_atr_mult x ATR.
    trigger_mode: str = "auto"
    # BE dipicu pada 1R: entry + trigger_rr x (entry - initial_stop).
    trigger_rr: float = 1.0
    # Pemicu BE saat trigger_mode atr. 1.5 x ATR sama dengan 1R bila SL
    # dipasang 1.5 x ATR, sehingga BE tetap "bebas risiko di 1R".
    trigger_atr_mult: float = 1.5
    # Buffer minimal 2x fee tetap diberlakukan agar BE tidak rugi karena fee.
    buffer_pct: float = 0.25


@dataclass
class TrailingCfg:
    enabled: bool = True
    # percent -> SL = highest x (1 - percent_pct%), seperti sebelumnya.
    # atr     -> SL = highest - (atr_multiplier x ATR), ATR dibekukan saat
    #            entry supaya level tidak bergerak karena volatilitas saja.
    mode: str = "atr"              # percent | atr
    percent_pct: float = 0.5
    atr_multiplier: float = 2.0
    update_step_pct: float = 0.15


@dataclass
class ExecutionCfg:
    exit_mode: str = "oco"         # oco | manual
    entry_order_type: str = "market"  # market | limit
    limit_entry_timeout_sec: int = 20
    oco_sl_limit_buffer_pct: float = 0.3
    reconcile_sec: int = 5
    # Umur maksimum data pasar (detik) sebelum ENTRY BARU diblokir dan
    # pembaruan SL trailing/breakeven ditunda. 0 = gate dimatikan.
    # Default 90 detik = 1,5x interval watchdog WebSocket (60 detik).
    max_data_age_sec: float = 90.0
    # Deteksi fill TP/SL lewat User Data Stream (WebSocket API). Bila mati
    # atau tidak tersedia, bot kembali ke polling `reconcile_sec`.
    user_data_stream: bool = True
    # Paksa endpoint OCO lama POST /api/v3/order/oco (DEPRECATED sejak
    # 2024-04-02). Default false = pakai POST /api/v3/orderList/oco.
    oco_legacy_endpoint: bool = False


@dataclass
class DashboardCfg:
    # Default loopback: dashboard bisa menutup posisi dan mengubah parameter
    # risiko, jadi jangan pernah terbuka ke jaringan secara default.
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass
class DatabaseCfg:
    # Path default otomatis DIPISAH PER MODE oleh resolve_db_path()
    # (data/pumpbot.db -> data/pumpbot-{paper|live}.db).
    # Tulis path custom di YAML kalau ingin mengatur sendiri.
    path: str = "data/pumpbot.db"


# Konstanta path DB default - acuan pemisahan per mode (resolve_db_path).
DEFAULT_DB_PATH = "data/pumpbot.db"


@dataclass
class LoggingCfg:
    level: str = "INFO"
    file: str = "logs/bot.log"
    max_bytes: int = 10_485_760
    backups: int = 5


@dataclass
class DustSweepCfg:
    """
    Konversi berkala sisa koin kecil (dust) menjadi BNB.
    Hanya berjalan di mode LIVE (akun demo tidak punya aset BNB sungguhan).
    """
    enabled: bool = False
    interval_minutes: int = 360     # jeda antar sweep (menit)
    min_value_usd: float = 1.0      # aset bernilai < ini dianggap dust


@dataclass
class PaperCfg:
    """
    Parameter AKUN DEMO (mode `paper`).

    Harga, volume, dan order book diambil dari pasar Binance SUNGGUHAN lewat
    endpoint publik tanpa API key; hanya saldo dan order yang virtual.
    """
    # Modal virtual saat akun demo pertama kali dibuat. Setelah itu saldo
    # mengikuti hasil trading dan disimpan di database mode paper.
    start_balance: float = 10_000.0
    # Slippage taker per sisi, dalam basis point (10 bps = 0,1%). Beli terisi
    # di atas best ask, jual terisi di bawah best bid, supaya hasil demo tidak
    # lebih optimistis daripada kenyataan.
    slippage_bps: float = 2.0
    # true -> saldo & aset virtual dikosongkan ulang ke start_balance setiap
    # bot dinyalakan. Berguna untuk mengulang eksperimen dari titik yang sama.
    reset_on_start: bool = False


@dataclass
class Config:
    # Default sengaja `paper`: menjalankan bot tanpa mengubah apa pun tidak
    # boleh berisiko menyentuh uang sungguhan.
    mode: str = "paper"
    quote_asset: str = "USDT"
    universe: UniverseCfg = field(default_factory=UniverseCfg)
    data: DataCfg = field(default_factory=DataCfg)
    atr: AtrCfg = field(default_factory=AtrCfg)
    signal: SignalCfg = field(default_factory=SignalCfg)
    risk: RiskCfg = field(default_factory=RiskCfg)
    stops: StopsCfg = field(default_factory=StopsCfg)
    take_profit: TakeProfitCfg = field(default_factory=TakeProfitCfg)
    breakeven: BreakevenCfg = field(default_factory=BreakevenCfg)
    trailing: TrailingCfg = field(default_factory=TrailingCfg)
    execution: ExecutionCfg = field(default_factory=ExecutionCfg)
    dashboard: DashboardCfg = field(default_factory=DashboardCfg)
    database: DatabaseCfg = field(default_factory=DatabaseCfg)
    logging: LoggingCfg = field(default_factory=LoggingCfg)
    dust_sweep: DustSweepCfg = field(default_factory=DustSweepCfg)
    paper: PaperCfg = field(default_factory=PaperCfg)

    # Jalur file YAML asal, diisi otomatis oleh load_config. BUKAN field YAML:
    # dashboard memakainya agar tahu file mana yang harus ditulis ulang saat
    # menerapkan hasil backtest.
    source_path: str = ""


# ---------------------------------------------------------------------------
# Helper membangun dataclass bersarang dari dict YAML
# ---------------------------------------------------------------------------

_SUBDATACLASS_FIELDS = {
    "universe": UniverseCfg, "data": DataCfg, "atr": AtrCfg,
    "signal": SignalCfg,
    "risk": RiskCfg, "stops": StopsCfg, "take_profit": TakeProfitCfg,
    "breakeven": BreakevenCfg, "trailing": TrailingCfg,
    "execution": ExecutionCfg, "dashboard": DashboardCfg,
    "database": DatabaseCfg, "logging": LoggingCfg,
    "dust_sweep": DustSweepCfg, "paper": PaperCfg,
}

_NESTED = {
    ("signal", "manipulation"): ManipulationCfg,
    ("signal", "orderbook"): OrderBookCfg,
    ("signal", "trade_flow"): TradeFlowCfg,
    ("signal", "volume"): VolumeCfg,
    ("signal", "whale"): WhaleCfg,
    ("signal", "price_action"): PriceActionCfg,
    ("signal", "vwap"): VWAPCfg,
    ("signal", "change_24h"): Change24hCfg,
}


def _build(cls, data: Optional[dict]):
    """Buat instance dataclass dari dict, mengabaikan key tak dikenal."""
    if data is None:
        return cls()
    if not isinstance(data, dict):
        raise ConfigError(f"Nilai konfigurasi untuk {cls.__name__} harus berupa map/dict, dapat: {type(data)}")
    import dataclasses as dc
    valid = {f.name for f in dc.fields(cls)}
    kwargs = {k: v for k, v in data.items() if k in valid}
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise ConfigError(f"Konfigurasi section {cls.__name__} salah: {exc}") from exc


def resolve_db_path(mode: str, path: str) -> str:
    """
    Pisahkan file database per mode (paper/live) supaya histori dan statistik
    antar mode tidak tercampur - tanpa ini, trade akun demo tampil di
    dashboard live dan mengotorkan win rate / PF / MDD.

    Aturan:
    - Path DEFAULT ("data/pumpbot.db") otomatis menjadi
      "data/pumpbot-{mode}.db" (paper / live).
    - Path custom (mis. "data/demo.db") TIDAK diubah - kendali penuh
      tetap di tangan pengguna.
    - File lama "data/pumpbot.db" TIDAK dipindah/dihapus otomatis.
      Untuk memakai histori lama sebagai akun demo, jalankan sekali:
          mv data/pumpbot.db data/pumpbot-paper.db
    """
    if os.path.normpath(path) == os.path.normpath(DEFAULT_DB_PATH):
        root, ext = os.path.splitext(path)
        return f"{root}-{mode}{ext or '.db'}"
    return path


def load_config(path: str) -> Config:
    """Muat YAML -> Config + terapkan override environment variable."""
    # Muat .env agar API key dan pengaturan runtime non-strategi terbaca.
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    if not os.path.exists(path):
        raise ConfigError(
            f"File konfigurasi tidak ditemukan: {path}\n"
            "PumpBot hanya memakai config/config.yaml; pastikan file tersebut tersedia."
        )

    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    cfg = Config()
    for key, value in raw.items():
        if key in _SUBDATACLASS_FIELDS:
            setattr(cfg, key, _build(_SUBDATACLASS_FIELDS[key], value))
        elif hasattr(cfg, key):
            setattr(cfg, key, value)
        else:
            raise ConfigError(f"Section konfigurasi tidak dikenal: '{key}'")

    # Sub-section bertingkat (signal.manipulation, dst.)
    for (section, sub), cls in _NESTED.items():
        parent_raw = raw.get(section) or {}
        if isinstance(parent_raw, dict) and sub in parent_raw:
            setattr(getattr(cfg, section), sub, _build(cls, parent_raw[sub]))

    # Kunci lama fitur ATR: sebelum ATR dihidupkan kembali, fitur ini pernah
    # dihapus dan kuncinya ditolak keras. Sekarang `trailing.mode` dan
    # `trailing.atr_multiplier` kembali SAH (lihat TrailingCfg), sedangkan
    # `trailing.atr_period` dipindahkan ke section `atr.period` karena satu
    # nilai periode kini dipakai bersama SL, TP, BE, dan trailing. Kunci lama
    # tetap diterima sebagai alias agar config versi sebelumnya tidak mati,
    # dengan peringatan supaya dipindahkan.
    _LEGACY_TRAILING_ATR_PERIOD = "atr_period"
    trailing_raw = raw.get("trailing")
    atr_raw = raw.get("atr") or {}
    legacy_atr_period = (trailing_raw.get(_LEGACY_TRAILING_ATR_PERIOD)
                         if isinstance(trailing_raw, dict) else None)
    if legacy_atr_period is not None:
        if isinstance(atr_raw, dict) and "period" in atr_raw:
            warnings.warn(
                "config: 'trailing.atr_period' diabaikan karena 'atr.period' "
                "sudah diisi. Hapus kunci lama itu dari config.yaml.",
                UserWarning, stacklevel=2)
        else:
            try:
                cfg.atr.period = int(legacy_atr_period)
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"trailing.atr_period harus bilangan bulat (dapat: "
                    f"{legacy_atr_period!r})") from exc
            warnings.warn(
                "config: 'trailing.atr_period' kini bernama 'atr.period' "
                "(satu periode ATR dipakai SL, TP, BE, dan trailing). "
                "Nilainya tetap dipakai, tapi pindahkan kuncinya.",
                UserWarning, stacklevel=2)

    # Normalisasi daftar target TP -> list[TPTarget]
    tp_raw = raw.get("take_profit") or {}
    if isinstance(tp_raw, dict) and "targets" in tp_raw:
        raw_targets = tp_raw["targets"]
        if not isinstance(raw_targets, list):
            raise ConfigError(
                "take_profit.targets harus berupa daftar map, contoh: "
                "[{gain_pct: 1.2, sell_pct: 50}, {gain_pct: 2.5, sell_pct: 50}]")
        targets = []
        for i, t in enumerate(raw_targets):
            if not isinstance(t, dict):
                raise ConfigError(
                    f"take_profit.targets[{i}] harus map dengan kunci "
                    f"'gain_pct' dan 'sell_pct' (dapat: {type(t).__name__})")
            try:
                targets.append(TPTarget(float(t.get("gain_pct", 0)),
                                        float(t.get("sell_pct", 0))))
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"take_profit.targets[{i}]: gain_pct/sell_pct harus "
                    f"angka ({exc})") from exc
        cfg.take_profit.targets = targets

    # ---------------- Override environment non-strategi ----------------
    # Mode sengaja TIDAK dapat dioverride via environment: ubah langsung
    # config/config.yaml supaya satu sumber konfigurasi selalu jelas.
    if os.getenv("DASHBOARD_HOST"):
        cfg.dashboard.host = os.getenv("DASHBOARD_HOST")
    if os.getenv("DASHBOARD_PORT"):
        try:
            cfg.dashboard.port = int(os.getenv("DASHBOARD_PORT"))
        except ValueError:
            raise ConfigError("DASHBOARD_PORT harus angka")
    if os.getenv("LOG_LEVEL"):
        cfg.logging.level = os.getenv("LOG_LEVEL")

    # Pisahkan file DB per mode (paper/live) - path custom lolos tanpa
    # diubah. Mode final selalu berasal dari YAML tunggal.
    cfg.database.path = resolve_db_path(cfg.mode, cfg.database.path)

    # Catat asal file supaya komponen lain (mis. dashboard saat menerapkan
    # hasil backtest) menulis ke file yang benar, bukan menebak jalur.
    cfg.source_path = path

    errors = validate(cfg)
    if errors:
        msg = "\n  - ".join(["Konfigurasi TIDAK VALID:"] + errors)
        raise ConfigError(msg)
    return cfg


# ---------------------------------------------------------------------------
# Validasi menyeluruh - semua dicek SEBELUM bot berjalan
# ---------------------------------------------------------------------------

def validate(cfg: Config) -> list[str]:
    errors: list[str] = []

    # --- mode & aset ---
    if cfg.mode not in ("paper", "live"):
        errors.append(
            f"mode harus 'paper' (akun demo) | 'live' (uang sungguhan) "
            f"(dapat '{cfg.mode}')")
    if not cfg.quote_asset or not cfg.quote_asset.isalpha():
        errors.append("quote_asset harus nama aset yang valid, mis. 'USDT'")
    # HANYA mode live yang butuh kredensial. Akun demo memakai endpoint
    # market data publik Binance yang tidak menerima API key sama sekali.
    if cfg.mode == "live":
        if not os.getenv("BINANCE_API_KEY") or not os.getenv("BINANCE_API_SECRET"):
            errors.append(
                "mode 'live' membutuhkan BINANCE_API_KEY & BINANCE_API_SECRET di file .env"
            )

    # --- akun demo (paper) ---
    p = cfg.paper
    if (isinstance(p.start_balance, bool)
            or not isinstance(p.start_balance, (int, float))
            or not math.isfinite(float(p.start_balance))
            or float(p.start_balance) <= 0):
        errors.append("paper.start_balance harus angka positif (> 0)")
    if (isinstance(p.slippage_bps, bool)
            or not isinstance(p.slippage_bps, (int, float))
            or not math.isfinite(float(p.slippage_bps))
            or not 0 <= float(p.slippage_bps) <= 500):
        errors.append("paper.slippage_bps harus di rentang 0..500 basis point")
    if not isinstance(p.reset_on_start, bool):
        errors.append("paper.reset_on_start harus true atau false")

    # --- dust sweep ---
    # (mode != live TIDAK dianggap error - fitur cukup dilewati dengan
    #  warning di main, supaya satu file config bisa dipakai lintas mode)
    if not 30 <= cfg.dust_sweep.interval_minutes <= 10080:
        errors.append("dust_sweep.interval_minutes harus di 30..10080")
    if cfg.dust_sweep.min_value_usd < 0:
        errors.append("dust_sweep.min_value_usd tidak boleh negatif")

    # --- universe ---
    u = cfg.universe
    # 0 berarti semua simbol yang lolos filter universe. Tidak diberi batas
    # angka di sini; kapasitas nyata tetap ditentukan koneksi WebSocket/API.
    if (isinstance(u.max_symbols, bool)
            or not isinstance(u.max_symbols, int)
            or u.max_symbols < 0):
        errors.append("universe.max_symbols harus bilangan bulat >= 0 (0 = tanpa batas)")
    if u.min_quote_volume_24h < 0:
        errors.append("universe.min_quote_volume_24h tidak boleh negatif")
    if u.refresh_minutes < 5:
        errors.append("universe.refresh_minutes minimal 5 menit (hindari spam API)")

    # --- data ---
    d = cfg.data
    if d.kline_interval not in ("1m", "3m", "5m", "15m"):
        errors.append("data.kline_interval harus salah satu dari: 1m, 3m, 5m, 15m")
    if not 20 <= d.history_candles <= 1000:
        errors.append("data.history_candles harus di rentang 20..1000")
    if d.depth_levels not in (5, 10, 20):
        errors.append("data.depth_levels harus 5, 10, atau 20")
    if d.depth_speed_ms not in (100, 1000):
        errors.append("data.depth_speed_ms harus 100 atau 1000")

    # --- signal ---
    s = cfg.signal
    if not 0 <= s.score_threshold <= 100:
        errors.append("signal.score_threshold harus di rentang 0..100")
    if s.cooldown_after_exit_min < 0:
        errors.append("signal.cooldown_after_exit_min tidak boleh negatif")
    if s.reevaluate_sec < 1:
        errors.append("signal.reevaluate_sec minimal 1 detik")
    if s.min_candles < 10:
        errors.append("signal.min_candles minimal 10")

    w = s.weights
    if not 1 <= cfg.signal.orderbook.imbalance_levels <= 20:
        errors.append("signal.orderbook.imbalance_levels harus di 1..20")
    if cfg.signal.orderbook.imbalance_levels > cfg.data.depth_levels:
        errors.append(
            "signal.orderbook.imbalance_levels tidak boleh lebih besar dari "
            "data.depth_levels (detektor tak bisa membaca level yang tidak "
            "dikirim stream)")
    for name in ("orderbook", "trade_flow", "volume", "whale", "price_action"):
        if name not in w:
            errors.append(f"signal.weights.{name} wajib ada")
        elif w[name] < 0:
            errors.append(f"signal.weights.{name} tidak boleh negatif")
    if sum(v for v in w.values()) <= 0:
        errors.append("signal.weights: jumlah bobot harus > 0")

    m = s.manipulation
    if not 0 <= m.weight <= 1:
        errors.append("signal.manipulation.weight harus di rentang 0..1")
    if not 0 <= m.veto_threshold <= 1:
        errors.append("signal.manipulation.veto_threshold harus di rentang 0..1")
    c24 = s.change_24h
    if not isinstance(c24.enabled, bool):
        errors.append("signal.change_24h.enabled harus true/false")
    try:
        min_pct = float(c24.min_pct)
        max_pct = float(c24.max_pct)
    except (TypeError, ValueError):
        errors.append("signal.change_24h.min_pct/max_pct harus angka")
    else:
        if not math.isfinite(min_pct) or not math.isfinite(max_pct):
            errors.append("signal.change_24h.min_pct/max_pct harus angka hingga")
        else:
            if min_pct < 0:
                errors.append(
                    "signal.change_24h.min_pct tidak boleh negatif: band ini "
                    "mengukur BESAR pergerakan (nilai absolut), jadi batas "
                    "bawah negatif tidak punya arti")
            if min_pct > max_pct:
                errors.append(
                    f"signal.change_24h.min_pct ({min_pct:g}) tidak boleh lebih "
                    f"besar dari max_pct ({max_pct:g})")
            if c24.enabled and max_pct <= 0:
                errors.append(
                    "signal.change_24h.max_pct harus > 0 selama gate aktif: "
                    "band nol membuat SEMUA koin ditolak")

    if m.max_change_24h_pct <= 0:
        errors.append(
            "signal.manipulation.max_change_24h_pct harus > 0: nilai ini "
            "adalah ambang kenaikan 24 jam, dan nilai nol atau negatif "
            "membuat semua koin dianggap sudah naik terlalu jauh")
    if m.pump_dump_gain_pct <= 0 or m.pump_dump_window_min < 1:
        errors.append("signal.manipulation.pump_dump_* tidak valid")
    if m.wash_volume_spike < 1:
        errors.append("signal.manipulation.wash_volume_spike harus >= 1")
    if m.wash_range_pct <= 0:
        errors.append("signal.manipulation.wash_range_pct harus > 0")

    if s.trade_flow.max_spread_bps <= 0:
        errors.append("signal.trade_flow.max_spread_bps harus > 0")
    if s.volume.ma_period < 2:
        errors.append("signal.volume.ma_period minimal 2")
    if s.whale.multiplier < 1:
        errors.append("signal.whale.multiplier harus >= 1")
    if s.whale.min_notional_usd < 0:
        errors.append("signal.whale.min_notional_usd tidak boleh negatif")

    pa = s.price_action
    if pa.structure_candles < 10:
        errors.append("signal.price_action.structure_candles minimal 10")
    if pa.breakout_lookback < 5:
        errors.append("signal.price_action.breakout_lookback minimal 5")
    if len(pa.fib_zone) != 2 or not (0 < pa.fib_zone[0] < pa.fib_zone[1] < 1):
        errors.append("signal.price_action.fib_zone harus [lower, upper] dengan 0 < lower < upper < 1")

    # --- filter anchored vwap (gate entry, bukan skor) ---
    v = s.vwap
    if not isinstance(v.enabled, bool):
        errors.append("signal.vwap.enabled harus true atau false")
    if v.anchor_mode not in ("pump_start", "impulse_low", "manual"):
        errors.append("signal.vwap.anchor_mode harus 'pump_start', "
                      "'impulse_low', atau 'manual'")
    if v.no_anchor_action not in ("impulse_low", "block"):
        errors.append("signal.vwap.no_anchor_action harus 'impulse_low' atau 'block'")
    if v.on_insufficient_data not in ("block", "allow"):
        errors.append("signal.vwap.on_insufficient_data harus 'block' "
                      "atau 'allow'")
    if v.anchor_lookback_candles < 5:
        errors.append("signal.vwap.anchor_lookback_candles minimal 5")
    if v.anchor_lookback_candles + v.pump_baseline_candles > cfg.data.history_candles:
        errors.append(
            "signal.vwap.anchor_lookback_candles + pump_baseline_candles tidak boleh "
            "melebihi data.history_candles (buffer tidak cukup untuk mencari anchor)")
    if v.pump_volume_mult <= 1:
        errors.append("signal.vwap.pump_volume_mult harus > 1")
    if v.pump_baseline_candles < 2:
        errors.append("signal.vwap.pump_baseline_candles minimal 2")
    if v.pump_max_gap_candles < 0:
        errors.append("signal.vwap.pump_max_gap_candles tidak boleh negatif")
    if v.min_anchor_candles < 1:
        errors.append("signal.vwap.min_anchor_candles minimal 1")
    if not v.min_above_pct < v.max_above_pct:
        errors.append("signal.vwap.min_above_pct harus lebih kecil dari max_above_pct")
    if v.min_above_pct < -5:
        errors.append("signal.vwap.min_above_pct minimal -5 persen")
    if v.max_above_pct > 100:
        errors.append("signal.vwap.max_above_pct maksimal 100 persen")
    if not isinstance(v.manual_anchors, dict):
        errors.append("signal.vwap.manual_anchors harus map {SIMBOL: epoch_ms}")
    else:
        for k, val in v.manual_anchors.items():
            if not isinstance(k, str):
                errors.append("signal.vwap.manual_anchors: kunci harus nama "
                              "simbol (string)")
            if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
                errors.append(
                    f"signal.vwap.manual_anchors['{k}'] harus epoch "
                    "milidetik bulat positif")

    # --- risk (paling kritis!) ---
    r = cfg.risk
    if (isinstance(r.risk_per_trade_pct, bool)
            or not isinstance(r.risk_per_trade_pct, (int, float))
            or not math.isfinite(float(r.risk_per_trade_pct))
            or float(r.risk_per_trade_pct) <= 0):
        errors.append("risk.risk_per_trade_pct harus angka positif (> 0); tidak ada batas atas software")
    if not isinstance(r.allow_multiple_positions, bool):
        errors.append("risk.allow_multiple_positions harus true atau false")
    if (isinstance(r.max_open_positions, bool)
            or not isinstance(r.max_open_positions, int)
            or not 1 <= r.max_open_positions <= 3):
        errors.append("risk.max_open_positions harus bilangan bulat di rentang 1..3")
    if not isinstance(r.daily_loss_enabled, bool):
        errors.append("risk.daily_loss_enabled harus true atau false")
    if not 0 < r.daily_loss_limit_pct <= 50:
        errors.append("risk.daily_loss_limit_pct harus di rentang (0, 50]")
    if not 0 <= r.fee_pct <= 1:
        errors.append("risk.fee_pct harus di rentang 0..1 (persen)")

    # --- stops ---
    st = cfg.stops
    if st.mode not in ("structure", "percent", "atr"):
        errors.append("stops.mode harus 'structure', 'percent', atau 'atr'")
    if not 0.1 <= st.percent_pct <= 20:
        errors.append("stops.percent_pct harus di rentang 0.1..20 persen")
    if not 0.05 <= st.atr_multiplier <= 100:
        errors.append("stops.atr_multiplier harus di rentang 0.05..100")
    if not 0.05 <= st.atr_min_multiplier <= 100:
        errors.append("stops.atr_min_multiplier harus di rentang 0.05..100")
    if not 0.05 <= st.atr_max_multiplier <= 100:
        errors.append("stops.atr_max_multiplier harus di rentang 0.05..100")
    if not st.atr_min_multiplier <= st.atr_max_multiplier:
        errors.append(
            "stops.atr_min_multiplier harus <= stops.atr_max_multiplier")
    if not 0 <= st.structure_buffer_pct <= 5:
        errors.append("stops.structure_buffer_pct harus di rentang 0..5 persen")
    if not st.min_stop_pct <= st.max_stop_pct:
        errors.append("stops.min_stop_pct harus <= stops.max_stop_pct")
    if not 0.1 <= st.max_stop_pct <= 20:
        errors.append("stops.max_stop_pct harus di rentang 0.1..20 persen")

    # --- ATR (dipakai bersama SL, TP, BE, dan trailing) ---
    a = cfg.atr
    if (isinstance(a.period, bool) or not isinstance(a.period, int)
            or not ATR_MIN_PERIOD <= a.period <= ATR_MAX_PERIOD):
        errors.append(
            f"atr.period harus bilangan bulat di rentang "
            f"{ATR_MIN_PERIOD}..{ATR_MAX_PERIOD}")
    if a.method not in ATR_METHODS:
        errors.append(
            f"atr.method harus salah satu dari {', '.join(ATR_METHODS)}")
    if (isinstance(a.period, int) and not isinstance(a.period, bool)
            and a.period + 1 > cfg.data.history_candles):
        errors.append(
            f"atr.period + 1 ({a.period + 1}) tidak boleh melebihi "
            f"data.history_candles ({cfg.data.history_candles}): buffer candle "
            f"tidak cukup untuk menghitung ATR")

    # --- take profit ---
    tp = cfg.take_profit
    if tp.mode not in ("multi", "rr", "single", "atr"):
        errors.append("take_profit.mode harus 'multi', 'rr', 'single', atau 'atr'")
    if not 0.05 <= tp.atr_multiplier <= 100:
        errors.append("take_profit.atr_multiplier harus di rentang 0.05..100")
    if tp.mode == "rr" and tp.rr <= 0:
        errors.append("take_profit.rr harus > 0")
    if tp.mode in ("multi", "single"):
        if not tp.targets:
            errors.append("take_profit.targets tidak boleh kosong untuk mode multi/single")
        else:
            for i, t in enumerate(tp.targets):
                if t.gain_pct <= 0:
                    errors.append(f"take_profit.targets[{i}].gain_pct harus > 0")
            total_sell = sum(t.sell_pct for t in tp.targets)
            if tp.mode == "multi" and abs(total_sell - 100.0) > 0.01:
                errors.append(
                    f"take_profit.targets: jumlah sell_pct harus tepat 100% (sekarang {total_sell}%)"
                )
            gains = [t.gain_pct for t in tp.targets]
            if gains != sorted(gains):
                errors.append("take_profit.targets harus diurutkan dari gain_pct terkecil")

    # --- breakeven & trailing ---
    b = cfg.breakeven
    if b.trigger_mode not in ("auto", "rr", "atr"):
        errors.append("breakeven.trigger_mode harus 'auto', 'rr', atau 'atr'")
    if b.trigger_rr <= 0:
        errors.append("breakeven.trigger_rr harus > 0")
    if not 0.05 <= b.trigger_atr_mult <= 100:
        errors.append("breakeven.trigger_atr_mult harus di rentang 0.05..100")
    if not 0 <= b.buffer_pct <= 2:
        errors.append("breakeven.buffer_pct harus di rentang 0..2 persen")

    t = cfg.trailing
    if t.mode not in ("percent", "atr"):
        errors.append("trailing.mode harus 'percent' atau 'atr'")
    if not 0.1 <= t.percent_pct <= 20:
        errors.append("trailing.percent_pct harus di rentang 0.1..20 persen")
    if not 0.05 <= t.atr_multiplier <= 100:
        errors.append("trailing.atr_multiplier harus di rentang 0.05..100")
    if t.update_step_pct <= 0:
        errors.append("trailing.update_step_pct harus > 0")

    # --- execution ---
    e = cfg.execution
    if e.exit_mode not in ("oco", "manual"):
        errors.append("execution.exit_mode harus 'oco' atau 'manual'")
    if e.entry_order_type not in ("market", "limit"):
        errors.append("execution.entry_order_type harus 'market' atau 'limit'")
    if e.limit_entry_timeout_sec < 5:
        errors.append("execution.limit_entry_timeout_sec minimal 5 detik")
    if not 0 <= e.oco_sl_limit_buffer_pct <= 5:
        errors.append("execution.oco_sl_limit_buffer_pct harus di rentang 0..5 persen")
    if e.reconcile_sec < 2:
        errors.append("execution.reconcile_sec minimal 2 detik")
    if e.max_data_age_sec < 0:
        errors.append("execution.max_data_age_sec tidak boleh negatif "
                      "(0 = gate data basi dimatikan)")

    # --- lain-lain ---
    if not 1 <= cfg.dashboard.port <= 65535:
        errors.append("dashboard.port harus port yang valid (1..65535)")
    return errors
