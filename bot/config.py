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

import os
from dataclasses import dataclass, field
from typing import Optional

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
    max_change_24h_pct: float = 20.0
    pump_dump_gain_pct: float = 8.0
    pump_dump_pullback_pct: float = 40.0
    pump_dump_window_min: int = 15
    overextended_5m_pct: float = 5.0
    wash_volume_spike: float = 2.0
    wash_range_pct: float = 0.4


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


@dataclass
class RiskCfg:
    # Satu-satunya batas risiko per entry: % dari balance (bukan equity/PnL berjalan).
    risk_per_trade_pct: float = 1.0
    # Default aman: hanya satu posisi. Multi-posisi perlu opt-in eksplisit.
    allow_multiple_positions: bool = False
    max_open_positions: int = 3
    # Fitur opsional; saat false, limit tidak memblokir entry.
    daily_loss_enabled: bool = False
    daily_loss_limit_pct: float = 3.0
    fee_pct: float = 0.1


@dataclass
class StopsCfg:
    mode: str = "percent"
    percent_pct: float = 1.0
    structure_buffer_pct: float = 0.15
    max_stop_pct: float = 4.0
    min_stop_pct: float = 0.5


@dataclass
class TPTarget:
    gain_pct: float = 1.2
    sell_pct: float = 50.0


@dataclass
class TakeProfitCfg:
    # Default satu TP 2R: SL 1% menghasilkan TP 2%.
    mode: str = "rr"               # rr | multi | single (mode lama tetap didukung)
    rr: float = 2.0
    targets: list = field(default_factory=lambda: [TPTarget(2.0, 100)])


@dataclass
class BreakevenCfg:
    enabled: bool = True
    # BE dipicu pada 1R: entry + trigger_rr x (entry - initial_stop).
    trigger_rr: float = 1.0
    # Buffer minimal 2x fee tetap diberlakukan agar BE tidak rugi karena fee.
    buffer_pct: float = 0.25


@dataclass
class TrailingCfg:
    enabled: bool = True
    mode: str = "percent"          # percent | atr
    # Default: setelah BE aktif, SL mengikuti high dengan jarak 0,5%.
    percent_pct: float = 0.5
    atr_period: int = 14
    atr_multiplier: float = 2.5
    update_step_pct: float = 0.15


@dataclass
class ExecutionCfg:
    exit_mode: str = "oco"         # oco | manual
    entry_order_type: str = "market"  # market | limit
    limit_entry_timeout_sec: int = 20
    oco_sl_limit_buffer_pct: float = 0.3
    reconcile_sec: int = 5


@dataclass
class DashboardCfg:
    host: str = "0.0.0.0"
    port: int = 8000


@dataclass
class DatabaseCfg:
    # Path default otomatis DIPISAH PER MODE oleh resolve_db_path()
    # (data/pumpbot.db -> data/pumpbot-{testnet|live|paper}.db).
    # Tulis path custom di YAML kalau ingin mengatur sendiri.
    path: str = "data/pumpbot.db"


# Konstanta path DB default — acuan pemisahan per mode (resolve_db_path).
DEFAULT_DB_PATH = "data/pumpbot.db"


@dataclass
class LoggingCfg:
    level: str = "INFO"
    file: str = "logs/bot.log"
    max_bytes: int = 10_485_760
    backups: int = 5


@dataclass
class PaperCfg:
    start_equity: float = 10_000
    symbols: int = 12
    time_scale: float = 1.0
    seed: int = 42


@dataclass
class DustSweepCfg:
    """
    Konversi berkala sisa koin kecil (dust) menjadi BNB.
    Hanya berjalan di mode LIVE (endpoint SAPI tidak ada di testnet).
    """
    enabled: bool = False
    interval_minutes: int = 360     # jeda antar sweep (menit)
    min_value_usd: float = 1.0      # aset bernilai < ini dianggap dust


@dataclass
class Config:
    # Safety default: konfigurasi tunggal memulai di simulator internal.
    mode: str = "paper"
    quote_asset: str = "USDT"
    universe: UniverseCfg = field(default_factory=UniverseCfg)
    data: DataCfg = field(default_factory=DataCfg)
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
    paper: PaperCfg = field(default_factory=PaperCfg)
    dust_sweep: DustSweepCfg = field(default_factory=DustSweepCfg)


# ---------------------------------------------------------------------------
# Helper membangun dataclass bersarang dari dict YAML
# ---------------------------------------------------------------------------

_SUBDATACLASS_FIELDS = {
    "universe": UniverseCfg, "data": DataCfg, "signal": SignalCfg,
    "risk": RiskCfg, "stops": StopsCfg, "take_profit": TakeProfitCfg,
    "breakeven": BreakevenCfg, "trailing": TrailingCfg,
    "execution": ExecutionCfg, "dashboard": DashboardCfg,
    "database": DatabaseCfg, "logging": LoggingCfg, "paper": PaperCfg,
    "dust_sweep": DustSweepCfg,
}

_NESTED = {
    ("signal", "manipulation"): ManipulationCfg,
    ("signal", "orderbook"): OrderBookCfg,
    ("signal", "trade_flow"): TradeFlowCfg,
    ("signal", "volume"): VolumeCfg,
    ("signal", "whale"): WhaleCfg,
    ("signal", "price_action"): PriceActionCfg,
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
    Pisahkan file database per mode (testnet/live/paper) supaya histori
    dan statistik antar mode tidak tercampur — tanpa ini, trade testnet
    lama tampil di dashboard live dan mengotorkan win rate / PF / MDD.

    Aturan:
    - Path DEFAULT ("data/pumpbot.db") otomatis menjadi
      "data/pumpbot-{mode}.db" (testnet / live / paper).
    - Path custom (mis. "data/demo.db") TIDAK diubah — kendali penuh
      tetap di tangan pengguna.
    - File lama "data/pumpbot.db" TIDAK dipindah/dihapus otomatis.
      Untuk mempertahankan histori lama bagi testnet, jalankan sekali:
          mv data/pumpbot.db data/pumpbot-testnet.db
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

    # Normalisasi daftar target TP -> list[TPTarget]
    tp_raw = raw.get("take_profit") or {}
    if isinstance(tp_raw, dict) and "targets" in tp_raw:
        targets = []
        for i, t in enumerate(tp_raw["targets"]):
            targets.append(TPTarget(float(t.get("gain_pct", 0)), float(t.get("sell_pct", 0))))
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

    # Pisahkan file DB per mode (testnet/live/paper) — path custom lolos
    # tanpa diubah. Mode final selalu berasal dari YAML tunggal.
    cfg.database.path = resolve_db_path(cfg.mode, cfg.database.path)

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
    if cfg.mode not in ("testnet", "live", "paper"):
        errors.append(f"mode harus 'testnet' | 'live' | 'paper' (dapat '{cfg.mode}')")
    if not cfg.quote_asset or not cfg.quote_asset.isalpha():
        errors.append("quote_asset harus nama aset yang valid, mis. 'USDT'")
    if cfg.mode in ("testnet", "live"):
        if not os.getenv("BINANCE_API_KEY") or not os.getenv("BINANCE_API_SECRET"):
            errors.append(
                f"mode '{cfg.mode}' membutuhkan BINANCE_API_KEY & BINANCE_API_SECRET di file .env"
            )

    # --- dust sweep ---
    # (mode != live TIDAK dianggap error — fitur cukup dilewati dengan
    #  warning di main, supaya satu file config bisa dipakai lintas mode)
    if not 30 <= cfg.dust_sweep.interval_minutes <= 10080:
        errors.append("dust_sweep.interval_minutes harus di 30..10080")
    if cfg.dust_sweep.min_value_usd < 0:
        errors.append("dust_sweep.min_value_usd tidak boleh negatif")

    # --- universe ---
    u = cfg.universe
    if u.max_symbols < 1 or u.max_symbols > 200:
        errors.append("universe.max_symbols harus di rentang 1..200")
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

    # --- risk (paling kritis!) ---
    r = cfg.risk
    if not 0 < r.risk_per_trade_pct <= 10:
        errors.append("risk.risk_per_trade_pct harus di rentang (0, 10] persen - jangan buang modal")
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
    if st.mode not in ("structure", "percent"):
        errors.append("stops.mode harus 'structure' atau 'percent'")
    if not 0.1 <= st.percent_pct <= 20:
        errors.append("stops.percent_pct harus di rentang 0.1..20 persen")
    if not 0 <= st.structure_buffer_pct <= 5:
        errors.append("stops.structure_buffer_pct harus di rentang 0..5 persen")
    if not st.min_stop_pct <= st.max_stop_pct:
        errors.append("stops.min_stop_pct harus <= stops.max_stop_pct")
    if not 0.1 <= st.max_stop_pct <= 20:
        errors.append("stops.max_stop_pct harus di rentang 0.1..20 persen")

    # --- take profit ---
    tp = cfg.take_profit
    if tp.mode not in ("multi", "rr", "single"):
        errors.append("take_profit.mode harus 'multi', 'rr', atau 'single'")
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
    if b.trigger_rr <= 0:
        errors.append("breakeven.trigger_rr harus > 0")
    if not 0 <= b.buffer_pct <= 2:
        errors.append("breakeven.buffer_pct harus di rentang 0..2 persen")

    t = cfg.trailing
    if t.mode not in ("percent", "atr"):
        errors.append("trailing.mode harus 'percent' atau 'atr'")
    if not 0.1 <= t.percent_pct <= 20:
        errors.append("trailing.percent_pct harus di rentang 0.1..20 persen")
    if t.atr_period < 2:
        errors.append("trailing.atr_period minimal 2")
    if t.atr_multiplier <= 0:
        errors.append("trailing.atr_multiplier harus > 0")
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

    # --- lain-lain ---
    if not 1 <= cfg.dashboard.port <= 65535:
        errors.append("dashboard.port harus port yang valid (1..65535)")
    if cfg.paper.time_scale <= 0:
        errors.append("paper.time_scale harus > 0")
    if cfg.paper.start_equity <= 0:
        errors.append("paper.start_equity harus > 0")
    if cfg.paper.symbols < 1:
        errors.append("paper.symbols minimal 1")

    return errors
