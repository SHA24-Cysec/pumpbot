"""
Dust sweep — konversi sisa koin kecil (dust) menjadi BNB.

Sumber dust di bot ini: sisa qty pembulatan LOT_SIZE setelah seluruh chunk
TP terjual (umumnya < 0.001 unit, nilainya di bawah MIN_NOTIONAL sehingga
tidak bisa dijual biasa — lihat executor._finalize_if_done), ditambah dust
lain di akun yang sama dari trading manual sebelumnya.

Cara kerja (HANYA mode live — endpoint SAPI tidak tersedia di testnet):
  1. Query daftar aset yang bisa dikonversi ke BNB
     (POST /sapi/v1/asset/dust-btc via binance-sdk-wallet). Binance sudah
     memfilter dengan aturan dust mereka sendiri (saldo di bawah ambang).
  2. Filter tambahan dari config:
       - nilai aset < dust_sweep.min_value_usd (perkiraan = toBTC x harga
         BTC); kalau harga BTC tidak diketahui, filter nilai dilewati
       - aset quote (mis. USDT) dan BNB tidak ikut dikonversi
  3. POST dust transfer (POST /sapi/v1/asset/dust) per kelompok aset,
     maksimal 10 aset per request (batas API Binance).
  4. Hasil (BNB diterima + aset yang ikut) dicatat ke log & tabel events.

Catatan desain:
  - Metode binance-sdk-wallet itu SINKRON (blocking) -> semua pemanggilan
    dibungkus asyncio.to_thread supaya tidak memblokir event loop bot.
  - BNB hasil konversi tidak dipakai bot (bot trading dengan quote USDT);
    BNB hanya terkumpul di wallet (bisa dipakai membayar fee manual).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

logger = logging.getLogger("pumpbot.dust")

# Batas API Binance: maksimal 10 aset per request dust transfer
MAX_ASSETS_PER_TRANSFER = 10


def _to_plain(data) -> dict:
    """Normalisasi respons SDK -> dict murni (camelCase, sesuai JSON asli)."""
    if hasattr(data, "data"):          # ApiResponse
        data = data.data()
    if hasattr(data, "to_dict"):
        return data.to_dict()
    return data if isinstance(data, dict) else {}


class DustSweeper:
    """
    Task latar yang secara berkala menyapu dust ke BNB.

    Parameter:
      cfg        : Config aplikasi (dipakai: cfg.dust_sweep, cfg.quote_asset)
      db         : Database (untuk mencatat event DUST_SWEEP)
      wallet_rest: objek rest_api dari binance-sdk-wallet (metode SINKRON)
      btc_price_fn: async callable tanpa argumen -> harga BTC dalam quote
    """

    def __init__(self, cfg, db, wallet_rest,
                 btc_price_fn: Callable[[], Awaitable[float]]):
        self.cfg = cfg
        self.db = db
        self.wallet = wallet_rest
        self.btc_price_fn = btc_price_fn

    # ------------------------------------------------------------------
    async def sweep_once(self) -> Optional[dict]:
        """Satu putaran sweep. Return ringkasan kalau ada yang dikonversi."""
        # --- 1. daftar aset yang layak dikonversi menurut Binance ---
        try:
            resp = await asyncio.to_thread(
                self.wallet.get_assets_that_can_be_converted_into_bnb)
        except Exception as exc:
            logger.warning(f"Dust sweep: gagal query aset convertible: {exc}")
            return None
        details = _to_plain(resp).get("details") or []
        if not details:
            logger.info("Dust sweep: tidak ada aset yang bisa dikonversi")
            return None

        # --- 2. filter tambahan milik kita ---
        btc_price = 0.0
        try:
            btc_price = float(await self.btc_price_fn() or 0)
        except Exception:
            btc_price = 0.0   # harga tidak diketahui -> filter nilai dilewati

        ds = self.cfg.dust_sweep
        quote = (self.cfg.quote_asset or "").upper()
        targets: list[str] = []
        for d in details:
            asset = str(d.get("asset", "")).upper()
            amount = float(d.get("amountFree", 0) or 0)
            to_btc = float(d.get("toBTC", 0) or 0)
            if asset in ("BNB", quote) or amount <= 0:
                continue
            # aset yang nilainya MASIH BESAR bukan dust bagi kita —
            # kalau dibiarkan, kita malah mencairkan posisi yang disengaja
            if btc_price > 0 and to_btc * btc_price >= ds.min_value_usd:
                continue
            targets.append(asset)

        if not targets:
            logger.info(f"Dust sweep: tidak ada dust bernilai "
                        f"< {ds.min_value_usd} {quote}")
            return None

        # --- 3. transfer per kelompok maks 10 aset ---
        bnb_received = 0.0
        transferred: list[str] = []
        for i in range(0, len(targets), MAX_ASSETS_PER_TRANSFER):
            chunk = targets[i:i + MAX_ASSETS_PER_TRANSFER]
            try:
                resp = await asyncio.to_thread(
                    self.wallet.dust_transfer, asset=",".join(chunk))
            except Exception as exc:
                logger.error(f"Dust sweep: transfer gagal "
                             f"({', '.join(chunk)}): {exc}")
                continue
            for r in _to_plain(resp).get("transferResult") or []:
                transferred.append(str(r.get("fromAsset", "?")))
                bnb_received += float(r.get("transferedAmount", 0) or 0)

        # --- 4. catat hasil ---
        if transferred:
            msg = (f"Dust sweep: {len(transferred)} aset dikonversi ke BNB "
                   f"({', '.join(transferred)}), diterima {bnb_received:.8f} BNB")
            logger.info(msg)
            self.db.record_event("INFO", "DUST_SWEEP", msg)
            return {"assets": transferred, "bnb": bnb_received}
        return None

    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Loop latar: sweep pertama langsung, lalu tiap interval menit."""
        ds = self.cfg.dust_sweep
        interval = max(60, ds.interval_minutes * 60)
        logger.info(f"Dust sweep aktif: tiap {ds.interval_minutes} menit "
                    f"(dust < {ds.min_value_usd} {self.cfg.quote_asset} "
                    f"-> BNB)")
        while True:
            try:
                await self.sweep_once()
            except Exception as exc:       # jangan biarkan loop mati
                logger.error(f"Dust sweep error tak terduga: {exc}")
            await asyncio.sleep(interval)
