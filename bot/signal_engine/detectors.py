"""
Detector sinyal - masing-masing menganalisis satu aspek pasar dan
menghasilkan skor 0-100 (semakin tinggi = semakin mendukung pump).

Kontrak tiap detector:  score(buf, cfg) -> DetectorResult
  * score       : 0..100
  * eligible    : False = gate keras (data kurang / spread kelewat lebar)
  * veto        : True  = larang entry (manipulasi jelas)
  * details     : angka mentah untuk log & dashboard

Skor akhir digabung di engine.py (rata-rata tertimbang - penalti manipulasi).
"""

from __future__ import annotations

import statistics
from typing import Optional

from bot.config import Config
from bot.data_collector.buffers import SymbolBuffer
from bot.models import DetectorResult
from bot.utils import clamp, clamp01, now_ms


# ============================================================================
# 1. DETECTOR ORDER BOOK
#    Rasio bid/ask berbobot kedekatan + deteksi wall (support/resistance)
# ============================================================================

class OrderBookDetector:
    name = "orderbook"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        book = buf.book
        if not book or not book.bids or not book.asks:
            return DetectorResult(self.name, eligible=False, details={"reason": "book kosong"})

        c = cfg.signal.orderbook
        n = c.imbalance_levels
        mid = book.mid or 0.0
        if mid <= 0:
            return DetectorResult(self.name, eligible=False, details={"reason": "mid invalid"})

        # --- rasio bid/ask berbobot kedekatan level ---
        # Level 1 (paling dekat harga pasar) diberi bobot penuh, level ke-i
        # diberi bobot 1/(1+i) karena semakin jauh semakin kurang relevan.
        bid_sum = sum(q / (1 + i) for i, (p, q) in enumerate(book.bids[:n]))
        ask_sum = sum(q / (1 + i) for i, (p, q) in enumerate(book.asks[:n]))
        total = bid_sum + ask_sum
        if total <= 0:
            return DetectorResult(self.name, eligible=False, details={"reason": "book kosong"})
        ratio = bid_sum / total          # 0..1, 0.5 = seimbang
        # ratio 0.5+scale -> skor 100 (skala default 0.25 -> ratio 0.75 penuh)
        base = clamp01((ratio - 0.5) / max(c.imbalance_scale, 1e-9)) * 100

        # --- deteksi wall ---
        # Wall = order di satu level dengan qty >= wall_ratio x median level.
        # Bid wall dekat harga = support kuat (bullish).
        # Ask wall dekat harga = resistance berat (bearish / tanda distribution).
        all_qtys = [q for _, q in book.bids[:n]] + [q for _, q in book.asks[:n]]
        med = statistics.median(all_qtys) if all_qtys else 0.0
        wall_thr = med * c.wall_ratio
        near = c.wall_near_pct / 100.0

        bid_wall_qty = bid_wall_dist = ask_wall_qty = ask_wall_dist = 0.0
        if wall_thr > 0:
            for p, q in book.bids[:n]:
                if q >= wall_thr:
                    bid_wall_qty, bid_wall_dist = q, (mid - p) / mid
                    break
            for p, q in book.asks[:n]:
                if q >= wall_thr:
                    ask_wall_qty, ask_wall_dist = q, (p - mid) / mid
                    break

        wall_adjust = 0.0
        if bid_wall_qty > 0 and bid_wall_dist <= near:
            wall_adjust += 15.0        # support kuat di bawah harga
        if ask_wall_qty > 0 and ask_wall_dist <= near:
            wall_adjust -= 25.0        # resistance berat tepat di atas harga

        final = clamp(base + wall_adjust, 0.0, 100.0)
        return DetectorResult(self.name, score=final, details={
            "bid_ask_ratio": round(ratio, 4),
            "bid_wall": round(bid_wall_qty, 2), "ask_wall": round(ask_wall_qty, 2),
        })


# ============================================================================
# 2. DETECTOR FREKUENSI TRADE & LIKUIDITAS
# ============================================================================

class TradeFlowDetector:
    name = "trade_flow"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        c = cfg.signal.trade_flow
        now = now_ms()

        # --- gate likuiditas: spread bid-ask ---
        # Spread lebar = eksekusi mahal & slippage tak terkendali -> jangan entry.
        if buf.book is None or buf.book.spread_bps is None:
            return DetectorResult(self.name, eligible=False, details={"reason": "book kosong"})
        spread_bps = buf.book.spread_bps
        if spread_bps > c.max_spread_bps:
            return DetectorResult(self.name, eligible=False, details={
                "reason": f"spread {spread_bps:.1f} bps > {c.max_spread_bps} bps",
                "spread_bps": round(spread_bps, 2)})

        # --- lonjakan jumlah trade per menit vs baseline historis ---
        tpm_now = len(buf.trades_since(now - 60_000))
        # baseline = rata-rata trade per menit dari candle yang sudah close
        hist = list(buf.candles)[-c.baseline_minutes:]
        base_per_min = statistics.fmean([cd.trades for cd in hist]) if hist else 0.0
        base_per_min = max(base_per_min, 0.5)
        spike_ratio = tpm_now / base_per_min
        tpm_score = clamp01((spike_ratio - 1.0) / max(c.spike_scale, 1e-9)) * 100

        # --- tekanan beli (taker buy vs taker sell, window 2 menit) ---
        recent = buf.trades_since(now - 120_000)
        buy_vol = sum(t.qty for t in recent if not t.buyer_is_maker)
        sell_vol = sum(t.qty for t in recent if t.buyer_is_maker)
        tot = buy_vol + sell_vol
        buy_ratio = (buy_vol / tot) if tot > 0 else 0.5
        bp_score = clamp01((buy_ratio - 0.5) / 0.25) * 100

        final = 0.6 * tpm_score + 0.4 * bp_score
        return DetectorResult(self.name, score=final, details={
            "trades_per_min": tpm_now, "tpm_baseline": round(base_per_min, 1),
            "tpm_ratio": round(spike_ratio, 2),
            "buy_ratio": round(buy_ratio, 3),
            "spread_bps": round(spread_bps, 2),
        })


# ============================================================================
# 3. DETECTOR VOLUME
# ============================================================================

class VolumeDetector:
    name = "volume"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        c = cfg.signal.volume
        candles = list(buf.candles)
        if len(candles) < c.ma_period + 1:
            return DetectorResult(self.name, eligible=False,
                                  details={"reason": "candle kurang"})

        last = candles[-1]
        prev = candles[-(c.ma_period + 1):-1]
        ma = statistics.fmean([cd.volume for cd in prev])
        if ma <= 0:
            return DetectorResult(self.name, eligible=False, details={"reason": "MA=0"})

        # Spike volume candle terakhir yang sudah close...
        spike_closed = last.volume / ma
        # ...dan volume menit BERJALAN (deteksi dini sebelum candle close):
        # jumlahkan trade sejak menit ini dimulai.
        now = now_ms()
        bucket_start = now // 60_000 * 60_000
        running = sum(t.qty for t in buf.trades_since(max(
            bucket_start, last.close_time + 1)))
        spike_running = running / ma

        spike = max(spike_closed, spike_running)
        final = clamp01((spike - 1.0) / max(c.spike_scale, 1e-9)) * 100
        return DetectorResult(self.name, score=final, details={
            "volume_spike": round(spike, 2),
            "spike_closed": round(spike_closed, 2),
            "spike_running": round(spike_running, 2),
            "vol_ma": round(ma, 4),
        })


# ============================================================================
# 4. DETECTOR BANDAR / WHALE
# ============================================================================

class WhaleDetector:
    name = "whale"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        c = cfg.signal.whale
        now = now_ms()

        # --- transaksi tunggal bernilai besar (whale) ---
        # Rata-rata ukuran trade dihitung dari SELURUH buffer trade
        # (ukurannya = data.trade_window_sec, default 10 menit), lalu dicari
        # trade dengan notional >= multiplier x rata-rata (dan >= min USD).
        ref = list(buf.trades)
        if len(ref) < 20:
            return DetectorResult(self.name, eligible=False,
                                  details={"reason": "trade kurang"})
        notionals = [t.price * t.qty for t in ref]
        avg_notional = statistics.fmean(notionals)

        window = buf.trades_since(now - c.window_min * 60_000)
        thr = max(avg_notional * c.multiplier, c.min_notional_usd)
        whale_buy = sum(t.price * t.qty for t in window
                        if t.price * t.qty >= thr and not t.buyer_is_maker)
        whale_sell = sum(t.price * t.qty for t in window
                         if t.price * t.qty >= thr and t.buyer_is_maker)
        net = whale_buy - whale_sell
        whale_score = clamp01(net / max(c.net_normalize_usd, 1e-9)) * 100

        # --- deteksi spoofing (wall palsu) ---
        # Bandingkan snapshot order book sekarang vs ~6 detik lalu:
        # order besar yang MENGHILANG tanpa tersentuh harga = kemungkinan spoof.
        #   - ask wall hilang  -> penjual palsu -> justru bullish
        #   - bid wall hilang  -> pembeli palsu -> bearish
        spoof_bull = spoof_bear = 0
        hist = list(buf.book_history)
        if len(hist) >= 2 and hist[-1].bids and hist[-1].asks:
            new = hist[-1]
            old = None
            for snap in reversed(hist[:-1]):
                if new.ts - snap.ts >= 5000:
                    old = snap
                    break
            if old and old.bids and old.asks:
                med_old = statistics.median(
                    [q for _, q in old.bids] + [q for _, q in old.asks])
                spoof_thr = med_old * c.spoof_wall_ratio
                if spoof_thr > 0:
                    new_bid = {round(p, 10): q for p, q in new.bids}
                    new_ask = {round(p, 10): q for p, q in new.asks}
                    for p, q in old.asks:
                        now_q = new_ask.get(round(p, 10), 0.0)
                        if q >= spoof_thr and now_q < q * (1 - c.spoof_drop_pct):
                            spoof_bull += 1
                    for p, q in old.bids:
                        now_q = new_bid.get(round(p, 10), 0.0)
                        if q >= spoof_thr and now_q < q * (1 - c.spoof_drop_pct):
                            spoof_bear += 1

        spoof_score = clamp01((spoof_bull - spoof_bear) / 2.0) * 100
        final = clamp(0.6 * whale_score + 0.4 * spoof_score, 0.0, 100.0)
        return DetectorResult(self.name, score=final, details={
            "whale_buy_usd": round(whale_buy), "whale_sell_usd": round(whale_sell),
            "whale_net_usd": round(net), "avg_trade_usd": round(avg_notional, 2),
            "spoof_bull": spoof_bull, "spoof_bear": spoof_bear,
        })


# ============================================================================
# 5. DETECTOR MANIPULASI (skor TINGGI = berbahaya -> penalti / veto)
# ============================================================================

class ManipulationDetector:
    name = "manipulation"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        c = cfg.signal.manipulation
        candles = list(buf.candles)
        need = max(c.pump_dump_window_min, 12)
        if len(candles) < need or buf.ticker is None:
            return DetectorResult(self.name, eligible=False,
                                  details={"reason": "data kurang"})
        closes = [cd.close for cd in candles]
        highs = [cd.high for cd in candles]
        lows = [cd.low for cd in candles]
        vols = [cd.volume for cd in candles]
        last_close = closes[-1]

        # --- 1) pergerakan 24 jam ekstrem (sudah pump besar / crash) ---
        chg24 = abs(buf.ticker.price_change_pct)
        c1 = clamp01(chg24 / max(c.max_change_24h_pct, 1e-9)) * 100 \
            if chg24 >= c.max_change_24h_pct else (chg24 / c.max_change_24h_pct) * 70

        # --- 2) pola pump & dump ---
        # Naik tajam dalam window lalu sudah menarik diri (pullback)
        # -> kita terlambat, risiko jadi bag holder.
        win = candles[-c.pump_dump_window_min:]
        w_highs = [cd.high for cd in win]
        w_lows = [cd.low for cd in win]
        i_max = w_highs.index(max(w_highs))
        leg_low = min(w_lows[: i_max + 1]) if i_max > 0 else min(w_lows)
        runup = (max(w_highs) - leg_low) / leg_low * 100 if leg_low > 0 else 0.0
        pullback = ((max(w_highs) - last_close) / (max(w_highs) - leg_low) * 100
                    if max(w_highs) > leg_low else 0.0)
        c2 = 0.0
        if runup >= c.pump_dump_gain_pct and pullback >= c.pump_dump_pullback_pct:
            c2 = min(100.0, 40.0 + runup * 5)

        # --- 3) overextended 5 menit (jangan kejar puncak) ---
        ref5 = closes[-6] if len(closes) >= 6 else closes[0]
        chg5 = (last_close - ref5) / ref5 * 100 if ref5 > 0 else 0.0
        c3 = clamp01(chg5 / max(c.overextended_5m_pct, 1e-9)) * 100 if chg5 > 0 else 0.0

        # --- 4) wash trading ---
        # Volume tinggi tapi harga nyaris tidak bergerak -> kemungkinan besar
        # wash trading / fake volume, bukan minat pasar sungguhan.
        last10 = candles[-10:]
        prev20 = candles[-30:-10] if len(candles) >= 30 else []
        prev_avg = (statistics.fmean([cd.volume for cd in prev20])
                    if prev20 else 0.0)
        # Guard: pair sepi bisa punya rentetan candle volume 0 -> baseline 0.
        # Pembagian dengan 0 harus dicegah; baseline 0 berarti "tidak terukur",
        # jadi anggap netral (1.0), konsisten dengan konvensi prev20 kosong.
        vol_spike = (statistics.fmean([cd.volume for cd in last10]) /
                     prev_avg) if prev_avg > 0 else 1.0
        rng = ((max(cd.high for cd in last10) - min(cd.low for cd in last10))
               / last_close * 100) if last_close > 0 else 0.0
        c4 = 100.0 if (vol_spike >= c.wash_volume_spike and rng <= c.wash_range_pct) else 0.0

        # gabungan: komponen paruh jadi sudah cukup mencurigakan
        manip = clamp(0.40 * c1 + 0.45 * c2 + 0.25 * c3 + 0.30 * c4, 0.0, 100.0)
        veto = (manip / 100.0) >= c.veto_threshold

        return DetectorResult(self.name, score=manip, veto=veto, details={
            "chg24": round(chg24, 2), "runup": round(runup, 2),
            "pullback": round(pullback, 2), "chg5m": round(chg5, 2),
            "vol_spike10": round(vol_spike, 2), "range10": round(rng, 3),
            "components": {"c24h": round(c1), "pump_dump": round(c2),
                           "overext": round(c3), "wash": round(c4)},
        })


# ============================================================================
# 6. DETECTOR PRICE ACTION & RETRACEMENT (sinyal utama)
# ============================================================================

class PriceActionDetector:
    name = "price_action"

    def score(self, buf: SymbolBuffer, cfg: Config) -> DetectorResult:
        c = cfg.signal.price_action
        candles = list(buf.candles)
        if len(candles) < max(c.structure_candles, c.breakout_lookback + 2, 15):
            return DetectorResult(self.name, eligible=False,
                                  details={"reason": "candle kurang"})

        window = candles[-c.structure_candles:]
        closes = [cd.close for cd in window]
        highs = [cd.high for cd in window]
        lows = [cd.low for cd in window]
        vols = [cd.volume for cd in window]
        last_close = closes[-1]

        # --- struktur tren: higher-high & higher-low via swing fractal ---
        # Swing high = candle yang high-nya tertinggi di k x tetangga kiri-kanan.
        k = c.swing_neighbors
        n = len(window)
        swing_h = [i for i in range(k, n - k)
                   if highs[i] == max(highs[i - k: i + k + 1])]
        swing_l = [i for i in range(k, n - k)
                   if lows[i] == min(lows[i - k: i + k + 1])]
        structure = 0.0
        if len(swing_h) >= 2 and len(swing_l) >= 2:
            hh = highs[swing_h[-1]] > highs[swing_h[-2]]
            hl = lows[swing_l[-1]] > lows[swing_l[-2]]
            if hh and hl:
                structure = 30.0     # uptrend sehat (HH + HL)
            elif hh or hl:
                structure = 15.0

        # --- breakout resistance + konfirmasi volume (sinyal entry utama) ---
        look = candles[-(c.breakout_lookback + 1):-1]
        prev_high = max(cd.high for cd in look)
        vol_ma = statistics.fmean([cd.volume for cd in look])
        last_vol = vols[-1]
        vol_ok = vol_ma > 0 and last_vol >= c.breakout_vol_confirm * vol_ma
        breakout = last_close > prev_high
        breakout_score = 40.0 if (breakout and vol_ok) else (15.0 if breakout else 0.0)

        # --- retracement fibonacci dari kaki impulsif terakhir ---
        # Cari swing low terendah di window dan high tertinggi SETELAHNYA,
        # lalu ukur sudah sejauh mana harga menarik diri dari puncak.
        # Entry paling aman: zona 38.2% - 61.8% (golden zone).
        i_low = lows.index(min(lows))
        leg_high = max(highs[i_low:])
        leg = leg_high - min(lows)
        retr = ((leg_high - last_close) / leg) if leg > 0 else 0.0
        fib_lo, fib_hi = sorted(c.fib_zone)
        if fib_lo <= retr <= fib_hi:
            fib_score = 30.0            # di golden zone: area entry aman
        elif retr < fib_lo:
            # harga nyaris di puncak: bagus kalau breakout terkonfirmasi,
            # buruk kalau cuma ngejar harga (penalti di bawah)
            fib_score = 12.0
        else:
            fib_score = 0.0             # retracement terlalu dalam (>61.8%)

        # --- penalti kalau menempel puncak range tanpa breakout ---
        rng = max(highs) - min(lows)
        pos_in_range = ((last_close - min(lows)) / rng) if rng > 0 else 0.5
        penalty = 10.0 if (pos_in_range >= c.near_top_pct and not breakout) else 0.0

        # --- usulan stop loss dari struktur: di bawah swing low terakhir ---
        if swing_l:
            stop_ref = lows[swing_l[-1]]
        else:
            stop_ref = min(lows[-5:])
        suggested_stop = stop_ref * (1.0 - cfg.stops.structure_buffer_pct / 100.0)

        final = clamp(structure + breakout_score + fib_score - penalty, 0.0, 100.0)
        return DetectorResult(self.name, score=final, details={
            "structure": structure, "breakout": breakout, "vol_confirm": vol_ok,
            "fib_retr": round(retr, 3), "pos_in_range": round(pos_in_range, 3),
            "swing_stop": suggested_stop,
            "entry_type": "breakout" if breakout else "pullback",
        })


ALL_DETECTORS = {
    "orderbook": OrderBookDetector,
    "trade_flow": TradeFlowDetector,
    "volume": VolumeDetector,
    "whale": WhaleDetector,
    "manipulation": ManipulationDetector,
    "price_action": PriceActionDetector,
}
