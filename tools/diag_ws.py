#!/usr/bin/env python3
"""
Diagnosis koneksi WebSocket market-stream Binance dari jaringan ANDA.

Latar: bot live berlangganan 4 stream per simbol (kline/aggTrade/depth/
ticker) via wss://stream.binance.com:9443. Kalau jaringan/ISP memblokir
port non-standar (9443), koneksi WS diam-diam mati -> orderbook tidak
pernah terisi -> semua simbol kena flag GATE selamanya, padahal REST
(universe refresh, harga seed) tetap normal.

Alat ini MENGUJI beberapa endpoint sekaligus (tanpa API key, stream
publik), menghitung pesan yang benar-benar diterima per jenis stream
selama ~18 detik, lalu mencetak vonis + saran perbaikan.

Pemakaian (di mesin Anda, venv bot aktif):
    python tools/diag_ws.py
"""

from __future__ import annotations

import asyncio
import sys

# Endpoint yang diuji, urutan penting (kontrol dulu, kandidat fix belakangan)
TARGETS = [
    ("LIVE  :9443  (dipakai bot sekarang)", "wss://stream.binance.com:9443"),
    ("LIVE  :443   (port standar, kandidat fix)",
     "wss://stream.binance.com:443"),
    ("MIRROR market-data (kandidat fix)",
     "wss://data-stream.binance.vision"),
    ("TESTNET :443 (kontrol - dulu jalan di Anda)",
     "wss://stream.testnet.binance.vision"),
]

SYMBOLS = ["btcusdt", "ethusdt"]
DURATION_S = 18          # lama pengumpulan pesan per endpoint
CONNECT_TIMEOUT_S = 12   # batas waktu connect (port diblokir = hang)


def _parse_kline(m):  _ = m.k
def _parse_trade(m):  _ = m.p, m.q
def _parse_book(m):   _ = (m.bids or [])[0], (m.asks or [])[0]
def _parse_ticker(m): _ = m.c, m.h, m.l


async def test_endpoint(label: str, url: str) -> dict:
    """Connect + langgan 4 stream x 2 simbol -> hitung & parse pesan."""
    from binance_sdk_spot import Spot
    from binance_common.configuration import ConfigurationWebSocketStreams

    result = {"label": label, "url": url, "error": None,
              "counts": {}, "parse": {}}
    counts = {f"{s}:{k}": 0 for s in SYMBOLS
              for k in ("kline", "trade", "book", "ticker")}
    parse = {}
    handles = []
    client = None

    def cb_factory(sym, kind, parse_fn):
        def cb(m):
            counts[f"{sym}:{kind}"] += 1
            if f"{sym}:{kind}" not in parse:
                try:
                    parse_fn(m)
                    parse[f"{sym}:{kind}"] = True
                except Exception as exc:
                    parse[f"{sym}:{kind}"] = f"{type(exc).__name__}: {exc}"
        return cb

    try:
        client = Spot(config_ws_streams=ConfigurationWebSocketStreams(
            stream_url=url))
        streams = client.websocket_streams
        await asyncio.wait_for(streams.create_connection(),
                               timeout=CONNECT_TIMEOUT_S)

        for sym in SYMBOLS:
            h = await streams.kline(symbol=sym, interval="1m")
            h.on("message", cb_factory(sym, "kline", _parse_kline))
            handles.append(h)
            h = await streams.agg_trade(symbol=sym)
            h.on("message", cb_factory(sym, "trade", _parse_trade))
            handles.append(h)
            h = await streams.partial_book_depth(symbol=sym, levels=20)
            h.on("message", cb_factory(sym, "book", _parse_book))
            handles.append(h)
            h = await streams.ticker(symbol=sym)
            h.on("message", cb_factory(sym, "ticker", _parse_ticker))
            handles.append(h)

        await asyncio.sleep(DURATION_S)
    except asyncio.TimeoutError:
        result["error"] = (f"TIMEOUT {CONNECT_TIMEOUT_S}s saat connect "
                           f"(kemungkinan port diblokir jaringan)")
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if handles:
            for h in handles:
                try:
                    await h.unsubscribe()
                except Exception:
                    pass
        if client is not None:
            try:
                await client.websocket_streams.close_connection(
                    close_session=True)
            except Exception:
                pass

    result["counts"] = counts
    result["parse"] = parse
    return result


def print_result(r: dict) -> None:
    print(f"\n--- {r['label']} ---")
    print(f"    {r['url']}")
    if r["error"]:
        print(f"    !! GAGAL: {r['error']}")
        return
    total = sum(r["counts"].values())
    if total == 0:
        print("    !! TERHUBUNG tapi 0 pesan diterima "
              "(koneksi dibuka lalu dibungkam / diblokir)")
        return
    for key in sorted(r["counts"]):
        n = r["counts"][key]
        p = r["parse"].get(key)
        ptxt = "OK" if p is True else (f"PARSE GAGAL {p}" if p else "-")
        print(f"    {key:16s}: {n:5d} pesan  (parse: {ptxt})")


def main() -> int:
    print("Diagnosis WebSocket Binance market-stream")
    print(f"Durasi uji per endpoint: {DURATION_S}s x {len(TARGETS)} endpoint "
          f"(total ~{DURATION_S * len(TARGETS) + 15}s)\n")
    results = []
    for label, url in TARGETS:
        print(f"menguji: {label} ...", flush=True)
        results.append(asyncio.run(test_endpoint(label, url)))

    print("\n" + "=" * 62)
    print("VONIS")
    print("=" * 62)
    for r in results:
        print_result(r)

    print("\n" + "=" * 62)
    print("CARA MEMBACA")
    print("=" * 62)
    print("""- LIVE :9443 gagal/0 pesan, tapi :443 atau MIRROR menerima pesan
  -> jaringan Anda memblokir port 9443. Solusi: ganti URL WS bot ke
     port 443 / mirror (kirim hasil ini ke saya, perubahannya kecil).
- Semua endpoint LIVE gagal, TESTNET jalan
  -> pemblokiran per-domain oleh ISP/proxy; mirror biasanya lolos.
- Semua endpoint menerima pesan tapi parse GAGAL
  -> kirim output ini (ada perubahan format dari sisi Binance/SDK).
- Semua endpoint jalan normal
  -> masalah bukan jaringan; kirim log bot (logs/pumpbot.log).""")
    ok = 0
    for r in results:
        if not r["error"] and sum(r["counts"].values()) > 0:
            ok += 1
    print(f"\nEndpoint sehat: {ok}/{len(results)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
