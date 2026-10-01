"""HBase table `surge_multipliers`: the current surge price of every zone (millisecond reads).

row key      zone_id#minute   e.g. 132#202609291145   (zone zero-padded to 3 digits so keys sort correctly)
column fam   s  :  multiplier, demand, supply, ratio, rain_mm, temp_c, updated_at
access       latest value for a zone  = reverse prefix scan on `zone#`
             last hour of a zone      = range scan
retention    TTL 7 days (long-term history: Kafka surge_updates -> Databricks Gold zone_minute_surge)

    python -m streaming.hbase_table                 # create the table
    python -m streaming.hbase_table --latest 132    # latest multiplier of zone 132
    python -m streaming.hbase_table --history 132   # last 60 minutes of zone 132
    python -m streaming.hbase_table --refresh       # when the multipliers were last refreshed (SLA: every 60 s)
"""
import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import happybase

from common.config import HBASE_HOST, HBASE_PORT, HBASE_TABLE


def connect(retries: int = 40) -> happybase.Connection:
    last = None
    for _ in range(retries):
        try:
            conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=20000)
            conn.tables()
            return conn
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(3)
    raise RuntimeError(f"HBase Thrift not reachable: {last}")


def create_table(conn) -> None:
    names = [t.decode() for t in conn.tables()]
    if HBASE_TABLE in names:
        print(f"[hbase] table {HBASE_TABLE} already exists")
        return
    conn.create_table(HBASE_TABLE, {"s": dict(max_versions=1, time_to_live=7 * 24 * 3600)})
    print(f"[hbase] created {HBASE_TABLE} (cf=s, TTL=7d)")


def latest(conn, zone: int):
    t = conn.table(HBASE_TABLE)
    for key, data in t.scan(row_start=f"{zone:03d}#\xff".encode(), row_stop=f"{zone:03d}#".encode(), reverse=True, limit=1):
        return key.decode(), {k.decode(): v.decode() for k, v in data.items()}
    return None


def history(conn, zone: int, minutes: int = 60):
    t = conn.table(HBASE_TABLE)
    rows = list(t.scan(row_start=f"{zone:03d}#".encode(), row_stop=f"{zone:03d}#\xff".encode(),
                       reverse=True, limit=minutes))
    return [(k.decode(), {c.decode(): v.decode() for c, v in d.items()}) for k, d in rows]


def latest_per_zone(conn, zones=range(1, 264)) -> Dict[int, dict]:
    """Newest row of every zone: {zone: {"key": row key, "updated_at": ISO UTC, "multiplier": float}}."""
    t = conn.table(HBASE_TABLE)
    out = {}
    for z in zones:
        for key, data in t.scan(row_start=f"{z:03d}#\xff".encode(), row_stop=f"{z:03d}#".encode(), reverse=True, limit=1,
                                columns=[b"s:updated_at", b"s:multiplier"]):
            out[z] = {"key": key.decode(), "updated_at": data.get(b"s:updated_at", b"").decode(),
                      "multiplier": float(data.get(b"s:multiplier", b"1") or 1)}
    return out


def refresh_summary(rows: Dict[int, dict], now: Optional[datetime] = None, sla_sec: int = 60) -> dict:
    """Last refresh of the surge table: newest updated_at, its age, zones written in that refresh, SLA verdict.
    A refresh writes every active zone with the same updated_at (one micro-batch per minute)."""
    now = now or datetime.now(timezone.utc)
    stamps = [r["updated_at"] for r in rows.values() if r.get("updated_at")]
    if not stamps:
        return {"last_refresh_utc": None, "age_sec": None, "zones_in_last_refresh": 0, "zones_total": len(rows),
                "surging_zones": 0, "sla_sec": sla_sec, "ok": False}
    last = max(stamps)
    age = (now - datetime.fromisoformat(last.replace("Z", "+00:00"))).total_seconds()
    in_last = [r for r in rows.values() if r.get("updated_at") == last]
    return {"last_refresh_utc": last, "age_sec": round(age, 1), "zones_in_last_refresh": len(in_last),
            "zones_total": len(rows), "surging_zones": sum(1 for r in in_last if r["multiplier"] > 1.0),
            "sla_sec": sla_sec, "ok": age <= 2 * sla_sec}     # one trigger interval + processing time


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--latest", type=int)
    ap.add_argument("--history", type=int)
    ap.add_argument("--refresh", action="store_true")
    a = ap.parse_args()
    c = connect()
    if a.latest:
        print(latest(c, a.latest))
    elif a.history:
        for k, d in history(c, a.history):
            print(k, d.get("s:multiplier"), d.get("s:demand"), d.get("s:supply"))
    elif a.refresh:
        r = refresh_summary(latest_per_zone(c))
        if r["last_refresh_utc"] is None:
            print("[hbase] no surge rows yet - is rh-surge-stream running? (make logs)")
        else:
            print(f"[hbase] last refresh {r['last_refresh_utc']} ({r['age_sec']:.0f} s ago): "
                  f"{r['zones_in_last_refresh']} zones written, {r['surging_zones']} surging (> 1.0x); "
                  f"zones with data: {r['zones_total']}")
            print(f"[hbase] refresh SLA every {r['sla_sec']} s: {'OK' if r['ok'] else 'LATE'}")
    else:
        create_table(c)
    sys.exit(0)
