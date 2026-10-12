"""Live data for the operations dashboard (http://localhost:8501), read straight from the Kafka stream.

The Gold tables are refreshed every hour; the dashboard shows what is happening *now*, so it reads the last minutes of
the Kafka topics directly (read-only: a throw-away consumer group, nothing is committed):
  trip_requests      ride requests (riders asking for a car)          -> live demand, requests per minute
  trip_matches       dispatch results of the fleet simulator          -> live trip feed, ETA, fare, matched / no driver
  driver_gps_pings   driver positions + status (source = driver)      -> drivers online / available / on a trip, map
  surge_updates      multipliers published by the surge engine        -> current surge per zone, top zones, map

`read_recent()` talks to Kafka; the `summarise_*` functions turn the events into the numbers on the page.
All times shown are New York local time, like the Gold tables.
"""
import csv
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Dict, Iterable, List, Optional

from common.config import (KAFKA_BOOTSTRAP, TLC_DIR, TOPIC_GPS, TOPIC_MATCHES, TOPIC_SURGE, TOPIC_TRIPS,
                           ZONE_GRID_PATH, env_int, to_ny_naive)

FLEET_SIZE = env_int("FLEET_SIZE", 700)          # Python simulator drivers 1..700, Scala producer drivers after that


# ------------------------------------------------------------------ zones
@lru_cache(maxsize=1)
def zone_names() -> Dict[int, str]:
    path = f"{TLC_DIR}/taxi_zone_lookup.csv"
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as fh:
        return {int(r["LocationID"]): f'{r["Zone"]} ({r["Borough"]})' for r in csv.DictReader(fh)}


@lru_cache(maxsize=1)
def zone_centres() -> Dict[int, tuple]:
    if not os.path.exists(ZONE_GRID_PATH):
        return {}
    return {int(k): tuple(v) for k, v in json.load(open(ZONE_GRID_PATH))["centroids"].items()}


def zone_label(z) -> str:
    try:
        return zone_names().get(int(z), f"zone {int(z)}")
    except (TypeError, ValueError):
        return "-"


# ------------------------------------------------------------------ Kafka
def read_recent(topics: Iterable[str], seconds: int, max_messages: int = 60000, timeout_s: float = 6.0,
                bootstrap: str = KAFKA_BOOTSTRAP) -> Dict[str, List[dict]]:
    """Messages of the last `seconds` from each topic, oldest first: {topic: [json event, ...]}.
    Seeks every partition to `now - seconds` with offsetsForTimes and reads up to the current end."""
    from confluent_kafka import Consumer, TopicPartition
    topics = list(topics)
    out: Dict[str, List[dict]] = {t: [] for t in topics}
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"ops-dashboard-{uuid.uuid4()}",
                  "enable.auto.commit": False, "auto.offset.reset": "latest", "socket.timeout.ms": 5000})
    try:
        md = c.list_topics(timeout=5)
        since_ms = int((time.time() - seconds) * 1000)
        want = [TopicPartition(t, p, since_ms) for t in topics if t in md.topics for p in md.topics[t].partitions]
        starts = c.offsets_for_times(want, timeout=5) if want else []
        assign, end = [], {}
        for tp in starts:
            if tp.offset < 0:                                       # nothing newer than `since` in this partition
                continue
            hi = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)[1]
            if hi > tp.offset:
                assign.append(tp)
                end[(tp.topic, tp.partition)] = hi - 1
        if not assign:
            return out
        c.assign(assign)
        left, n, t0 = set(end), 0, time.monotonic()
        while left and n < max_messages and time.monotonic() - t0 < timeout_s:
            for m in c.consume(num_messages=2000, timeout=0.5):
                if m.error():
                    continue
                key = (m.topic(), m.partition())
                if m.offset() >= end.get(key, -1):
                    left.discard(key)
                try:
                    out[m.topic()].append(json.loads(m.value()))
                    n += 1
                except (TypeError, ValueError):
                    pass
    finally:
        c.close()
    for evs in out.values():
        evs.sort(key=lambda e: e.get("event_time") or "")
    return out


def snapshot(seconds_trips: int = 1800, seconds_gps: int = 90, seconds_surge: int = 150) -> dict:
    """One read of everything the dashboard needs (three Kafka reads with different look-back windows)."""
    trips = read_recent([TOPIC_TRIPS, TOPIC_MATCHES], seconds_trips)
    gps = read_recent([TOPIC_GPS], seconds_gps)
    surge = read_recent([TOPIC_SURGE], seconds_surge)
    return {"requests": trips[TOPIC_TRIPS], "matches": trips[TOPIC_MATCHES], "pings": gps[TOPIC_GPS],
            "surge": surge[TOPIC_SURGE], "read_at": datetime.now(timezone.utc)}


# ------------------------------------------------------------------ pure summaries
def parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def ny(s: Optional[str]) -> Optional[datetime]:
    t = parse_ts(s)
    return to_ny_naive(t) if t else None


def summarise_drivers(pings: List[dict], now: datetime, online_sec: int = 90) -> dict:
    """Latest ping per driver (source = driver) within `online_sec` -> counts and positions."""
    latest: Dict[int, dict] = {}
    cutoff = now - timedelta(seconds=online_sec)
    for p in pings:
        if p.get("source") != "driver" or p.get("driver_id") is None:
            continue
        t = parse_ts(p.get("event_time"))
        if t is None or t < cutoff:
            continue
        d = int(p["driver_id"])
        if d not in latest or t >= latest[d]["_t"]:
            latest[d] = {**p, "_t": t}
    rows = [{"driver_id": d, "status": p.get("status"), "zone_id": p.get("zone_id"), "lat": p.get("lat"),
             "lon": p.get("lon"), "fleet": "Scala producer" if d > FLEET_SIZE else "Python simulator"}
            for d, p in sorted(latest.items())]
    avail = sum(1 for r in rows if r["status"] == "available")
    on_trip = sum(1 for r in rows if r["status"] == "on_trip")
    by_zone: Dict[int, int] = {}
    for r in rows:
        if r["status"] == "available" and r["zone_id"]:
            by_zone[int(r["zone_id"])] = by_zone.get(int(r["zone_id"]), 0) + 1
    return {"online": len(rows), "available": avail, "on_trip": on_trip,
            "utilisation_now": round(on_trip / len(rows), 4) if rows else None,
            "scala_online": sum(1 for r in rows if r["fleet"] == "Scala producer"),
            "available_by_zone": by_zone, "drivers": rows}


def summarise_surge(updates: List[dict]) -> dict:
    """Newest multiplier per zone from surge_updates (zones without an update are at 1.0x)."""
    latest: Dict[int, dict] = {}
    for u in updates:
        try:
            z = int(u["zone_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if z not in latest or (u.get("event_time") or "") >= (latest[z].get("event_time") or ""):
            latest[z] = u
    rows = [{"zone_id": z, "zone": zone_label(z), "multiplier": float(u.get("multiplier") or 1.0),
             "demand": u.get("demand"), "supply": u.get("supply"), "ratio": u.get("ratio"),
             "updated_ny": ny(u.get("event_time"))} for z, u in latest.items()]
    rows.sort(key=lambda r: (-r["multiplier"], -(r["ratio"] or 0)))
    mults = [r["multiplier"] for r in rows]
    stamps = [r["updated_ny"] for r in rows if r["updated_ny"]]
    return {"zones": rows, "surging": sum(1 for m in mults if m > 1.0), "max": max(mults) if mults else None,
            "avg": round(sum(mults) / len(mults), 3) if mults else None,
            "last_update_ny": max(stamps) if stamps else None,
            "rain_mm": next((u.get("rain_mm") for u in reversed(updates) if u.get("rain_mm") is not None), None),
            "temp_c": next((u.get("temp_c") for u in reversed(updates) if u.get("temp_c") is not None), None)}


def summarise_trips(requests: List[dict], matches: List[dict], now: datetime, window_min: int = 15,
                    feed_rows: int = 25) -> dict:
    """Live KPIs over the last `window_min` minutes, requests/matches per minute (whole read window) and the trip feed."""
    cut = now - timedelta(minutes=window_min)
    req_w = [r for r in requests if (parse_ts(r.get("event_time")) or now) >= cut]
    m_w = [m for m in matches if (parse_ts(m.get("event_time")) or now) >= cut]
    matched = [m for m in m_w if m.get("status") == "MATCHED"]
    unmatched = [m for m in m_w if m.get("status") == "UNMATCHED"]
    etas = [float(m["eta_min"]) for m in matched if m.get("eta_min") is not None]
    fares = [float(m["fare"]) for m in matched if m.get("fare") is not None]
    km = [float(m["distance_km"]) for m in matched if m.get("distance_km") is not None]
    surged = [m for m in matched if (m.get("surge_multiplier") or 1.0) > 1.0]

    per_min: Dict[datetime, Dict[str, int]] = {}
    for kind, evs in (("requests", requests), ("matched", [m for m in matches if m.get("status") == "MATCHED"]),
                      ("no driver", [m for m in matches if m.get("status") == "UNMATCHED"])):
        for e in evs:
            t = ny(e.get("event_time"))
            if t is None:
                continue
            k = t.replace(second=0, microsecond=0)
            per_min.setdefault(k, {"requests": 0, "matched": 0, "no driver": 0})[kind] += 1
    series = [{"minute": k, **v} for k, v in sorted(per_min.items())][-30:]

    feed = []
    for m in sorted(matches, key=lambda e: e.get("event_time") or "")[-feed_rows:][::-1]:     # newest first
        feed.append({"time (NYC)": ny(m.get("event_time")), "status": m.get("status"),
                     "pickup": zone_label(m.get("pickup_zone_id")), "drop-off": zone_label(m.get("dropoff_zone_id")),
                     "driver": m.get("driver_id"), "ETA min": m.get("eta_min"), "fare USD": m.get("fare"),
                     "km": m.get("distance_km"), "surge": m.get("surge_multiplier")})
    return {"window_min": window_min, "requests": len(req_w), "matched": len(matched), "no_driver": len(unmatched),
            "match_rate": round(len(matched) / len(m_w), 4) if m_w else None,
            "avg_eta_min": round(sum(etas) / len(etas), 2) if etas else None,
            "revenue": round(sum(fares), 2), "revenue_per_km": round(sum(fares) / sum(km), 2) if km and sum(km) else None,
            "surge_trips": len(surged), "per_minute": series, "feed": feed}
