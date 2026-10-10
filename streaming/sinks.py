"""Outputs of the surge engine, called once per minute:
  * HBase surge_multipliers   current price per zone (row key zone_id#minute) - what a rider app would read
  * Kafka surge_updates       the same values announced to listeners (Ops page, fleet simulator, raw archiver)

Exactly-once on restart: the Kafka offsets live in the Spark checkpoint, and the HBase row key zone_id#minute is
fixed, so a repeated batch overwrites the same cells instead of adding new ones.
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from common.config import (HBASE_HOST, HBASE_PORT, HBASE_TABLE, KAFKA_BOOTSTRAP, TOPIC_SURGE, env, env_float, iso_utc,
                           to_ny_naive)
from streaming.surge_rules import surge_multiplier

log = logging.getLogger("sinks")
WINDOW_MIN, SLIDE_MIN = 5, 1


def minute_key(zone_id: int, window_end_utc: datetime) -> str:
    """`132#202609291145` - zone zero-padded to 3 digits so keys sort correctly (NYC local minute)."""
    return f"{int(zone_id):03d}#{to_ny_naive(window_end_utc):%Y%m%d%H%M}"


def new_state() -> dict:
    return {"prev": {}, "rain": None, "temp": None}


def build_surge_records(rows: List[dict], state: dict, batch_id: int, now_utc: Optional[datetime] = None,
                        bike_weight: Optional[float] = None, rain_threshold: Optional[float] = None) -> List[dict]:
    """rows: updated (window, zone) aggregates of one micro-batch (zone 0 = city-level weather).

    The *current* window of a zone is the 5-minute window ending in the next minute boundary, i.e.
    window_end == max(window_end) - 4 min. The weather reading of that window is joined to every zone;
    if no reading arrived the last known one is used.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    bike_weight = env_float("CITYBIKES_SUPPLY_WEIGHT", 0.01) if bike_weight is None else bike_weight
    rain_threshold = env_float("RAIN_SURGE_THRESHOLD_MM", 2.5) if rain_threshold is None else rain_threshold
    zone_rows = [r for r in rows if int(r["zone"]) != 0]
    if not zone_rows:
        return []
    max_end = max(r["window_end"] for r in zone_rows)
    current_end = max_end - timedelta(minutes=WINDOW_MIN - SLIDE_MIN)

    # weather: reading of the current window, else the newest one in the batch, else last known
    wx = sorted((r for r in rows if int(r["zone"]) == 0 and r.get("rain_mm") is not None or
                 int(r["zone"]) == 0 and r.get("temp_c") is not None), key=lambda r: r["window_end"])
    cur_wx = [r for r in wx if r["window_end"] == current_end] or wx[-1:]
    if cur_wx:
        state["rain"], state["temp"] = cur_wx[-1].get("rain_mm"), cur_wx[-1].get("temp_c")
    rain, temp = state.get("rain"), state.get("temp")

    out, seen = [], set()
    for r in zone_rows:
        if r["window_end"] != current_end:
            continue
        z = int(r["zone"])
        seen.add(z)
        bikes = (r.get("bikes_sum") or 0) / max(r.get("bike_polls") or 0, 1)
        bike_supply = int(round(bikes * bike_weight))
        drivers = int(r.get("supply_drivers") or 0)
        supply, demand = drivers + bike_supply, int(r.get("demand") or 0)
        mult, ratio = surge_multiplier(demand, supply, rain, temp, state["prev"].get(z), rain_threshold)
        state["prev"][z] = mult
        out.append(_record(z, r["window_end"], demand, supply, drivers, bike_supply, ratio, mult, rain, temp,
                           now_utc, False, batch_id))

    # zones that went quiet: decay towards 1.0x at <= 0.3x per minute instead of staying stuck in HBase
    for z, prev in list(state["prev"].items()):
        if z in seen or prev <= 1.0:
            continue
        mult, ratio = surge_multiplier(0, 0, rain, temp, prev, rain_threshold)
        state["prev"][z] = mult
        out.append(_record(z, current_end, 0, 0, 0, 0, ratio, mult, rain, temp, now_utc, True, batch_id))
    return out


def _record(zone, window_end, demand, supply, drivers, bike_supply, ratio, mult, rain, temp, now, decayed, batch_id):
    return {
        "zone_id": zone,
        "window_start": window_end - timedelta(minutes=WINDOW_MIN),
        "window_end": window_end,
        "minute_key": minute_key(zone, window_end),
        "minute_local": to_ny_naive(window_end),
        "demand": demand, "supply": supply, "supply_drivers": drivers, "bike_supply": bike_supply,
        "ratio": ratio, "multiplier": mult, "rain_mm": rain, "temp_c": temp,
        "updated_at": now.replace(tzinfo=None), "decayed": decayed, "city": env("CITY", "NYC"), "batch_id": int(batch_id),
    }


# ------------------------------------------------------------------ HBase
def hbase_connection():
    import happybase
    return happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=20000)


def write_hbase(records: List[dict]) -> None:
    conn = hbase_connection()
    try:
        table = conn.table(HBASE_TABLE)
        with table.batch(batch_size=500) as b:
            for r in records:
                b.put(r["minute_key"].encode(), {
                    b"s:multiplier": str(r["multiplier"]).encode(),
                    b"s:demand": str(r["demand"]).encode(),
                    b"s:supply": str(r["supply"]).encode(),
                    b"s:ratio": str(r["ratio"]).encode(),
                    b"s:rain_mm": str(r["rain_mm"] if r["rain_mm"] is not None else "").encode(),
                    b"s:temp_c": str(r["temp_c"] if r["temp_c"] is not None else "").encode(),
                    b"s:updated_at": iso_utc(r["updated_at"].replace(tzinfo=timezone.utc)).encode(),
                })
    finally:
        conn.close()


def load_prev_multipliers(zones: range = range(1, 264)) -> Dict[int, float]:
    """Seed smoothing state from HBase after a restart (latest row per zone via reverse prefix scan)."""
    prev: Dict[int, float] = {}
    try:
        conn = hbase_connection()
        table = conn.table(HBASE_TABLE)
        for z in zones:
            for _, data in table.scan(row_start=f"{z:03d}#\xff".encode(), row_stop=f"{z:03d}#".encode(),
                                      reverse=True, limit=1, columns=[b"s:multiplier"]):
                prev[z] = float(data[b"s:multiplier"])
        conn.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("could not preload previous multipliers: %s", exc)
    return prev


# ------------------------------------------------------------------ Kafka
_producer = None


def publish_kafka(records: List[dict]) -> None:
    global _producer
    from confluent_kafka import Producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all", "enable.idempotence": True,
                              "client.id": "surge-engine"})
    for r in records:
        payload = {"event_id": r["minute_key"], "event_time": iso_utc(r["updated_at"].replace(tzinfo=timezone.utc)),
                   "zone_id": r["zone_id"], "multiplier": r["multiplier"], "demand": r["demand"],
                   "supply": r["supply"], "ratio": r["ratio"], "rain_mm": r["rain_mm"], "temp_c": r["temp_c"],
                   "minute_key": r["minute_key"], "decayed": r["decayed"],
                   "window_end": iso_utc(r["window_end"].replace(tzinfo=timezone.utc)), "city": r["city"]}
        _producer.produce(TOPIC_SURGE, key=str(r["zone_id"]), value=json.dumps(payload))
        _producer.poll(0)
    _producer.flush(30)
