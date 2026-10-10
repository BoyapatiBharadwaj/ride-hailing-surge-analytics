"""The 5 read-only tools of the GenAI Ops assistant.

The LLM never writes SQL. It picks one of these tools; each runs a fixed, parameterised query on the Gold tables in
Databricks (SQL warehouse), so the assistant can read curated data but cannot change anything.
"""
import csv
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from cloud.databricks.workspace import CATALOG, SCHEMA, query, table
from common.config import NY_TZ, TLC_DIR, databricks_configured

GOLD_TABLES = ["zone_minute_surge", "zone_hourly_kpis", "driver_daily_utilisation", "zone_daily_utilisation",
               "revenue_per_km", "demand_forecast", "driver_payouts"]
SOURCE = f"Databricks Gold {CATALOG}.{SCHEMA}"


def q(sql: str, params: List[Any] = ()) -> List[dict]:
    """{zone_minute_surge} etc. -> workspace.ridehail.zone_minute_surge; values are always bound parameters (?)."""
    if not databricks_configured():
        raise RuntimeError("the Databricks keys in .env are empty, so the Gold tables cannot be read")
    return query(sql.format(**{t: table(t) for t in GOLD_TABLES}), list(params))


# ------------------------------------------------------------------ helpers
def _zones() -> List[Dict[str, Any]]:
    path = f"{TLC_DIR}/taxi_zone_lookup.csv"
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return [{"zone_id": int(r["LocationID"]), "name": r["Zone"], "borough": r["Borough"]} for r in csv.DictReader(fh)]


def resolve_zone(zone: Any) -> List[Dict[str, Any]]:
    """Zone id or (partial) name -> matching zones, at most 8. 'Midtown' matches several taxi zones."""
    zs = _zones()
    try:
        zid = int(zone)
        return [z for z in zs if z["zone_id"] == zid] or [{"zone_id": zid, "name": f"zone {zid}", "borough": ""}]
    except (TypeError, ValueError):
        pass
    q = str(zone).strip().lower()
    hits = [z for z in zs if q in z["name"].lower()] or [z for z in zs if q in z["borough"].lower()]
    return hits[:8]


def _names(zs: List[dict]) -> Dict[int, str]:
    return {z["zone_id"]: z["name"] for z in zs}


def _day(value: Optional[str]) -> Optional[str]:
    if not value or value == "latest":
        return None
    v = value.strip().lower()
    today = datetime.now(NY_TZ).date()          # Gold dates are New York dates; the containers run in UTC
    if v == "today":
        return today.isoformat()
    if v == "yesterday":
        return (today - timedelta(days=1)).isoformat()
    return datetime.fromisoformat(v).date().isoformat()


def _in(ids: List[int]) -> str:
    return ",".join("?" for _ in ids)


# ------------------------------------------------------------------ tools
def get_current_surge(zone: Any) -> Dict[str, Any]:
    zs = resolve_zone(zone)
    if not zs:
        return {"error": f"no zone matches '{zone}'"}
    ids = [z["zone_id"] for z in zs]
    rows = q(f"""
        SELECT s.zone_id, s.minute_ts, s.multiplier, s.demand, s.supply, s.ratio, s.rain_mm, s.temp_c
        FROM {{zone_minute_surge}} s
        JOIN (SELECT zone_id, MAX(minute_ts) AS m FROM {{zone_minute_surge}} WHERE zone_id IN ({_in(ids)}) GROUP BY zone_id) l
          ON s.zone_id = l.zone_id AND s.minute_ts = l.m
        ORDER BY s.multiplier DESC""", ids)
    names = _names(zs)
    for r in rows:
        r["zone_name"] = names.get(r["zone_id"])
    tz = "all timestamps are New York local time"
    return ({"zones": rows, "time_zone": tz} if rows
            else {"zones": [], "note": "no surge data yet for these zones", "matched": zs, "time_zone": tz})


def top_zones(metric: str = "surge", n: int = 5, order: str = "highest") -> Dict[str, Any]:
    n = max(1, min(int(n), 25))
    lowest = str(order).strip().lower() == "lowest"
    names = {z["zone_id"]: z["name"] for z in _zones()}
    out: Dict[str, Any] = {"metric": metric, "order": "lowest first" if lowest else "highest first",
                           "time_zone": "all timestamps are New York local time"}
    if metric in ("surge", "demand"):
        col = "multiplier" if metric == "surge" else "demand"
        mx = q("SELECT MAX(minute_ts) AS m FROM {zone_minute_surge}")
        if not mx or mx[0]["m"] is None:
            return {"zones": [], "note": "no surge data yet"}
        cutoff = datetime.fromisoformat(mx[0]["m"]) - timedelta(minutes=10)
        latest = q("""
            SELECT s.zone_id, s.minute_ts, s.multiplier, s.demand, s.supply, s.ratio
            FROM {zone_minute_surge} s
            JOIN (SELECT zone_id, MAX(minute_ts) AS m FROM {zone_minute_surge} WHERE minute_ts >= ? GROUP BY zone_id) l
              ON s.zone_id = l.zone_id AND s.minute_ts = l.m""", [cutoff])
        latest = list({r["zone_id"]: r for r in latest}.values())          # one row per zone
        latest.sort(key=lambda r: (r[col] or 0, r["ratio"] or 0), reverse=not lowest)
        rows = latest[:n]
        by_mult: Dict[str, int] = {}
        for r in sorted(latest, key=lambda r: r["multiplier"] or 0):
            k = f"{(r['multiplier'] or 0):.1f}x"
            by_mult[k] = by_mult.get(k, 0) + 1
        mults = [r["multiplier"] for r in latest if r["multiplier"] is not None]
        out["summary_all_zones"] = {
            "taxi_zones_in_city": sum(1 for z in names if 1 <= z <= 263),
            "zones_with_live_data": len(latest),
            "zones_surging_above_1x": sum(1 for m in mults if m > 1.0),
            "zones_at_1x_no_surge": sum(1 for m in mults if m <= 1.0),
            "min_multiplier": min(mults) if mults else None,
            "max_multiplier": max(mults) if mults else None,
            "zones_per_multiplier": by_mult,
            "note": "zones without live data in the last 10 minutes have no current multiplier (1.0x by default)"}
    elif metric in ("wait_time", "eta"):
        rows = q(f"""
            SELECT zone_id, hour_ts, avg_eta_min, requests, completed_trips
            FROM {{zone_hourly_kpis}}
            WHERE source = 'live' AND avg_eta_min IS NOT NULL
              AND hour_ts = (SELECT MAX(hour_ts) FROM {{zone_hourly_kpis}} WHERE source = 'live')
            ORDER BY avg_eta_min {"ASC" if lowest else "DESC"} LIMIT {n}""")
    else:
        return {"error": "metric must be one of: surge, demand, wait_time"}
    for r in rows:
        r["zone_name"] = names.get(r["zone_id"])
    out["zones"] = rows
    return out


def driver_utilisation(date: str = "yesterday", zone: Any = None) -> Dict[str, Any]:  # noqa: A002
    day = _day(date)
    if zone:
        zs = resolve_zone(zone)
        if not zs:
            return {"error": f"no zone matches '{zone}'"}
        ids = [z["zone_id"] for z in zs]
        day_sql = "?" if day else "(SELECT MAX(date) FROM {zone_daily_utilisation})"
        rows = q(f"""
            SELECT zone_id, date, pings, on_trip_pings, utilisation FROM {{zone_daily_utilisation}}
            WHERE zone_id IN ({_in(ids)}) AND date = {day_sql} ORDER BY zone_id""", ids + ([day] if day else []))
        names = _names(zs)
        for r in rows:
            r["zone_name"] = names.get(r["zone_id"])
        target = 0.78
        return {"date": day or "latest", "zones": rows, "target_utilisation": target}
    def overall(d):
        day_sql = "?" if d else "(SELECT MAX(date) FROM {driver_daily_utilisation})"
        return q(f"""
            SELECT date, COUNT(*) AS drivers, AVG(utilisation) AS avg_utilisation, SUM(trips) AS trips,
                   SUM(CASE WHEN utilisation >= 0.78 THEN 1 ELSE 0 END) AS drivers_at_or_above_target
            FROM {{driver_daily_utilisation}} WHERE date = {day_sql} GROUP BY date""", [d] if d else [])
    rows = overall(day)
    out = {"date": day or "latest", "overall": rows[0] if rows else None, "target_utilisation": 0.78}
    if day and not rows:
        # the live replay restamps events to "now", so usually only today's data exists
        latest = overall(None)
        if latest:
            out.update({"overall": latest[0], "date": str(latest[0]["date"]),
                        "note": f"no driver data for {day}; showing the latest available day {latest[0]['date']}"})
    return out


def demand_forecast(zone: Any) -> Dict[str, Any]:
    zs = resolve_zone(zone)
    if not zs:
        return {"error": f"no zone matches '{zone}'"}
    ids = [z["zone_id"] for z in zs]
    fc = q(f"""
        SELECT zone_id, forecast_ts, predicted_demand, model_version, scored_at
        FROM {{demand_forecast}}
        WHERE mode = 'live' AND zone_id IN ({_in(ids)})
          AND scored_at = (SELECT MAX(scored_at) FROM {{demand_forecast}} WHERE mode = 'live')
        ORDER BY zone_id""", ids)
    note = None
    if not fc:
        fc = q(f"""
            SELECT zone_id, forecast_ts, predicted_demand, actual_demand, model_version
            FROM {{demand_forecast}} WHERE zone_id IN ({_in(ids)}) ORDER BY forecast_ts DESC LIMIT 4""", ids)
        note = "no live forecast yet; showing the latest hold-out predictions with actuals"
    latest_fc = {}
    for r in sorted(fc, key=lambda r: str(r.get("forecast_ts")), reverse=True):
        latest_fc.setdefault(r["zone_id"], r)                  # newest target slot per zone only
    recent = q(f"""
        SELECT zone_id, minute_ts, demand, supply, multiplier FROM {{zone_minute_surge}}
        WHERE zone_id IN ({_in(ids)}) ORDER BY minute_ts DESC LIMIT 6""", ids)
    names = _names(zs)
    for r in list(latest_fc.values()) + recent:
        r["zone_name"] = names.get(r["zone_id"])
    out = {"forecast": list(latest_fc.values()), "recent_live_requests": recent,
           "units": ("predicted_demand = expected ride requests in the next 30-minute slot, in the scale of the "
                     "historical sample the model was trained on; recent_live_requests.demand = requests in the "
                     "streaming engine's 5-minute window ending at minute_ts (live replay)"),
           "time_zone": "all timestamps are New York local time"}
    if note:
        out["note"] = note
    return out


def kpi_summary(date: str = "yesterday") -> Dict[str, Any]:  # noqa: A002
    day = _day(date)
    dsql = "CAST(? AS DATE)" if day else "(SELECT MAX(CAST(hour_ts AS DATE)) FROM {zone_hourly_kpis})"
    p = [day] if day else []
    k = q(f"""
        SELECT SUM(requests) AS requests, SUM(completed_trips) AS completed_trips, SUM(cancelled_trips) AS cancelled_trips,
               SUM(avg_eta_min * completed_trips) / NULLIF(SUM(CASE WHEN avg_eta_min IS NOT NULL THEN completed_trips END), 0) AS avg_rider_eta_min,
               SUM(revenue) AS revenue, SUM(distance_km) AS distance_km,
               SUM(revenue) / NULLIF(SUM(distance_km), 0) AS revenue_per_km
        FROM {{zone_hourly_kpis}} WHERE CAST(hour_ts AS DATE) = {dsql}""", p)
    s = q(f"""
        SELECT AVG(multiplier) AS avg_multiplier, MAX(multiplier) AS max_multiplier,
               AVG(CASE WHEN multiplier > 1.0 THEN 1.0 ELSE 0.0 END) AS share_of_zone_minutes_surging
        FROM {{zone_minute_surge}} WHERE CAST(minute_ts AS DATE) = {dsql}""", p)
    if day and (not k or not k[0].get("requests")):
        latest = q("SELECT CAST(MAX(CAST(hour_ts AS DATE)) AS STRING) AS d FROM {zone_hourly_kpis}")
        if latest and latest[0]["d"] and latest[0]["d"] != day:
            res = kpi_summary(latest[0]["d"])
            res["note"] = f"no KPI data for {day}; showing the latest available day {latest[0]['d']}"
            return res
    u = driver_utilisation(date or "latest")
    return {"date": day or "latest", "trips": k[0] if k else None, "surge": s[0] if s else None,
            "utilisation": u.get("overall"), "targets": {"avg_rider_eta_min": 4.5, "driver_utilisation": 0.78}}


TOOLS = {"get_current_surge": get_current_surge, "top_zones": top_zones, "driver_utilisation": driver_utilisation,
         "demand_forecast": demand_forecast, "kpi_summary": kpi_summary}

TOOL_SPECS = [
    {"type": "function", "function": {
        "name": "get_current_surge",
        "description": "Latest surge multiplier, demand, supply and weather for a taxi zone (id or name, e.g. 'Midtown', 132).",
        "parameters": {"type": "object", "properties": {"zone": {"type": "string", "description": "zone id or name"}}, "required": ["zone"]}}},
    {"type": "function", "function": {
        "name": "top_zones",
        "description": ("Zones right now ranked by surge multiplier, demand, or rider wait time (ETA), highest or lowest "
                        "first. For surge/demand it also returns summary_all_zones over EVERY zone: how many zones "
                        "have live data, how many are surging, min/max multiplier and the number of zones per multiplier."),
        "parameters": {"type": "object", "properties": {
            "metric": {"type": "string", "enum": ["surge", "demand", "wait_time"]},
            "n": {"type": "integer", "description": "how many zones to list (1-25)"},
            "order": {"type": "string", "enum": ["highest", "lowest"], "description": "highest first (default) or lowest first"}},
            "required": ["metric"]}}},
    {"type": "function", "function": {
        "name": "driver_utilisation",
        "description": "Driver utilisation (share of pings on a trip) and trips for a date, optionally for one zone. Target is 78%.",
        "parameters": {"type": "object", "properties": {
            "date": {"type": "string", "description": "YYYY-MM-DD, 'today', 'yesterday' or 'latest'"},
            "zone": {"type": "string", "description": "optional zone id or name"}}, "required": ["date"]}}},
    {"type": "function", "function": {
        "name": "demand_forecast",
        "description": "Next 30-minute demand forecast for a zone together with the most recent actual demand.",
        "parameters": {"type": "object", "properties": {"zone": {"type": "string"}}, "required": ["zone"]}}},
    {"type": "function", "function": {
        "name": "kpi_summary",
        "description": "Daily KPI summary: rider ETA, utilisation, revenue per km and surge statistics.",
        "parameters": {"type": "object", "properties": {"date": {"type": "string", "description": "YYYY-MM-DD, 'today', 'yesterday' or 'latest'"}},
                       "required": ["date"]}}},
]


def run_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    fn = TOOLS.get(name)
    if fn is None:
        return {"error": f"unknown tool {name}"}
    try:
        return fn(**args)
    except TypeError as exc:
        return {"error": f"bad arguments: {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}
