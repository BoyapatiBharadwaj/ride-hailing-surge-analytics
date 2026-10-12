"""Ops page server (http://localhost:8501): the React dashboard + chat in frontend/dist and its small JSON API.

    python -m genai.ops_server            # container rh-genai (docker-compose service `genai`)

Standard library only (no extra packages in the tools image):
  GET  /api/live    live trips, drivers and surge from the last minutes of Kafka (genai/live_feed.py), cached 10 s
  GET  /api/gold    latest day's KPIs + next-30-minute forecast from the Databricks Gold tables, cached 120 s
  POST /api/chat    {"question": "...", "history": [{"role", "content"}]} -> GenAI Ops assistant answer + tool trace
  GET  /api/health  ok
  GET  /*           the built React app (frontend/dist, single-page app)
"""
import json
import mimetypes
import os
import threading
import time
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Tuple

from common.config import env, env_int, to_ny_naive
from genai import live_feed

for _ext, _type in ((".js", "text/javascript"), (".mjs", "text/javascript"), (".css", "text/css"),
                    (".svg", "image/svg+xml"), (".json", "application/json"), (".woff2", "font/woff2")):
    mimetypes.add_type(_type, _ext)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.environ.get("OPS_FRONTEND_DIR", os.path.join(ROOT, "frontend", "dist"))
LIVE_TTL, GOLD_TTL = 10, 120


# ------------------------------------------------------------------ helpers
def _json_default(o: Any):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if hasattr(o, "item"):
        return o.item()
    return str(o)


class TTLCache:
    """One cached value per key, refreshed at most every `ttl` seconds (one refresh at a time)."""

    def __init__(self):
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, ttl: float, fn: Callable[[], Any]) -> Any:
        with self._lock:
            hit = self._data.get(key)
            if hit and time.monotonic() - hit[0] < ttl:
                return hit[1]
            val = fn()
            self._data[key] = (time.monotonic(), val)
            return val


CACHE = TTLCache()


# ------------------------------------------------------------------ payloads
def build_live(snap: dict, now: datetime) -> dict:
    """Snapshot of the stream -> everything the React page draws."""
    trips = live_feed.summarise_trips(snap["requests"], snap["matches"], now)
    drivers = live_feed.summarise_drivers(snap["pings"], now)
    surge = live_feed.summarise_surge(snap["surge"])
    cents = live_feed.zone_centres()
    for p in trips["per_minute"]:
        p["minute"] = f"{p['minute']:%H:%M}"
    for f in trips["feed"]:
        f["time (NYC)"] = f"{f['time (NYC)']:%H:%M:%S}" if f["time (NYC)"] else None
    zones = [{**{k: z[k] for k in ("zone_id", "zone", "multiplier", "demand", "supply", "ratio")},
              "lat": cents.get(z["zone_id"], (None, None))[0], "lon": cents.get(z["zone_id"], (None, None))[1]}
             for z in surge["zones"]]
    top_free = sorted(drivers["available_by_zone"].items(), key=lambda kv: -kv[1])[:10]
    return {
        "ok": True, "read_at_utc": f"{now:%H:%M:%S}", "read_at_ny": f"{to_ny_naive(now):%H:%M:%S}",
        "trips": {k: v for k, v in trips.items()},
        "drivers": {"online": drivers["online"], "available": drivers["available"], "on_trip": drivers["on_trip"],
                    "utilisation_now": drivers["utilisation_now"], "scala_online": drivers["scala_online"],
                    "points": [[d["lat"], d["lon"], 1 if d["status"] == "on_trip" else 0, d["driver_id"],
                                live_feed.zone_label(d["zone_id"])]
                               for d in drivers["drivers"] if d.get("lat") is not None and d.get("lon") is not None],
                    "top_free_zones": [{"zone": live_feed.zone_label(z), "free": n} for z, n in top_free]},
        "surge": {"surging": surge["surging"], "max": surge["max"], "avg": surge["avg"], "zones": zones,
                  "last_update_ny": f"{surge['last_update_ny']:%H:%M:%S}" if surge["last_update_ny"] else None,
                  "rain_mm": surge["rain_mm"], "temp_c": surge["temp_c"]},
    }


def live_payload() -> dict:
    def load():
        now = datetime.now(timezone.utc)
        try:
            return build_live(live_feed.snapshot(), now)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "read_at_utc": f"{now:%H:%M:%S}",
                    "read_at_ny": f"{to_ny_naive(now):%H:%M:%S}"}
    return CACHE.get("live", LIVE_TTL, load)


def gold_payload() -> dict:
    def load():
        from genai import tools
        out = {"backend": tools.SOURCE}
        try:
            out["kpi"] = tools.kpi_summary("latest")
        except Exception as exc:  # noqa: BLE001
            out["kpi_error"] = f"{type(exc).__name__}: {exc}"
        try:
            rows = tools.q("""
                SELECT zone_id, forecast_ts, predicted_demand, model_version FROM {demand_forecast}
                WHERE mode = 'live' AND scored_at = (SELECT MAX(scored_at) FROM {demand_forecast} WHERE mode = 'live')
                ORDER BY predicted_demand DESC LIMIT 12""")
            out["forecast"] = [{**r, "zone": live_feed.zone_label(r["zone_id"])} for r in rows]
        except Exception as exc:  # noqa: BLE001
            out["forecast_error"] = f"{type(exc).__name__}: {exc}"
        return out
    return CACHE.get("gold", GOLD_TTL, load)


def chat_payload(body: dict) -> dict:
    from genai.assistant import ask
    q = str(body.get("question") or "").strip()[:1000]
    if not q:
        return {"answer": "Please type a question.", "trace": []}
    hist = [{"role": m.get("role"), "content": str(m.get("content", ""))[:4000]}
            for m in (body.get("history") or [])[-12:] if m.get("role") in ("user", "assistant")]
    try:
        answer, trace = ask(q, hist)
    except Exception as exc:  # noqa: BLE001
        answer, trace = f"⚠️ {exc}", []
    return {"answer": answer, "trace": trace}


# ------------------------------------------------------------------ HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "RideHailOps/1.0"

    def log_message(self, fmt, *args):            # quiet access log (one line per API call only)
        if self.path.startswith("/api/") and not self.path.startswith("/api/live"):
            super().log_message(fmt, *args)

    def _send(self, code: int, body: bytes, ctype: str, cache: str = "no-store"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200):
        self._send(code, json.dumps(obj, default=_json_default).encode(), "application/json; charset=utf-8")

    def do_GET(self):  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/api/health":
            return self._json({"ok": True})
        if path == "/api/live":
            return self._json(live_payload())
        if path == "/api/gold":
            return self._json(gold_payload())
        if path.startswith("/api/"):
            return self._json({"error": "not found"}, 404)
        return self._static(path)

    def do_POST(self):  # noqa: N802
        if self.path.split("?", 1)[0] != "/api/chat":
            return self._json({"error": "not found"}, 404)
        try:
            n = min(int(self.headers.get("Content-Length") or 0), 200_000)
            body = json.loads(self.rfile.read(n) or b"{}")
        except (ValueError, TypeError):
            return self._json({"error": "bad JSON"}, 400)
        return self._json(chat_payload(body if isinstance(body, dict) else {}))

    def _static(self, path: str):
        rel = os.path.normpath(path.lstrip("/")) if path not in ("", "/") else "index.html"
        full = os.path.realpath(os.path.join(DIST, rel))
        if not full.startswith(os.path.realpath(DIST)) or not os.path.isfile(full):
            full = os.path.join(DIST, "index.html")                       # single-page app fallback
        if not os.path.isfile(full):
            return self._send(503, b"Ops page not built: frontend/dist is missing (cd frontend && npm ci && npm run build)",
                              "text/plain; charset=utf-8")
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        cache = "public, max-age=31536000, immutable" if "/assets/" in full else "no-cache"
        with open(full, "rb") as fh:
            self._send(200, fh.read(), ctype, cache)


def main() -> None:
    port = env_int("OPS_PORT", 8501)
    srv = ThreadingHTTPServer((env("OPS_HOST", "0.0.0.0"), port), Handler)
    print(f"[ops] Ops page on http://0.0.0.0:{port}  (frontend: {DIST})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
