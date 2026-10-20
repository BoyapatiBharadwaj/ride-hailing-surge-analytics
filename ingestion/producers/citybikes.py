"""CityBikes poller (live supply proxy) -> `driver_gps_pings` (source = citybikes).

Every CITYBIKES_POLL_SEC the Citi Bike NYC network is polled; each station's free bikes are mapped to its
taxi zone (pre-computed point-in-polygon grid) and published as one event per station.
"""
import logging
import uuid
from datetime import datetime, timezone

import requests

from common.config import TOPIC_GPS, env, env_int, iso_utc
from common.zones import load_grid
from ingestion.producers.common import Stats, Stopper, make_producer, retry, send

log = logging.getLogger("citybikes")


def poll(network: str) -> list:
    r = requests.get(f"https://api.citybik.es/v2/networks/{network}", timeout=30)
    r.raise_for_status()
    return r.json()["network"]["stations"]


def main() -> None:
    network = env("CITYBIKES_NETWORK", "citi-bike-nyc")
    every = env_int("CITYBIKES_POLL_SEC", 60)
    grid = load_grid()
    prod, stats, stop = make_producer("citybikes"), Stats(), Stopper()
    while not stop.stop:
        try:
            stations = retry(lambda: poll(network), logger=log)
        except RuntimeError:
            log.error("CityBikes unavailable, skipping this cycle (TLC replay + simulator keep the stream going)")
            stop.sleep(every)
            continue
        now = iso_utc(datetime.now(timezone.utc))
        n = 0
        for s in stations:
            lat, lon = s.get("latitude"), s.get("longitude")
            zone = grid.zone_of(lat, lon)
            if not zone:
                continue
            ev = {"event_id": str(uuid.uuid4()), "event_time": now, "station_id": s.get("id"), "lat": lat,
                  "lon": lon, "free_bikes": int(s.get("free_bikes") or 0), "empty_slots": int(s.get("empty_slots") or 0),
                  "zone_id": zone, "source": "citybikes", "status": "available", "city": env("CITY", "NYC")}
            send(prod, TOPIC_GPS, str(zone), ev, stats)
            n += 1
        prod.poll(0)
        log.info("published %d station readings (delivered=%d failed=%d)", n, stats.delivered, stats.failed)
        stop.sleep(every)
    prod.flush(30)


if __name__ == "__main__":
    main()
