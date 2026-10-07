"""Open-Meteo poller (live weather, no API key) -> `weather` topic every WEATHER_POLL_SEC."""
import logging
import uuid
from datetime import datetime, timezone

import requests

from common.config import TOPIC_WEATHER, env, env_int, iso_utc
from ingestion.producers.common import Stats, Stopper, make_producer, retry, send

log = logging.getLogger("weather")
URL = ("https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
       "&current=temperature_2m,precipitation,rain,weather_code&timezone=UTC")


def fetch(lat: str, lon: str) -> dict:
    r = requests.get(URL.format(lat=lat, lon=lon), timeout=20)
    r.raise_for_status()
    return r.json()["current"]


def main() -> None:
    lat, lon = env("WEATHER_LAT", "40.7128"), env("WEATHER_LON", "-74.0060")
    every = env_int("WEATHER_POLL_SEC", 60)
    prod, stats, stop = make_producer("weather"), Stats(), Stopper()
    last = {"temperature_2m": 10.0, "precipitation": 0.0, "rain": 0.0, "weather_code": 0}
    while not stop.stop:
        try:
            cur = retry(lambda: fetch(lat, lon), logger=log)
            last = {k: cur.get(k, last[k]) for k in last}
        except RuntimeError:
            log.error("Open-Meteo unavailable - re-publishing the last known reading")
        ev = {"event_id": str(uuid.uuid4()), "event_time": iso_utc(datetime.now(timezone.utc)),
              "zone_id": 0, "temperature_2m": last["temperature_2m"], "precipitation": last["precipitation"],
              "rain": last["rain"], "weather_code": last["weather_code"], "source": "open-meteo",
              "city": env("CITY", "NYC")}
        send(prod, TOPIC_WEATHER, env("CITY", "NYC"), ev, stats)
        log.info("weather temp=%.1fC rain=%.2fmm", ev["temperature_2m"], ev["rain"])
        stop.sleep(every)
    prod.flush(30)


if __name__ == "__main__":
    main()
