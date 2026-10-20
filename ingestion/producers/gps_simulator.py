"""Driver GPS simulator (synthetic fleet) -> `driver_gps_pings`, plus simple dispatch -> `trip_matches`.

* Fleet of FLEET_SIZE drivers moves between taxi zones; every driver pings every 15-30 s.
* It consumes the replayed `trip_requests` and marks drivers `available` / `on_trip` as trips are matched.
  Matching is a nearest-idle-driver heuristic, only used to make supply realistic and to
  record a live rider ETA - real dispatch is out of scope.
* It also consumes `surge_updates`: idle drivers drift towards high-multiplier zones, which closes the
  loop the project is about (surge pricing draws drivers into under-supplied zones).
"""
import json
import heapq
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer

from common.config import (DATA_DIR, KAFKA_BOOTSTRAP, TOPIC_GPS, TOPIC_MATCHES, TOPIC_SURGE, TOPIC_TRIPS,
                           env, env_float, env_int, iso_utc)
from common.zones import load_grid
from ingestion.producers.common import Stats, Stopper, make_producer, send

log = logging.getLogger("gps_simulator")
AVG_SPEED_KMH = 22.0
MATCH_RADIUS_KM = 8.0


class FleetSim:
    def __init__(self, fleet: int, speed: float, seed: int = 11):
        self.grid = load_grid()
        self.rng = random.Random(seed)
        self.speed = max(speed, 0.01)
        self.n = fleet
        self.weights = self._weights()
        zones = list(self.weights)
        w = [self.weights[z] for z in zones]
        self.zone = [0] * (fleet + 1)
        self.status = ["available"] * (fleet + 1)
        self.trip = [None] * (fleet + 1)          # (pickup_zone, dropoff_zone, switch_time) while on_trip
        self.idle = {z: set() for z in self.grid.zones}
        self.surge = {}                           # zone -> (multiplier, ts)
        self.heap = []                            # (time, kind, driver)
        now = time.monotonic()
        for d in range(1, fleet + 1):
            z = self.rng.choices(zones, w)[0]
            self.zone[d] = z
            self.idle[z].add(d)
            heapq.heappush(self.heap, (now + self.rng.uniform(0, 30), "ping", d))
            heapq.heappush(self.heap, (now + self.rng.uniform(60, 240), "move", d))
        self.matched = self.unmatched = 0
        self.eta_sum = 0.0

    def _weights(self) -> dict:
        try:
            raw = json.load(open(f"{DATA_DIR}/zone_weights.json"))
            w = {int(k): v for k, v in raw.items() if int(k) in self.grid.centroids}
        except FileNotFoundError:
            w = {}
        return w or {z: 1.0 for z in self.grid.zones}

    # ---------------- events
    def ping_event(self, d: int, now_utc: datetime) -> dict:
        z = self.zone[d]
        if self.status[d] == "on_trip" and self.trip[d] and time.monotonic() >= self.trip[d][2]:
            z = self.trip[d][1]
        lat, lon = self.grid.random_point(z, self.rng)
        return {"event_id": str(uuid.uuid4()), "event_time": iso_utc(now_utc), "driver_id": d,
                "lat": round(lat, 6), "lon": round(lon, 6), "status": self.status[d], "zone_id": z,
                "source": "driver", "city": env("CITY", "NYC")}

    def process_due(self, now_m: float, now_utc: datetime, emit) -> None:
        while self.heap and self.heap[0][0] <= now_m:
            t, kind, d = heapq.heappop(self.heap)
            if kind == "ping":
                emit(TOPIC_GPS, str(self.zone[d]), self.ping_event(d, now_utc))
                heapq.heappush(self.heap, (t + self.rng.uniform(env_float("GPS_PING_MIN_SEC", 15),
                                                                 env_float("GPS_PING_MAX_SEC", 30)), "ping", d))
            elif kind == "free":
                trip = self.trip[d]
                self.zone[d] = trip[1] if trip else self.zone[d]
                self.status[d], self.trip[d] = "available", None
                self.idle[self.zone[d]].add(d)
            elif kind == "move":
                if self.status[d] == "available" and self.rng.random() < 0.4:
                    self.idle[self.zone[d]].discard(d)
                    self.zone[d] = self.pick_target_zone(self.zone[d], now_m)
                    self.idle[self.zone[d]].add(d)
                heapq.heappush(self.heap, (t + self.rng.uniform(120, 300), "move", d))

    # ---------------- behaviour
    def pick_target_zone(self, z: int, now_m: float) -> int:
        cands = self.grid.neighbors(z, 25)
        wts = []
        for c in cands:
            m, ts = self.surge.get(c, (1.0, 0))
            m = m if now_m - ts < 300 else 1.0
            wts.append((self.weights.get(c, 0.0005) + 0.0005) * (1.0 + 3.0 * (m - 1.0)))
        return self.rng.choices(cands, wts)[0]

    def surge_mult(self, zone: int, now_m: float) -> float:
        m, ts = self.surge.get(zone, (1.0, 0))
        return m if now_m - ts < 300 else 1.0

    def handle_request(self, ev: dict, now_m: float, now_utc: datetime, emit) -> None:
        pz, dz = int(ev["zone_id"]), int(ev["dropoff_zone_id"])
        if pz not in self.idle or dz not in self.idle:
            return
        driver, dist = None, 0.0
        for cz in self.grid.neighbors(pz, 15):
            d_km = self.grid.distance_km(pz, cz)
            if d_km > MATCH_RADIUS_KM:
                break
            if self.idle[cz]:
                driver = self.rng.choice(tuple(self.idle[cz]))
                self.idle[cz].discard(driver)
                dist = d_km
                break
        base = {"event_id": str(uuid.uuid4()), "event_time": iso_utc(now_utc), "trip_id": ev["event_id"],
                "pickup_zone_id": pz, "dropoff_zone_id": dz, "city": ev.get("city", "NYC")}
        if driver is None:
            self.unmatched += 1
            emit(TOPIC_MATCHES, str(pz), {**base, "status": "UNMATCHED", "driver_id": None, "eta_min": None,
                                         "fare": 0.0, "distance_km": ev.get("trip_miles", 0) * 1.60934,
                                         "surge_multiplier": self.surge_mult(pz, now_m)})
            return
        eta_min = round(dist / AVG_SPEED_KMH * 60.0 + 1.0 + self.rng.uniform(0, 1.5), 2)
        trip_s = float(ev.get("trip_time_sec", 600))
        mult = self.surge_mult(pz, now_m)
        self.status[driver] = "on_trip"
        self.trip[driver] = (pz, dz, now_m + (eta_min * 60 + trip_s / 2) / self.speed)
        self.zone[driver] = pz
        heapq.heappush(self.heap, (now_m + (eta_min * 60 + trip_s) / self.speed, "free", driver))
        self.matched += 1
        self.eta_sum += eta_min
        emit(TOPIC_MATCHES, str(pz), {**base, "status": "MATCHED", "driver_id": driver, "eta_min": eta_min,
                                      "fare": round(float(ev.get("fare", 0)) * mult, 2),
                                      "distance_km": round(float(ev.get("trip_miles", 0)) * 1.60934, 3),
                                      "surge_multiplier": mult})

    def handle_surge(self, ev: dict, now_m: float) -> None:
        try:
            self.surge[int(ev["zone_id"])] = (float(ev["multiplier"]), now_m)
        except (KeyError, TypeError, ValueError):
            pass


def main() -> None:
    fleet = min(env_int("FLEET_SIZE", 700), env_int("N_DRIVERS", 5000))
    speed = env_float("REPLAY_SPEED", 1.0)
    sim = FleetSim(fleet, speed)
    prod, stats, stop = make_producer("gps-simulator"), Stats(), Stopper()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "gps-simulator",
        "auto.offset.reset": "latest", "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_TRIPS, TOPIC_SURGE])
    emit = lambda topic, key, ev: send(prod, topic, key, ev, stats)  # noqa: E731
    log.info("fleet=%d zones=%d speed=%.1fx", fleet, len(sim.grid.zones), speed)
    last_log = time.monotonic()
    while not stop.stop:
        now_m, now_utc = time.monotonic(), datetime.now(timezone.utc)
        sim.process_due(now_m, now_utc, emit)
        for _ in range(500):
            msg = consumer.poll(0.01 if _ else 0.1)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                ev = json.loads(msg.value())
            except ValueError:
                continue
            if msg.topic() == TOPIC_TRIPS:
                sim.handle_request(ev, time.monotonic(), datetime.now(timezone.utc), emit)
            else:
                sim.handle_surge(ev, time.monotonic())
        if time.monotonic() - last_log > 30:
            idle = sum(len(s) for s in sim.idle.values())
            avg = sim.eta_sum / sim.matched if sim.matched else 0.0
            log.info("idle=%d/%d matched=%d unmatched=%d avg_eta=%.2fmin delivered=%d failed=%d",
                     idle, fleet, sim.matched, sim.unmatched, avg, stats.delivered, stats.failed)
            last_log = time.monotonic()
    consumer.close()
    prod.flush(30)


if __name__ == "__main__":
    main()
