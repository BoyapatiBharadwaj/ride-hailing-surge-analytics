"""NYC TLC HVFHV replay producer -> Kafka topic `trip_requests`.

live mode (default)   re-stamps every historical request with the current time while keeping the
                      original gaps between requests; REPLAY_SPEED speeds the clock up.
backfill mode         re-publishes a historical date range with its ORIGINAL event times, as fast as
                      possible, e.g. to fill a gap in the history after a fix.

    python -m ingestion.producers.tlc_replay                      # live
    python -m ingestion.producers.tlc_replay --mode backfill --from 2025-01-10 --to 2025-01-11
"""
import argparse
import logging
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from common.config import NY_TZ, TLC_DIR, TOPIC_TRIPS, env, env_float, iso_utc
from ingestion.producers.common import Stats, Stopper, make_producer, send

log = logging.getLogger("tlc_replay")
COLS = ["request_datetime", "PULocationID", "DOLocationID", "trip_miles", "trip_time", "base_passenger_fare"]


def trip_file() -> str:
    return f"{TLC_DIR}/fhvhv_tripdata_{env('TLC_MONTH', '2025-01')}.parquet"


def load_day(day: date, sample_rate: float, seed: int = 7) -> pd.DataFrame:
    start = pd.Timestamp(datetime.combine(day, dtime.min))
    end = start + pd.Timedelta(days=1)
    table = pq.read_table(trip_file(), columns=COLS,
                          filters=[("request_datetime", ">=", start), ("request_datetime", "<", end)])
    df = table.to_pandas()
    df = df[df["PULocationID"].between(1, 263) & df["DOLocationID"].between(1, 263)
            & (df["trip_miles"] > 0) & (df["base_passenger_fare"] > 0)]
    if sample_rate < 1.0:
        df = df.sample(frac=sample_rate, random_state=seed)
    df = df.sort_values("request_datetime").reset_index(drop=True)
    df["offset_sec"] = (df["request_datetime"] - start).dt.total_seconds()
    log.info("loaded %d requests for %s (sample_rate=%.3f)", len(df), day, sample_rate)
    return df


def make_event(row, event_id: str, event_time: datetime) -> dict:
    return {
        "event_id": event_id,
        "event_time": iso_utc(event_time),
        "zone_id": int(row.PULocationID),
        "dropoff_zone_id": int(row.DOLocationID),
        "trip_miles": round(float(row.trip_miles), 3),
        "trip_time_sec": int(row.trip_time),
        "fare": round(float(row.base_passenger_fare), 2),
        "city": env("CITY", "NYC"),
        "source": "tlc_replay",
    }


def run_live() -> None:
    day = date.fromisoformat(env("REPLAY_DAY", "2025-01-15"))
    speed = env_float("REPLAY_SPEED", 1.0)
    rate = env_float("REPLAY_SAMPLE_RATE", 0.06)
    hh, mm = (int(x) for x in env("REPLAY_START_TIME", "07:30").split(":"))
    start_off = hh * 3600 + mm * 60
    df = load_day(day, rate)
    if df.empty:
        raise SystemExit("no trips for REPLAY_DAY - check TLC_MONTH / REPLAY_DAY")
    offs = df["offset_sec"].to_numpy()
    rows = list(df.itertuples(index=False))
    i0 = int(np.searchsorted(offs, start_off))
    prod, stats, stop = make_producer("tlc-replay"), Stats(), Stopper()
    mono0 = time.monotonic()
    loop, sent, last_log = 0, 0, time.monotonic()
    log.info("replaying day %s from %s at %.1fx, %d events/loop", day, env("REPLAY_START_TIME", "07:30"), speed, len(rows))
    while not stop.stop:
        first = i0 if loop == 0 else 0
        for idx in range(first, len(rows)):
            if stop.stop:
                break
            sim_t = offs[idx] + loop * 86400 - start_off
            due = mono0 + sim_t / speed
            delay = due - time.monotonic()
            if delay > 0:
                stop.sleep(delay)
                if stop.stop:
                    break
            # wall-clock time of sending: stays correct after the laptop sleeps (the monotonic clock pauses then)
            ev_time = datetime.now(timezone.utc)
            ev = make_event(rows[idx], f"tr-{day:%Y%m%d}-{idx}-{loop}", ev_time)
            send(prod, TOPIC_TRIPS, str(ev["zone_id"]), ev, stats)
            sent += 1
            if time.monotonic() - last_log > 30:
                log.info("sent=%d delivered=%d failed=%d loop=%d", sent, stats.delivered, stats.failed, loop)
                last_log = time.monotonic()
        loop += 1
    prod.flush(30)
    log.info("stopped: sent=%d delivered=%d failed=%d", sent, stats.delivered, stats.failed)


def run_backfill(d_from: date, d_to: date, rate: float, max_per_sec: float) -> None:
    prod, stats = make_producer("tlc-backfill"), Stats()
    sent = 0
    day = d_from
    while day <= d_to:
        df = load_day(day, rate)
        for idx, row in enumerate(df.itertuples(index=False)):
            local = datetime.combine(day, dtime.min) + timedelta(seconds=float(row.offset_sec))
            ev_time = local.replace(tzinfo=NY_TZ).astimezone(timezone.utc)
            ev = make_event(row, f"tr-bf-{day:%Y%m%d}-{idx}", ev_time)
            send(prod, TOPIC_TRIPS, str(ev["zone_id"]), ev, stats)
            sent += 1
            if max_per_sec and sent % int(max_per_sec) == 0:
                time.sleep(1)
        log.info("backfilled %s (%d total)", day, sent)
        day += timedelta(days=1)
    prod.flush(60)
    log.info("backfill done sent=%d delivered=%d failed=%d", sent, stats.delivered, stats.failed)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["live", "backfill"], default="live")
    ap.add_argument("--from", dest="d_from")
    ap.add_argument("--to", dest="d_to")
    ap.add_argument("--sample-rate", type=float, default=env_float("REPLAY_SAMPLE_RATE", 0.06))
    ap.add_argument("--max-per-sec", type=float, default=2000)
    a = ap.parse_args()
    if a.mode == "live":
        run_live()
    else:
        run_backfill(date.fromisoformat(a.d_from), date.fromisoformat(a.d_to or a.d_from), a.sample_rate, a.max_per_sec)
