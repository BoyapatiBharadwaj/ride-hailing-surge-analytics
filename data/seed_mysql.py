"""Seed the operational MySQL database from the TLC month.

* city_zones : TLC taxi_zone_lookup + centroids from the zone grid
* drivers/riders : synthetic identities generated with Faker (public TLC data has no driver ids)
* trips : uniform sample (every k-th row) of the month so the whole month is represented.
          TLC data has no cancellations, so ~8% are marked CANCELLED with a reason, and ~0.5% dirty
          rows are injected so the Spark Core cleansing job has something real to remove.

Usage: python -m data.seed_mysql [--force]
"""
import argparse
import sys
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from faker import Faker
from sqlalchemy import create_engine, text

from common.config import (MYSQL_URL, TLC_DIR, ZONE_GRID_PATH, CITY, env, env_int)

CANCEL_REASONS = ["RIDER_CANCELLED", "DRIVER_CANCELLED", "NO_DRIVER_AVAILABLE", "RIDER_TIMEOUT", "PRICE_TOO_HIGH"]
CANCEL_P = [0.38, 0.22, 0.20, 0.12, 0.08]
VEHICLES = ["SEDAN", "SUV", "HYBRID", "EV", "LUXURY"]
VEH_P = [0.45, 0.2, 0.2, 0.1, 0.05]
CANCEL_RATE = 0.08
DIRTY_RATE = 0.005


def wait_for_mysql(engine, tries: int = 60):
    for _ in range(tries):
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
            return
        except Exception:
            time.sleep(3)
    raise RuntimeError("MySQL not reachable")


def clean_name(n: str) -> str:
    return n.replace(",", "").replace('"', "")[:78]


def seed_dims(engine, rng):
    import json
    lookup = pd.read_csv(f"{TLC_DIR}/taxi_zone_lookup.csv")
    grid = json.load(open(ZONE_GRID_PATH))["centroids"]
    zones = pd.DataFrame({
        "zone_id": lookup["LocationID"].astype(int),
        "zone_name": lookup["Zone"].fillna("Unknown").map(clean_name),
        "borough": lookup["Borough"].fillna("Unknown").map(clean_name),
        "service_zone": lookup["service_zone"].fillna("Unknown").map(clean_name),
        "city": CITY,
    })
    zones["centroid_lat"] = zones["zone_id"].map(lambda z: grid.get(str(z), [None, None])[0])
    zones["centroid_lon"] = zones["zone_id"].map(lambda z: grid.get(str(z), [None, None])[1])
    zones.to_sql("city_zones", engine, if_exists="append", index=False)

    fk = Faker("en_US")
    Faker.seed(42)
    n_d, n_r = env_int("N_DRIVERS", 5000), env_int("N_RIDERS", 50000)
    today = pd.Timestamp("2025-01-01")
    drivers = pd.DataFrame({
        "id": np.arange(1, n_d + 1),
        "name": [clean_name(fk.name()) for _ in range(n_d)],
        "vehicle_type": rng.choice(VEHICLES, n_d, p=VEH_P),
        "joined_date": (today - pd.to_timedelta(rng.integers(30, 1800, n_d), unit="D")).date,
        "home_city": CITY,
        "rating": np.round(np.clip(rng.normal(4.7, 0.2, n_d), 3.5, 5.0), 2),
    })
    drivers.to_sql("drivers", engine, if_exists="append", index=False, chunksize=5000)
    riders = pd.DataFrame({
        "id": np.arange(1, n_r + 1),
        "name": [clean_name(fk.name()) for _ in range(n_r)],
        "joined_date": (today - pd.to_timedelta(rng.integers(30, 1800, n_r), unit="D")).date,
        "home_city": CITY,
        "rating": np.round(np.clip(rng.normal(4.8, 0.2, n_r), 3.0, 5.0), 2),
    })
    riders.to_sql("riders", engine, if_exists="append", index=False, chunksize=5000)
    print(f"[seed] city_zones={len(zones)} drivers={n_d} riders={n_r}")
    return n_d, n_r


def transform(df: pd.DataFrame, start_id: int, n_d: int, n_r: int, rng) -> pd.DataFrame:
    n = len(df)
    out = pd.DataFrame({
        "trip_id": np.arange(start_id, start_id + n),
        "driver_id": rng.integers(1, n_d + 1, n),
        "rider_id": rng.integers(1, n_r + 1, n),
        "city": CITY,
        "pickup_zone_id": df["PULocationID"].to_numpy(),
        "dropoff_zone_id": df["DOLocationID"].to_numpy(),
        "request_time": pd.to_datetime(df["request_datetime"]).to_numpy(),
        "pickup_time": pd.to_datetime(df["pickup_datetime"]).to_numpy(),
        "dropoff_time": pd.to_datetime(df["dropoff_datetime"]).to_numpy(),
        "distance_km": (df["trip_miles"].to_numpy() * 1.60934).round(3),
        "fare": df["base_passenger_fare"].to_numpy().round(2),
    })
    wait = (out["pickup_time"] - out["request_time"]).dt.total_seconds() / 60.0
    out["surge_multiplier"] = np.select([wait < 4, wait < 7, wait < 10], [1.0, 1.2, 1.5], default=2.0)
    out["status"] = "COMPLETED"
    out["cancel_reason"] = None

    # ---- synthetic cancellations (long waits cancel more, NO_DRIVER_AVAILABLE dominates then)
    p_cancel = np.clip(CANCEL_RATE * (0.5 + wait.fillna(5).to_numpy() / 8.0), 0.01, 0.5)
    cancelled = rng.random(n) < p_cancel * (CANCEL_RATE / p_cancel.mean())
    reasons = rng.choice(CANCEL_REASONS, n, p=CANCEL_P)
    long_wait = (wait.fillna(0).to_numpy() > 9) & cancelled
    reasons[long_wait] = np.where(rng.random(long_wait.sum()) < 0.6, "NO_DRIVER_AVAILABLE", "RIDER_TIMEOUT")
    out.loc[cancelled, "status"] = "CANCELLED"
    out.loc[cancelled, "cancel_reason"] = reasons[cancelled]
    out.loc[cancelled, ["pickup_time", "dropoff_time"]] = pd.NaT
    out.loc[cancelled, "fare"] = 0.0

    # ---- dirty rows for the cleansing job
    dirty = rng.random(n) < DIRTY_RATE
    kind = rng.integers(0, 3, n)
    out.loc[dirty & (kind == 0), "pickup_zone_id"] = np.nan
    out.loc[dirty & (kind == 1), "fare"] = -abs(out["fare"]) - 1
    k2 = dirty & (kind == 2) & ~cancelled
    out.loc[k2, "dropoff_time"] = out.loc[k2, "pickup_time"] - pd.Timedelta(minutes=5)
    return out


def seed_trips(engine, n_d, n_r, rng):
    trip_file = f"{TLC_DIR}/fhvhv_tripdata_{env('TLC_MONTH', '2025-01')}.parquet"
    pf = pq.ParquetFile(trip_file)
    total = pf.metadata.num_rows
    cap = env_int("SEED_MAX_ROWS", 2_000_000)
    k = max(1, total // cap)
    print(f"[seed] {trip_file}: {total:,} rows, keeping every {k}th (~{total // k:,})")
    import json
    from common.config import DATA_DIR
    json.dump({"total_rows": total, "keep_every": k, "sample_rate": 1.0 / k},
              open(f"{DATA_DIR}/seed_meta.json", "w"))      # used to scale live demand for the forecast
    cols = ["request_datetime", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID",
            "trip_miles", "base_passenger_fare"]
    seen, next_id, inserted = 0, 1, 0
    for batch in pf.iter_batches(batch_size=500_000, columns=cols):
        df = batch.to_pandas()
        idx = np.arange(seen, seen + len(df))
        seen += len(df)
        df = df[(idx % k == 0)]
        df = df[(df["PULocationID"].between(1, 263)) & (df["DOLocationID"].between(1, 263))
                & (df["trip_miles"] > 0) & (df["base_passenger_fare"] > 0)
                & df["request_datetime"].notna() & df["pickup_datetime"].notna() & df["dropoff_datetime"].notna()]
        if df.empty:
            continue
        out = transform(df, next_id, n_d, n_r, rng)
        next_id += len(out)
        out = out.astype(object).where(out.notna(), None)
        out.to_sql("trips", engine, if_exists="append", index=False, chunksize=10_000, method="multi")
        inserted += len(out)
        print(f"[seed] trips inserted: {inserted:,}")
    return inserted


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="truncate and re-seed")
    args = ap.parse_args()
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    wait_for_mysql(engine)
    with engine.begin() as c:
        existing = c.execute(text("SELECT COUNT(*) FROM trips")).scalar()
        if existing and not args.force:
            print(f"[seed] trips already has {existing:,} rows - skipping (use --force to reseed)")
            return 0
        for t in ("trips", "riders", "drivers", "city_zones"):
            c.execute(text(f"DELETE FROM {t}"))
    rng = np.random.default_rng(42)
    n_d, n_r = seed_dims(engine, rng)
    n = seed_trips(engine, n_d, n_r, rng)
    print(f"[seed] done: {n:,} trips")
    return 0


if __name__ == "__main__":
    sys.exit(main())
