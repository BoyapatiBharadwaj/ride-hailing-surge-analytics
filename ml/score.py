"""Score the next 30-minute slot for every zone -> Databricks Gold `demand_forecast` (hourly Airflow DAG).

    python -m ml.score

Features of the NEXT 30-minute slot per zone:
  * lag_1, lag_2, rolling means  <- live requests in Databricks silver_requests (scaled so they are comparable with the
    training sample: sample_rate_of_seed / (REPLAY_SAMPLE_RATE * REPLAY_SPEED))
  * lag_48, lag_336             <- historical profile of the zone for the same weekday / half hour
    (the replay restamps one historical day to "now", so real yesterday / last-week values do not exist)
  * weather                     <- latest reading in Databricks silver_weather
Exit code 99 = nothing to do (no model yet, no live data, or Databricks not configured) -> Airflow "skipped".
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import joblib
import numpy as np
import pandas as pd

from common.config import DATA_DIR, EXPORT, MODELS, SKIP, databricks_configured, env_float, to_ny_naive
from ml.features import add_calendar, full_grid, next_slot, profile_tables, us_holidays


def live_demand(hours: int = 8) -> pd.DataFrame:
    """Requests per zone and 30-minute slot over the last hours (New York time), from Databricks Silver."""
    from cloud.databricks.workspace import query, table
    cutoff = to_ny_naive(datetime.now(timezone.utc)) - timedelta(hours=hours)
    rows = query(f"""
        SELECT zone_id, CAST(from_unixtime(FLOOR(unix_timestamp(local_ts) / 1800) * 1800) AS TIMESTAMP) AS slot_ts,
               COUNT(*) AS demand
        FROM {table('silver_requests')} WHERE local_ts >= ? GROUP BY 1, 2""", [cutoff])
    df = pd.DataFrame(rows, columns=["zone_id", "slot_ts", "demand"])
    df["slot_ts"] = pd.to_datetime(df["slot_ts"])
    return df


def latest_weather():
    from cloud.databricks.workspace import query, table
    r = query(f"SELECT temperature_2m, rain FROM {table('silver_weather')} ORDER BY event_time DESC LIMIT 1")
    return (float(r[0]["temperature_2m"]), float(r[0]["rain"])) if r else (10.0, 0.0)


def history_profile():
    """Profile + zone mean over the whole history (dense grid, zero slots included - same as in training)."""
    h = pd.read_parquet(f"{EXPORT}/demand_30min")
    g, zm = profile_tables(full_grid(h))
    return g[["zone_id", "dow", "hour", "minute30", "profile"]], zm


def score_live() -> int:
    if not databricks_configured():
        print("[score] Databricks not configured - nothing to score from")
        return SKIP
    mpath = f"{MODELS}/demand_model.joblib"
    if not os.path.exists(mpath):
        print("[score] no trained model yet (run python -m ml.train)")
        return SKIP
    bundle = joblib.load(mpath)
    live = live_demand()
    if live.empty:
        print("[score] no live requests in Silver yet")
        return SKIP
    try:
        meta = json.load(open(f"{DATA_DIR}/seed_meta.json"))
        seed_rate = float(meta["sample_rate"])
    except Exception:  # noqa: BLE001
        seed_rate = 1.0
    scale = seed_rate / (env_float("REPLAY_SAMPLE_RATE", 0.06) * env_float("REPLAY_SPEED", 1.0))

    now = pd.Timestamp(to_ny_naive(datetime.now(timezone.utc)))
    target = next_slot(now)
    piv = live.pivot(index="slot_ts", columns="zone_id", values="demand").reindex(
        pd.date_range(target - pd.Timedelta(hours=4), target - pd.Timedelta(minutes=30), freq="30min")).fillna(0.0)
    rows = []
    prof, zone_mean = history_profile()
    # every zone: zones of the history plus any zone seen live (a zone without live requests has lags 0)
    zones = np.union1d(zone_mean["zone_id"].to_numpy(), live["zone_id"].unique()).astype(int)
    temp, rain = latest_weather()
    for z in zones:
        s = piv[z] * scale if z in piv.columns else pd.Series(np.zeros(len(piv)))   # no live requests -> lags 0
        vals = s.to_numpy()
        rows.append({"zone_id": int(z), "slot_ts": target, "lag_1": vals[-1], "lag_2": vals[-2],
                     "roll_mean_4": vals[-4:].mean(), "roll_mean_8": vals[-8:].mean()})
    df = pd.DataFrame(rows)
    df = add_calendar(df, us_holidays([target.year]))
    df = df.merge(prof, on=["zone_id", "dow", "hour", "minute30"], how="left").merge(zone_mean, on="zone_id", how="left")
    df["zone_mean"] = df["zone_mean"].fillna(0.0)
    df["profile"] = df["profile"].fillna(df["zone_mean"])
    df["lag_48"] = df["profile"]
    df["lag_336"] = df["profile"]
    df["temp_c"], df["rain_mm"] = temp, rain
    pred = np.clip(bundle["model"].predict(df[bundle["features"]]), 0, None)

    from cloud.databricks.workspace import write_table
    out = pd.DataFrame({"zone_id": df["zone_id"].astype("int32"), "forecast_ts": pd.to_datetime(df["slot_ts"]),
                        "predicted_demand": pred.astype(float), "actual_demand": np.nan,
                        "model_version": bundle["version"],
                        "scored_at": pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)), "mode": "live"})
    write_table(out, "demand_forecast", mode="append")
    print(f"[score] {len(out)} zones scored for slot {target} (live scale {scale:.2f}); top: "
          f"{out.nlargest(3, 'predicted_demand')[['zone_id', 'predicted_demand']].round(1).to_dict('records')}")
    return 0


if __name__ == "__main__":
    sys.exit(score_live())
