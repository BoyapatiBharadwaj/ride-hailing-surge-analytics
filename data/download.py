"""Download the public inputs: NYC TLC HVFHV month, taxi-zone lookup + shapefile, hourly weather history.

Usage (inside the tools container):  python -m data.download
"""
import json
import os
import sys
import zipfile

import pyarrow.parquet as pq
import requests

from common.config import DATA_DIR, TLC_DIR, env

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"


def fetch(url: str, dest: str, chunk: int = 1 << 20) -> None:
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        print(f"[download] exists, skipping: {dest}")
        return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    print(f"[download] {url}")
    tmp = dest + ".part"
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        done = 0
        with open(tmp, "wb") as fh:
            for part in r.iter_content(chunk):
                fh.write(part)
                done += len(part)
                if total:
                    print(f"\r[download]   {done / 1e6:8.1f} / {total / 1e6:.1f} MB", end="")
        print()
    os.replace(tmp, dest)


def zone_weights(trip_file: str, out: str) -> None:
    """Pickup share per zone: used by the GPS simulator to place the fleet."""
    col = pq.read_table(trip_file, columns=["PULocationID"]).column("PULocationID").to_pandas()
    vc = col[(col >= 1) & (col <= 263)].value_counts(normalize=True)
    with open(out, "w") as fh:
        json.dump({str(int(k)): float(v) for k, v in vc.items()}, fh)
    print(f"[download] zone weights for {len(vc)} zones -> {out}")


# NYC monthly mean temperature (deg C, Central Park normals) for the offline fallback
_NYC_NORMAL_C = [0.5, 1.6, 5.6, 11.7, 17.2, 22.4, 25.3, 24.7, 20.9, 14.5, 8.8, 3.4]
_HOURLY = "temperature_2m,precipitation,rain,weather_code"


def _open_meteo(base: str, start: str, end: str) -> dict:
    """GET one Open-Meteo endpoint with retries; returns the `hourly` block or raises."""
    import time
    url = (f"{base}?latitude={env('WEATHER_LAT', '40.7128')}&longitude={env('WEATHER_LON', '-74.0060')}"
           f"&start_date={start}&end_date={end}&hourly={_HOURLY}&timezone=America%2FNew_York")
    last = None
    for attempt in range(1, 5):
        print(f"[download] {url}" if attempt == 1 else f"[download]   retry {attempt}/4")
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 200 and r.text.lstrip().startswith("{"):
                h = r.json().get("hourly")
                if h and h.get("time"):
                    return h
                last = f"no hourly data in response: {r.text[:120]}"
            else:
                last = f"HTTP {r.status_code}: {r.text[:120].strip()}"
        except requests.RequestException as e:
            last = str(e)
        time.sleep(5 * attempt)            # 5, 10, 15 s - Open-Meteo rate limits recover quickly
    raise RuntimeError(last)


def _synthetic_weather(y: int, m: int, last: int):
    """Deterministic NYC-like hourly weather for the month (used only if every Open-Meteo endpoint fails)."""
    import numpy as np
    import pandas as pd
    hours = pd.date_range(f"{y}-{m:02d}-01", periods=last * 24, freq="h")
    rng = np.random.default_rng(y * 100 + m)
    temp = _NYC_NORMAL_C[m - 1] + 4.0 * np.sin((hours.hour - 9) / 24 * 2 * np.pi) + rng.normal(0, 2.0, len(hours))
    wet = np.zeros(len(hours), dtype=bool)
    for start in rng.choice(len(hours) - 8, size=max(3, last // 4), replace=False):     # ~7 rain spells a month
        wet[start:start + int(rng.integers(3, 9))] = True
    rain = np.where(wet, rng.gamma(1.5, 1.2, len(hours)), 0.0).round(1)
    return {"time": hours.strftime("%Y-%m-%dT%H:%M").tolist(), "temperature_2m": temp.round(1).tolist(),
            "precipitation": rain.tolist(), "rain": rain.tolist(),
            "weather_code": np.where(rain >= 2.5, 63, np.where(rain > 0, 61, 1)).tolist()}


def weather_history(month: str, out: str) -> None:
    if os.path.exists(out):
        print(f"[download] exists, skipping: {out}")
        return
    import calendar
    import pandas as pd

    y, m = (int(x) for x in month.split("-"))
    last = calendar.monthrange(y, m)[1]
    start, end = f"{y}-{m:02d}-01", f"{y}-{m:02d}-{last}"
    # 1) ERA5 archive  2) historical-forecast archive (same hourly variables)  3) offline NYC climatology
    h, source = None, None
    for base in ("https://archive-api.open-meteo.com/v1/archive",
                 "https://historical-forecast-api.open-meteo.com/v1/forecast"):
        try:
            h, source = _open_meteo(base, start, end), base.split("//")[1].split("/")[0]
            break
        except RuntimeError as e:
            print(f"[download] ! weather from {base.split('//')[1].split('/')[0]} failed: {e}")
    if h is None:
        print("[download] ! Open-Meteo unreachable - using built-in NYC climatology for the weather features "
              "(delete data/weather_hourly.parquet and re-run `python -m data.download` later for real data)")
        h, source = _synthetic_weather(y, m, last), "synthetic"
    df = pd.DataFrame({
        "hour_ts": pd.to_datetime(h["time"]),                    # NYC local wall clock (naive)
        "temp_c": h["temperature_2m"], "precip_mm": h["precipitation"],
        "rain_mm": h["rain"], "weather_code": h["weather_code"],
    })
    df[["temp_c", "precip_mm", "rain_mm"]] = df[["temp_c", "precip_mm", "rain_mm"]].astype(float).fillna(0.0)
    # microsecond timestamps: Spark cannot read pandas' default TIMESTAMP(NANOS) parquet
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    print(f"[download] weather history {len(df)} rows ({source}) -> {out}")


def main() -> int:
    month = env("TLC_MONTH", "2025-01")
    os.makedirs(TLC_DIR, exist_ok=True)
    trip_file = f"{TLC_DIR}/fhvhv_tripdata_{month}.parquet"
    fetch(f"{TLC_BASE}/trip-data/fhvhv_tripdata_{month}.parquet", trip_file)
    fetch(f"{TLC_BASE}/misc/taxi_zone_lookup.csv", f"{TLC_DIR}/taxi_zone_lookup.csv")
    fetch(f"{TLC_BASE}/misc/taxi_zones.zip", f"{TLC_DIR}/taxi_zones.zip")
    with zipfile.ZipFile(f"{TLC_DIR}/taxi_zones.zip") as z:
        z.extractall(f"{TLC_DIR}/taxi_zones")
    zone_weights(trip_file, f"{DATA_DIR}/zone_weights.json")
    weather_history(month, f"{DATA_DIR}/weather_hourly.parquet")
    print("[download] done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
