"""Feature engineering for the 30-minute forward demand forecast.

Row = (zone, target 30-minute slot s). Target = requests in slot s. Everything else is known BEFORE s starts:
  zone_id, hour, minute30, day of week, weekend / holiday flag,
  demand lags: previous 30 min (lag_1), 60 min (lag_2), same time yesterday (lag_48), same time last week (lag_336),
  rolling means of the last 4 / 8 slots, rain and temperature,
  profile   = the zone's average demand for this weekday + half hour (from the TRAINING period only;
              leave-one-out on the training rows themselves, so a row never sees its own target),
  zone_mean = the zone's average demand per slot (training period).
The single-slot lags are noisy (a few requests per zone per 30 min); the profile averages several weeks and is
the strongest predictor for sparse zones.
"""
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd

FEATURES = ["zone_id", "hour", "minute30", "dow", "is_weekend", "is_holiday", "lag_1", "lag_2", "lag_48", "lag_336",
            "roll_mean_4", "roll_mean_8", "temp_c", "rain_mm", "profile", "zone_mean"]
PROFILE_KEYS = ["zone_id", "dow", "hour", "minute30"]
SLOT = pd.Timedelta(minutes=30)


def us_holidays(years: Iterable[int]) -> set:
    import holidays
    return set(holidays.US(years=list(years)).keys())


def full_grid(slots: pd.DataFrame) -> pd.DataFrame:
    """Dense zone x slot grid (missing slot = 0 requests) so that lags are well defined."""
    slots = slots.copy()
    slots["slot_ts"] = pd.to_datetime(slots["slot_ts"]).dt.tz_localize(None)
    all_ts = pd.date_range(slots["slot_ts"].min(), slots["slot_ts"].max(), freq=SLOT)
    zones = np.sort(slots["zone_id"].unique())
    idx = pd.MultiIndex.from_product([zones, all_ts], names=["zone_id", "slot_ts"])
    grid = slots.set_index(["zone_id", "slot_ts"])["demand"].reindex(idx, fill_value=0).reset_index()
    return grid.sort_values(["zone_id", "slot_ts"]).reset_index(drop=True)


def add_calendar(df: pd.DataFrame, holiday_dates: Optional[set] = None) -> pd.DataFrame:
    ts = df["slot_ts"]
    df["hour"] = ts.dt.hour
    df["minute30"] = (ts.dt.minute // 30).astype(int)
    df["dow"] = ts.dt.dayofweek
    df["is_weekend"] = (df["dow"] >= 5).astype(int)
    hol = holiday_dates if holiday_dates is not None else us_holidays(ts.dt.year.unique())
    df["is_holiday"] = ts.dt.date.isin(hol).astype(int)
    return df


def add_weather(df: pd.DataFrame, weather: Optional[pd.DataFrame]) -> pd.DataFrame:
    if weather is None or weather.empty:
        df["temp_c"], df["rain_mm"] = 10.0, 0.0
        return df
    w = weather[["hour_ts", "temp_c", "rain_mm"]].copy()
    w["hour_ts"] = pd.to_datetime(w["hour_ts"]).dt.tz_localize(None)
    df["hour_ts"] = df["slot_ts"].dt.floor("h")
    df = df.merge(w, on="hour_ts", how="left").drop(columns="hour_ts")
    df["temp_c"] = df["temp_c"].fillna(df["temp_c"].median() if df["temp_c"].notna().any() else 10.0)
    df["rain_mm"] = df["rain_mm"].fillna(0.0)
    return df


def build_features(slots: pd.DataFrame, weather: Optional[pd.DataFrame] = None,
                   holiday_dates: Optional[set] = None, drop_incomplete: bool = True) -> pd.DataFrame:
    df = full_grid(slots)
    g = df.groupby("zone_id")["demand"]
    df["lag_1"] = g.shift(1)
    df["lag_2"] = g.shift(2)
    df["lag_48"] = g.shift(48)
    df["lag_336"] = g.shift(336)
    prev = g.shift(1)
    df["roll_mean_4"] = prev.groupby(df["zone_id"]).transform(lambda s: s.rolling(4, min_periods=1).mean())
    df["roll_mean_8"] = prev.groupby(df["zone_id"]).transform(lambda s: s.rolling(8, min_periods=1).mean())
    df = add_calendar(df, holiday_dates)
    df = add_weather(df, weather)
    if drop_incomplete:
        df = df.dropna(subset=["lag_336"]).reset_index(drop=True)
    return df


def profile_tables(history: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """history: dense grid rows (zone_id, slot_ts, demand) incl. zero slots -> (profile table, zone_mean table)."""
    h = history[["zone_id", "slot_ts", "demand"]].copy()
    if "dow" not in history.columns:
        h = add_calendar(h, set())
    else:
        h[["dow", "hour", "minute30"]] = history[["dow", "hour", "minute30"]]
    g = h.groupby(PROFILE_KEYS)["demand"].agg(["sum", "count"]).reset_index()
    g["profile"] = g["sum"] / g["count"]
    zm = h.groupby("zone_id")["demand"].mean().rename("zone_mean").reset_index()
    return g, zm


def add_profile(train: pd.DataFrame, test: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Profile features from the training period only. Training rows get the leave-one-out mean (own target removed)."""
    g, zm = profile_tables(train)
    tr = train.merge(g[PROFILE_KEYS + ["sum", "count"]], on=PROFILE_KEYS, how="left").merge(zm, on="zone_id", how="left")
    loo = (tr["sum"] - tr["demand"]) / (tr["count"] - 1)
    tr["profile"] = loo.where(tr["count"] > 1)
    tr = tr.drop(columns=["sum", "count"])
    te = test.merge(g[PROFILE_KEYS + ["profile"]], on=PROFILE_KEYS, how="left").merge(zm, on="zone_id", how="left")
    for d in (tr, te):
        d["zone_mean"] = d["zone_mean"].fillna(0.0)
        d["profile"] = d["profile"].fillna(d["zone_mean"])
    return tr, te


def time_split(df: pd.DataFrame, test_frac: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Chronological split - no shuffling, so the test period is truly in the future of the training period."""
    ts = np.sort(df["slot_ts"].unique())
    cutoff = ts[int(len(ts) * (1 - test_frac))]
    return df[df["slot_ts"] < cutoff].copy(), df[df["slot_ts"] >= cutoff].copy()


def next_slot(now_local: pd.Timestamp) -> pd.Timestamp:
    """First 30-minute boundary strictly after `now`."""
    return now_local.floor("30min") + SLOT
