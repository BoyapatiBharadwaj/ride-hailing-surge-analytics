"""Train the 30-minute demand forecast: Linear Regression (baseline) + Random Forest, tracked in MLflow.

    python -m ml.train
Data       : /lake/export/demand_30min (requests per zone per 30 minutes, one month of history from Hive)
Validation : chronological split - the last 20 % of the month is the held-out test set; R2, MAE, RMSE
Target     : R2 >= 0.70 on the held-out period
Outputs    : /lake/models/demand_model.joblib (best model), /lake/reports/forecast_metrics.json,
             Databricks Gold demand_forecast (hold-out predictions next to the actual demand, for Power BI)
MLflow     : the Databricks workspace (Experiments > ridehail-demand-forecast) when the keys are set, else /lake/mlruns
"""
import json
import os
import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from common.config import DATA_DIR, EXPORT, LAKE, MODELS, REPORTS, TARGET_R2, databricks_configured, env
from ml.features import FEATURES, add_profile, build_features, time_split


def mlflow_setup():
    """Databricks has MLflow built in: runs go to the workspace when the keys are set, otherwise to a local folder."""
    import mlflow
    if databricks_configured():
        try:
            from cloud.databricks.workspace import client, host
            os.environ["DATABRICKS_HOST"], os.environ["DATABRICKS_TOKEN"] = host(), env("DATABRICKS_TOKEN")
            mlflow.set_tracking_uri("databricks")
            name = f"/Users/{client().current_user.me().user_name}/ridehail-demand-forecast"
            mlflow.set_experiment(name)
            return mlflow, f"Databricks experiment {name}"
        except Exception as exc:  # noqa: BLE001
            print(f"[ml] Databricks MLflow not reachable ({type(exc).__name__}) - logging locally")
    mlflow.set_tracking_uri(f"file://{LAKE}/mlruns")
    mlflow.set_experiment("ridehail-demand-forecast")
    return mlflow, f"file://{LAKE}/mlruns"


def evaluate(y, p) -> dict:
    p = np.clip(p, 0, None)
    return {"r2": float(r2_score(y, p)), "mae": float(mean_absolute_error(y, p)),
            "rmse": float(np.sqrt(mean_squared_error(y, p)))}


def write_gold_forecast(test: pd.DataFrame, pred: np.ndarray, version: str) -> None:
    if not databricks_configured():
        print("[ml] Databricks not configured - hold-out predictions not written to Gold")
        return
    from cloud.databricks.workspace import write_table
    out = pd.DataFrame({
        "zone_id": test["zone_id"].astype("int32").to_numpy(),
        "forecast_ts": pd.to_datetime(test["slot_ts"]).to_numpy(),
        "predicted_demand": np.clip(pred, 0, None).astype(float),
        "actual_demand": test["demand"].astype(float).to_numpy(),
        "model_version": version, "scored_at": pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)),
        "mode": "holdout"})
    write_table(out, "demand_forecast", mode="overwrite")
    print(f"[ml] Databricks Gold demand_forecast <- {len(out):,} hold-out rows")


def main() -> int:
    src = f"{EXPORT}/demand_30min"
    if not os.path.isdir(src):
        print("demand history missing - run the nightly batch DAG (batch/export.py) first", file=sys.stderr)
        return 1
    slots = pd.read_parquet(src)
    wpath = f"{DATA_DIR}/weather_hourly.parquet"
    weather = pd.read_parquet(wpath) if os.path.exists(wpath) else None
    df = build_features(slots, weather)
    if df.empty:
        days = pd.to_datetime(slots["slot_ts"]).dt.date.nunique()
        print(f"[ml] only {days} day(s) of trip history - the features need more than 7 days (same time last week). "
              "The Hive import is incomplete: re-import the trips (see README, 'If something goes wrong').", file=sys.stderr)
        return 1
    train, test = time_split(df, 0.2)
    train, test = add_profile(train, test)
    print(f"[ml] rows train={len(train):,} test={len(test):,} | test period {test['slot_ts'].min()} .. {test['slot_ts'].max()}")

    models = {
        "linear_regression": LinearRegression(),
        # sized for the 2 GB tools container: 120 trees / leaf 3 peaked at ~1.3 GB RAM and a 370 MB model on ~250k
        # rows; 80 trees on 50 % bootstrap samples with leaf 5 peaks at ~0.6 GB, 120 MB, 2.5x faster
        "random_forest": RandomForestRegressor(n_estimators=80, max_depth=18, min_samples_leaf=5, max_samples=0.5,
                                               n_jobs=2, random_state=42),
    }
    mlflow, uri = mlflow_setup()
    results, fitted = {}, {}
    for name, model in models.items():
        with mlflow.start_run(run_name=name):
            model.fit(train[FEATURES], train["demand"])
            pred = model.predict(test[FEATURES])
            m = evaluate(test["demand"], pred)
            mlflow.log_params({"model": name, "n_train": len(train), "n_test": len(test), "features": ",".join(FEATURES),
                               **{f"hp_{k}": v for k, v in model.get_params().items() if k in ("n_estimators", "max_depth", "min_samples_leaf", "max_samples")}})
            mlflow.log_metrics(m)
        results[name], fitted[name] = m, model
        print(f"[ml] {name:18s} R2={m['r2']:.3f}  MAE={m['mae']:.3f}  RMSE={m['rmse']:.3f}")

    best = max(results, key=lambda k: results[k]["r2"])
    version = f"{best}-{datetime.now(timezone.utc):%Y%m%d%H%M}"
    os.makedirs(MODELS, exist_ok=True)
    joblib.dump({"model": fitted[best], "features": FEATURES, "name": best, "version": version, "metrics": results[best]},
                f"{MODELS}/demand_model.joblib")
    report = {"best_model": best, "version": version, "metrics": results, "target_r2": TARGET_R2,
              "pass": results[best]["r2"] >= TARGET_R2, "mlflow_uri": uri,
              "test_period": [str(test["slot_ts"].min()), str(test["slot_ts"].max())]}
    os.makedirs(REPORTS, exist_ok=True)
    json.dump(report, open(f"{REPORTS}/forecast_metrics.json", "w"), indent=2)
    write_gold_forecast(test, fitted[best].predict(test[FEATURES]), version)
    print(f"[ml] best={best}  R2={results[best]['r2']:.3f}  target >= {TARGET_R2}  -> {'PASS' if report['pass'] else 'FAIL'}")
    if not report["pass"]:
        print("[ml] R2 below target: demand per zone per 30 min is a small, noisy count - raise SEED_MAX_ROWS in .env "
              "(2,000,000 = every 10th trip) for denser counts, then reseed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
