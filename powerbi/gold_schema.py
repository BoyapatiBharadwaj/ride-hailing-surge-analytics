"""Columns of the Databricks Gold tables that Power BI loads (input of build_pbip.py)."""

# type names: int | float | str | datetime | bool
GOLD_SCHEMA = {
    "zone_minute_surge": [("zone_id", "int"), ("minute_ts", "datetime"), ("demand", "float"), ("supply", "float"),
                          ("ratio", "float"), ("multiplier", "float"), ("rain_mm", "float"), ("temp_c", "float"),
                          ("updated_at", "datetime"), ("decayed", "bool")],
    "zone_hourly_kpis": [("zone_id", "int"), ("hour_ts", "datetime"), ("source", "str"), ("requests", "int"),
                         ("completed_trips", "int"), ("cancelled_trips", "int"), ("avg_eta_min", "float"),
                         ("revenue", "float"), ("distance_km", "float"), ("avg_surge", "float"), ("avg_supply", "float")],
    "driver_daily_utilisation": [("driver_id", "int"), ("date", "datetime"), ("pings", "int"), ("on_trip_pings", "int"),
                                 ("online_min", "float"), ("on_trip_min", "float"), ("utilisation", "float"),
                                 ("trips", "int"), ("revenue", "float"), ("surge_trips", "int")],
    "zone_daily_utilisation": [("zone_id", "int"), ("date", "datetime"), ("pings", "int"), ("on_trip_pings", "int"),
                               ("utilisation", "float")],
    "revenue_per_km": [("zone_id", "int"), ("hour_of_day", "int"), ("weather_bucket", "str"), ("source", "str"),
                       ("trips", "int"), ("revenue", "float"), ("distance_km", "float"), ("revenue_per_km", "float")],
    "demand_forecast": [("zone_id", "int"), ("forecast_ts", "datetime"), ("predicted_demand", "float"),
                        ("actual_demand", "float"), ("model_version", "str"), ("scored_at", "datetime"), ("mode", "str")],
    "driver_payouts": [("driver_id", "int"), ("date", "datetime"), ("trips", "int"), ("online_min", "float"),
                       ("utilisation", "float"), ("revenue", "float"), ("surge_trips", "int"), ("bonus", "float"),
                       ("surge_extra", "float"), ("total_payout", "float")],
}
# Databricks dim_zone: zone names + zone centre (representative point of the TLC polygon) = heat-map bubble position
DIM_ZONE = [("zone_id", "int"), ("Borough", "str"), ("Zone", "str"), ("service_zone", "str"),
            ("centroid_lat", "float"), ("centroid_lon", "float")]
