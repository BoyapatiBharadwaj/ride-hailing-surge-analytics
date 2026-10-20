# Power BI dashboard

Five pages, all read from the **Gold tables in Databricks** (`workspace.ridehail`) with the Databricks connector:
**Surge Heat Map**, **Rider ETA** (target 4.5 min), **Driver Utilisation** gauge (target 78 %), **Revenue per km**
(zone, hour, weather) and **Demand Forecast** vs actual. Power BI Desktop runs on Windows.

## Files
| File | What it is |
|---|---|
| `RideHailSurge.pbip` | the Power BI project - open this in Power BI Desktop |
| `RideHailSurge.SemanticModel/` | the model: 7 Gold tables + `dim_zone`, relationships on `zone_id`, 29 measures, 4 connection parameters |
| `RideHailSurge.Report/` | the 5 report pages |
| `ridehail_theme.json` | colour theme |
| `build_pbip.py`, `gold_schema.py` | regenerate the project: `python dashboards/powerbi/build_pbip.py` |

## 1. Before you open it
The Gold tables must exist in Databricks: the Airflow DAGs build them (or run `make dbx` once).
From Databricks you need: **Server hostname** and **HTTP path** (SQL Warehouses -> your warehouse -> Connection details)
and a **personal access token** (the same values as `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` in `.env`).

## 2. Open and connect
1. Copy the folder `dashboards/powerbi` to the Windows PC (for example `C:\ridehail\powerbi`).
2. Power BI Desktop -> *File -> Open* -> `RideHailSurge.pbip`.
3. *Home -> Transform data -> Edit parameters*: `DatabricksHost` = `dbc-xxxx.cloud.databricks.com` (without https://),
   `DatabricksHttpPath` = `/sql/1.0/warehouses/...` (keep `DatabricksCatalog` = `workspace`, `DatabricksSchema` = `ridehail`).
4. *Refresh* -> sign in with **Personal Access Token** and paste the token.
5. *View -> Themes -> Browse for themes* -> `ridehail_theme.json`.
6. *File -> Save as* -> `RideHailSurge.pbix` (the dashboard file to hand in).

The heat map uses the **Azure Map** visual. If it shows a message instead of a map, enable it in
*File -> Options and settings -> Options -> Security -> Map and Filled Map visuals / Azure Map*.

## 3. Pages
| Page | Visuals | Key measures |
|---|---|---|
| Surge Heat Map | Azure Map: one bubble per taxi zone at its centre, sized by the current multiplier; cards; top zones; borough slicer | `Current Surge`, `Current Demand/Supply Ratio`, `Surging Zones Now`, `Last Surge Refresh` |
| Rider ETA | KPI cards, ETA vs target by hour, ETA by borough, history/live slicer | `Avg Rider ETA (min)`, `ETA Target`, `Cancellation Rate` |
| Driver Utilisation | gauge with the 78 % target, daily utilisation, utilisation by zone, payout table | `Driver Utilisation`, `Utilisation Target`, `Total Payout`, `Drivers with Bonus` |
| Revenue per km | cards, revenue/km by weather, by hour of day, by pickup zone | `Revenue per km`, `Revenue per km (by weather)` |
| Demand Forecast | hold-out MAE card, forecast vs actual, next-30-minute forecast by zone | `Forecast Demand`, `Holdout Predicted`, `Holdout Actual`, `Forecast Error (MAE)` |

**Live heat map:** set `zone_minute_surge` to **DirectQuery** (*Model view -> table -> Properties -> Advanced -> Storage mode*)
and *Format page -> Page refresh -> 1 minute* on the Surge Heat Map page.

## 4. If the project does not open (older Power BI Desktop)
Update Power BI Desktop, or build it by hand: *Get data -> Azure Databricks* (hostname, HTTP path, token) and import the
8 tables; relate every `zone_id` to `dim_zone[zone_id]`; copy the measures from
`RideHailSurge.SemanticModel/definition/tables/*.tmdl` (lines starting with `measure`); set `dim_zone[centroid_lat]` /
`[centroid_lon]` to the Latitude / Longitude data category; build the pages from the table above.
