"""Generate the Power BI project (PBIP) of the dashboard.

    python dashboards/powerbi/build_pbip.py          # rewrites dashboards/powerbi/RideHailSurge.*

Output (text-based Power BI Project, opens in Power BI Desktop -> File -> Open -> RideHailSurge.pbip):
  RideHailSurge.pbip
  RideHailSurge.SemanticModel/   TMDL: 8 tables, relationships to dim_zone, 29 KPI measures, source parameters
  RideHailSurge.Report/          5 pages: Surge Heat Map, Rider ETA, Driver Utilisation, Revenue per km, Demand Forecast
Save it as .pbix from Desktop (File -> Save as) for the PDF deliverable "Power BI dashboard file (.pbix)".

Data source: the Gold tables in Databricks (Databricks connector, parameters DatabricksHost / DatabricksHttpPath /
DatabricksCatalog / DatabricksSchema), including dim_zone (zone names + centre for the map).
"""
import json
import os
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from gold_schema import DIM_ZONE, GOLD_SCHEMA  # noqa: E402

NAME = "RideHailSurge"
NS = uuid.UUID("6f1c1d5e-7f55-4a57-9b7b-2d6f0c0e0a01")
SM = os.path.join(HERE, f"{NAME}.SemanticModel")
RP = os.path.join(HERE, f"{NAME}.Report")


def tag(*parts) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def q(name: str) -> str:
    """TMDL object name quoting."""
    return name if name.replace("_", "").isalnum() else "'" + name.replace("'", "''") + "'"


def write(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


# ------------------------------------------------------------------ semantic model
TMDL_TYPE = {"int": "int64", "float": "double", "str": "string", "datetime": "dateTime", "bool": "boolean"}
M_TYPE = {"int": "Int64.Type", "float": "type number", "str": "type text", "datetime": "type datetime",
          "bool": "type logical"}
FORMAT = {"int": "0", "float": "#,0.00", "datetime": "General Date"}
DATA_CATEGORY = {"centroid_lat": "Latitude", "centroid_lon": "Longitude"}   # heat-map bubble at the zone centre

MEASURES = {
    "zone_hourly_kpis": [
        ("Total Requests", "SUM ( zone_hourly_kpis[requests] )", "#,0"),
        ("Completed Trips", "SUM ( zone_hourly_kpis[completed_trips] )", "#,0"),
        ("Cancelled Trips", "SUM ( zone_hourly_kpis[cancelled_trips] )", "#,0"),
        ("Cancellation Rate", "DIVIDE ( [Cancelled Trips], [Total Requests] )", "0.0%"),
        ("Total Revenue", "SUM ( zone_hourly_kpis[revenue] )", "$#,0"),
        ("Distance km", "SUM ( zone_hourly_kpis[distance_km] )", "#,0"),
        ("Revenue per km", "DIVIDE ( [Total Revenue], [Distance km] )", "$#,0.00"),
        ("Avg Rider ETA (min)",
         "DIVIDE (\n    SUMX ( zone_hourly_kpis, zone_hourly_kpis[avg_eta_min] * zone_hourly_kpis[completed_trips] ),\n"
         "    CALCULATE ( SUM ( zone_hourly_kpis[completed_trips] ), NOT ISBLANK ( zone_hourly_kpis[avg_eta_min] ) )\n)",
         "0.00"),
        ("ETA Target", "4.5", "0.0"),
        ("ETA vs Target", "[Avg Rider ETA (min)] - [ETA Target]", "+0.00;-0.00;0.00"),
        ("Avg Surge (hourly)", "AVERAGE ( zone_hourly_kpis[avg_surge] )", "0.00x"),
    ],
    "zone_minute_surge": [
        ("Current Surge",
         "VAR lastMinute = CALCULATE ( MAX ( zone_minute_surge[minute_ts] ), ALL ( zone_minute_surge ) )\n"
         "RETURN CALCULATE ( AVERAGE ( zone_minute_surge[multiplier] ), zone_minute_surge[minute_ts] = lastMinute )",
         "0.00x"),
        ("Current Demand/Supply Ratio",
         "VAR lastMinute = CALCULATE ( MAX ( zone_minute_surge[minute_ts] ), ALL ( zone_minute_surge ) )\n"
         "RETURN CALCULATE ( AVERAGE ( zone_minute_surge[ratio] ), zone_minute_surge[minute_ts] = lastMinute )",
         "0.00"),
        ("Surging Zones Now",
         "VAR lastMinute = CALCULATE ( MAX ( zone_minute_surge[minute_ts] ), ALL ( zone_minute_surge ) )\n"
         "RETURN CALCULATE ( DISTINCTCOUNT ( zone_minute_surge[zone_id] ), zone_minute_surge[minute_ts] = lastMinute,"
         " zone_minute_surge[multiplier] > 1 )", "0"),
        ("Max Surge", "MAX ( zone_minute_surge[multiplier] )", "0.00x"),
        ("Max Surge Today",
         "MAXX (\n    FILTER ( zone_minute_surge, INT ( zone_minute_surge[minute_ts] ) = INT ( MAX ( zone_minute_surge[minute_ts] ) ) ),\n"
         "    zone_minute_surge[multiplier]\n)", "0.00x"),
        ("Last Surge Refresh", "MAX ( zone_minute_surge[updated_at] )", "General Date"),
    ],
    "driver_daily_utilisation": [
        ("Driver Utilisation",
         "DIVIDE ( SUM ( driver_daily_utilisation[on_trip_pings] ), SUM ( driver_daily_utilisation[pings] ) )", "0.0%"),
        ("Utilisation Target", "0.78", "0%"),
        ("Utilisation vs Target", "[Driver Utilisation] - [Utilisation Target]", "+0.0%;-0.0%;0.0%"),
        ("Active Drivers", "DISTINCTCOUNT ( driver_daily_utilisation[driver_id] )", "#,0"),
    ],
    "zone_daily_utilisation": [
        ("Zone Utilisation",
         "DIVIDE ( SUM ( zone_daily_utilisation[on_trip_pings] ), SUM ( zone_daily_utilisation[pings] ) )", "0.0%"),
    ],
    "revenue_per_km": [
        ("Revenue per km (by weather)",
         "DIVIDE ( SUM ( revenue_per_km[revenue] ), SUM ( revenue_per_km[distance_km] ) )", "$#,0.00"),
    ],
    "demand_forecast": [
        ("Forecast Demand", "CALCULATE ( SUM ( demand_forecast[predicted_demand] ), demand_forecast[mode] = \"live\" )",
         "#,0.0"),
        ("Holdout Predicted",
         "CALCULATE ( SUM ( demand_forecast[predicted_demand] ), demand_forecast[mode] = \"holdout\" )", "#,0.0"),
        ("Holdout Actual",
         "CALCULATE ( SUM ( demand_forecast[actual_demand] ), demand_forecast[mode] = \"holdout\" )", "#,0.0"),
        ("Forecast Error (MAE)",
         "AVERAGEX (\n    FILTER ( demand_forecast, demand_forecast[mode] = \"holdout\" ),\n"
         "    ABS ( demand_forecast[predicted_demand] - demand_forecast[actual_demand] )\n)", "0.00"),
    ],
    "driver_payouts": [
        ("Total Payout", "SUM ( driver_payouts[total_payout] )", "$#,0.00"),
        ("Drivers with Bonus", "CALCULATE ( COUNTROWS ( driver_payouts ), driver_payouts[bonus] > 0 )", "#,0"),
    ],
}


def m_query(table: str, cols) -> str:
    """Power Query of one table: read it from Databricks with the Databricks connector."""
    names = ", ".join(f'"{c}"' for c, _ in cols)
    types = ", ".join(f'{{"{c}", {M_TYPE[t]}}}' for c, t in cols)
    remote = (f'Databricks.Catalogs(DatabricksHost, DatabricksHttpPath, [Catalog=null, Database=null, '
              f'EnableAutomaticProxyDiscovery=null]){{[Name=DatabricksCatalog, Kind="Database"]}}[Data]'
              f'{{[Name=DatabricksSchema, Kind="Schema"]}}[Data]{{[Name="{table}", Kind="Table"]}}[Data]')
    lines = ["let", "    Source =", f"        {remote},",
             f"    Selected = Table.SelectColumns(Source, {{{names}}}, MissingField.UseNull),",
             f'    Typed = Table.TransformColumnTypes(Selected, {{{types}}}, "en-US")',
             "in",
             "    Typed"]
    return "\n".join(lines)


def table_tmdl(table: str, cols, mq: str, extra_cols=()) -> str:
    out = [f"table {table}", f"\tlineageTag: {tag(table)}", ""]
    for name, expr, fmt in MEASURES.get(table, []):
        if "\n" in expr:
            out.append(f"\tmeasure {q(name)} =")
            out += [f"\t\t\t{ln}" for ln in expr.split("\n")]
        else:
            out.append(f"\tmeasure {q(name)} = {expr}")
        out += [f"\t\tformatString: {fmt}", f"\t\tlineageTag: {tag(table, 'm', name)}", ""]
    for c, t in cols:
        out += [f"\tcolumn {q(c)}", f"\t\tdataType: {TMDL_TYPE[t]}"]
        if t in FORMAT:
            out.append(f"\t\tformatString: {'0.000000' if c in DATA_CATEGORY else FORMAT[t]}")
        if c in DATA_CATEGORY:
            out.append(f"\t\tdataCategory: {DATA_CATEGORY[c]}")
        none = t in ("str", "datetime", "bool") or c.endswith("_id") or c == "hour_of_day" or c in DATA_CATEGORY
        out += [f"\t\tlineageTag: {tag(table, 'c', c)}",
                f"\t\tsummarizeBy: {'none' if none else 'sum'}",
                f"\t\tsourceColumn: {c}", "",
                "\t\tannotation SummarizationSetBy = Automatic", ""]
    for line in extra_cols:
        out += line
    out += [f"\tpartition {table} = m", "\t\tmode: import", "\t\tsource ="]
    out += [f"\t\t\t\t{ln}" for ln in mq.split("\n")]
    out += ["", "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(out)


def build_model() -> None:
    d = os.path.join(SM, "definition")
    write(os.path.join(SM, "definition.pbism"), json.dumps({"version": "4.0", "settings": {}}, indent=2) + "\n")
    write(os.path.join(d, "database.tmdl"), "database\n\tcompatibilityLevel: 1567\n\n")
    tables = list(GOLD_SCHEMA) + ["dim_zone"]
    write(os.path.join(d, "model.tmdl"),
          "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\tsourceQueryCulture: en-US\n"
          "\tdataAccessOptions\n\t\tlegacyRedirects\n\t\treturnErrorValuesAsNull\n\n"
          "\tannotation PBI_QueryOrder = " + json.dumps(["DatabricksHost", "DatabricksHttpPath", "DatabricksCatalog",
                                                       "DatabricksSchema"] + tables) + "\n\n"
          + "".join(f"ref table {t}\n" for t in tables) + "\n")
    params = [
        ("DatabricksHost", '"dbc-xxxxxxxx-xxxx.cloud.databricks.com" meta [IsParameterQuery=true, Type="Text", '
                           'IsParameterQueryRequired=true]'),
        ("DatabricksHttpPath", '"/sql/1.0/warehouses/xxxxxxxxxxxxxxxx" meta [IsParameterQuery=true, Type="Text", '
                               'IsParameterQueryRequired=true]'),
        ("DatabricksCatalog", '"workspace" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'),
        ("DatabricksSchema", '"ridehail" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'),
    ]
    exp = []
    for n, v in params:
        exp += [f"expression {n} = {v}", f"\tlineageTag: {tag('param', n)}", "", "\tannotation PBI_ResultType = Text", ""]
    write(os.path.join(d, "expressions.tmdl"), "\n".join(exp))
    # the generator owns tables/: drop stale files (old table names, Windows ":Zone.Identifier" copies, ...)
    tdir = os.path.join(d, "tables")
    keep = {f"{t}.tmdl" for t in GOLD_SCHEMA} | {"dim_zone.tmdl"}
    if os.path.isdir(tdir):
        for f in os.listdir(tdir):
            if f not in keep:
                os.remove(os.path.join(tdir, f))
    for t, cols in GOLD_SCHEMA.items():
        write(os.path.join(d, "tables", f"{t}.tmdl"), table_tmdl(t, cols, m_query(t, cols)))
    map_col = [[
        "\tcolumn MapLocation = dim_zone[Zone] & \", \" & dim_zone[Borough] & \", New York, NY\"",
        "\t\tdataType: string",
        f"\t\tlineageTag: {tag('dim_zone', 'c', 'MapLocation')}",
        "\t\tdataCategory: Place",
        "\t\tsummarizeBy: none", "",
        "\t\tannotation SummarizationSetBy = Automatic", ""]]
    write(os.path.join(d, "tables", "dim_zone.tmdl"),
          table_tmdl("dim_zone", DIM_ZONE,
                     m_query("dim_zone", DIM_ZONE),
                     extra_cols=map_col))
    rel = []
    for t, cols in GOLD_SCHEMA.items():
        if any(c == "zone_id" for c, _ in cols):
            rel += [f"relationship {tag('rel', t)}", f"\tfromColumn: {t}.zone_id", "\ttoColumn: dim_zone.zone_id", ""]
    write(os.path.join(d, "relationships.tmdl"), "\n".join(rel))


# ------------------------------------------------------------------ report (PBIR: definition/ folder)
# Power BI Desktop (September 2026 and later) uses the PBIR report format. Schema versions are the ones the official
# Microsoft CLI pins (`powerbi-report-author scaffold --offline`); Desktop accepts them and upgrades them on save.
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric"
S_VISUAL = f"{SCHEMA}/item/report/definition/visualContainer/2.9.0/schema.json"
S_PAGE = f"{SCHEMA}/item/report/definition/page/2.1.0/schema.json"
S_PAGES = f"{SCHEMA}/item/report/definition/pagesMetadata/1.1.0/schema.json"
S_REPORT = f"{SCHEMA}/item/report/definition/report/3.3.0/schema.json"
S_VERSION = f"{SCHEMA}/item/report/definition/versionMetadata/1.0.0/schema.json"
S_PBIR = f"{SCHEMA}/item/report/definitionProperties/2.0.0/schema.json"
S_PBIP = f"{SCHEMA}/pbip/pbipProperties/1.0.0/schema.json"
S_PLATFORM = f"{SCHEMA}/gitIntegration/platformProperties/2.0.0/schema.json"
BASE_THEME = "Fluent2-CY26SU10"          # shipped in RideHailSurge.Report/StaticResources/SharedResources/BaseThemes/


def lit(v: str) -> dict:
    return {"expr": {"Literal": {"Value": v}}}


class Page:
    def __init__(self, name: str, title: str):
        self.name, self.title, self.visuals = name, title, []
        self.textbox(title, 20, 10, 900, 50, 20)

    def _vname(self) -> str:
        return uuid.uuid5(NS, f"visual/{self.name}/{len(self.visuals)}").hex[:20]

    def _add(self, visual: dict, x, y, w, h):
        z = len(self.visuals) * 1000
        self.visuals.append({"$schema": S_VISUAL, "name": self._vname(),
                             "position": {"x": x, "y": y, "z": z, "height": h, "width": w, "tabOrder": z},
                             "visual": visual})

    def textbox(self, text, x, y, w, h, size=12):
        self._add({"visualType": "textbox",
                   "objects": {"general": [{"properties": {"paragraphs": [{"textRuns": [
                       {"value": text, "textStyle": {"fontSize": f"{size}pt", "fontWeight": "bold"}}]}]}}]},
                   "visualContainerObjects": {"background": [{"properties": {"show": lit("false")}}]}},
                  x, y, w, h)

    def visual(self, vtype, x, y, w, h, roles: dict, title: str = None):
        """roles: {"Category": [("dim_zone","Zone","col")], "Y": [("zone_hourly_kpis","Total Requests","measure")]}"""
        state = {}
        for role, fields in roles.items():
            projs = []
            for ent, prop, kind in fields:
                key = "Measure" if kind == "measure" else "Column"
                pr = {"field": {key: {"Expression": {"SourceRef": {"Entity": ent}}, "Property": prop}},
                      "queryRef": f"{ent}.{prop}", "nativeQueryRef": prop}
                if role == "Category":
                    pr["active"] = True
                projs.append(pr)
            state[role] = {"projections": projs}
        v = {"visualType": vtype, "query": {"queryState": state}, "drillFilterOtherVisuals": True}
        if title:
            v["visualContainerObjects"] = {"title": [{"properties": {
                "show": lit("true"), "text": lit("'" + title.replace("'", "''") + "'")}}]}
        self._add(v, x, y, w, h)

    def files(self) -> dict:
        out = {f"pages/{self.name}/page.json": {"$schema": S_PAGE, "name": self.name, "displayName": self.title,
                                                "displayOption": "FitToPage", "height": 720, "width": 1280}}
        for v in self.visuals:
            out[f"pages/{self.name}/visuals/{v['name']}/visual.json"] = v
        return out


M, C = "measure", "col"


def build_pages() -> list:
    pages = []
    p = Page("surge_heat_map", "Surge Heat Map")
    # Azure Maps bubble layer: one bubble per taxi zone at its centre (Y = latitude, X = longitude), sized by surge
    p.visual("azureMap", 20, 70, 760, 630, {"Category": [("dim_zone", "MapLocation", C)],
                                            "Y": [("dim_zone", "centroid_lat", C)],
                                            "X": [("dim_zone", "centroid_lon", C)],
                                            "Size": [("zone_minute_surge", "Current Surge", M)],
                                            "Tooltips": [("zone_minute_surge", "Current Demand/Supply Ratio", M)]},
             "Current surge multiplier by taxi zone")
    p.visual("cardVisual", 800, 70, 225, 110, {"Data": [("zone_minute_surge", "Current Surge", M)]}, "Avg surge now")
    p.visual("cardVisual", 1035, 70, 225, 110, {"Data": [("zone_minute_surge", "Surging Zones Now", M)]}, "Zones surging")
    p.visual("cardVisual", 800, 190, 460, 90, {"Data": [("zone_minute_surge", "Last Surge Refresh", M)]},
             "Last refresh (target: every 60 s)")
    p.visual("clusteredBarChart", 800, 290, 460, 330, {"Category": [("dim_zone", "Zone", C)],
                                                       "Y": [("zone_minute_surge", "Current Surge", M)]},
             "Top zones by current surge")
    p.visual("slicer", 800, 630, 460, 70, {"Values": [("dim_zone", "Borough", C)]}, "Borough")
    pages.append(p)

    p = Page("rider_eta", "Rider ETA")
    p.visual("cardVisual", 20, 70, 300, 120, {"Data": [("zone_hourly_kpis", "Avg Rider ETA (min)", M)]},
             "Avg rider ETA (min) - target < 4.5")
    p.visual("cardVisual", 330, 70, 300, 120, {"Data": [("zone_hourly_kpis", "Cancellation Rate", M)]}, "Cancellation rate")
    p.visual("cardVisual", 640, 70, 300, 120, {"Data": [("zone_hourly_kpis", "Completed Trips", M)]}, "Completed trips")
    p.visual("slicer", 950, 70, 310, 120, {"Values": [("zone_hourly_kpis", "source", C)]}, "history / live")
    p.visual("lineChart", 20, 200, 1240, 300, {"Category": [("zone_hourly_kpis", "hour_ts", C)],
                                               "Y": [("zone_hourly_kpis", "Avg Rider ETA (min)", M),
                                                     ("zone_hourly_kpis", "ETA Target", M)]},
             "Average rider ETA vs 4.5-minute target")
    p.visual("clusteredBarChart", 20, 510, 1240, 190, {"Category": [("dim_zone", "Borough", C)],
                                                       "Y": [("zone_hourly_kpis", "Avg Rider ETA (min)", M)]},
             "ETA by borough")
    pages.append(p)

    p = Page("driver_utilisation", "Driver Utilisation")
    p.visual("gauge", 20, 70, 500, 320, {"Y": [("driver_daily_utilisation", "Driver Utilisation", M)],
                                         "TargetValue": [("driver_daily_utilisation", "Utilisation Target", M)]},
             "Driver utilisation vs 78% target")
    p.visual("cardVisual", 540, 70, 230, 150, {"Data": [("driver_daily_utilisation", "Active Drivers", M)]}, "Active drivers")
    p.visual("cardVisual", 780, 70, 230, 150, {"Data": [("driver_payouts", "Total Payout", M)]}, "Incentive payout")
    p.visual("cardVisual", 1020, 70, 240, 150, {"Data": [("driver_payouts", "Drivers with Bonus", M)]}, "Drivers with bonus")
    p.visual("clusteredColumnChart", 540, 230, 720, 160, {"Category": [("driver_daily_utilisation", "date", C)],
                                                          "Y": [("driver_daily_utilisation", "Driver Utilisation", M)]},
             "Daily utilisation")
    p.visual("clusteredBarChart", 20, 400, 600, 300, {"Category": [("dim_zone", "Zone", C)],
                                                      "Y": [("zone_daily_utilisation", "Zone Utilisation", M)]},
             "Utilisation by zone")
    p.visual("tableEx", 640, 400, 620, 300, {"Values": [("driver_payouts", "driver_id", C), ("driver_payouts", "date", C),
                                                        ("driver_payouts", "trips", C), ("driver_payouts", "utilisation", C),
                                                        ("driver_payouts", "bonus", C), ("driver_payouts", "total_payout", C)]},
             "Daily driver incentive payouts")
    pages.append(p)

    p = Page("revenue_per_km", "Revenue per km")
    p.visual("cardVisual", 20, 70, 300, 120, {"Data": [("zone_hourly_kpis", "Revenue per km", M)]}, "Revenue per km")
    p.visual("cardVisual", 330, 70, 300, 120, {"Data": [("zone_hourly_kpis", "Total Revenue", M)]}, "Revenue")
    p.visual("clusteredColumnChart", 640, 70, 620, 300, {"Category": [("revenue_per_km", "weather_bucket", C)],
                                                         "Y": [("revenue_per_km", "Revenue per km (by weather)", M)]},
             "Revenue per km by weather")
    p.visual("lineChart", 20, 200, 610, 170, {"Category": [("revenue_per_km", "hour_of_day", C)],
                                              "Y": [("revenue_per_km", "Revenue per km (by weather)", M)]},
             "Revenue per km by hour of day")
    p.visual("clusteredBarChart", 20, 380, 1240, 320, {"Category": [("dim_zone", "Zone", C)],
                                                       "Y": [("zone_hourly_kpis", "Revenue per km", M)]},
             "Revenue per km by pickup zone")
    pages.append(p)

    p = Page("demand_forecast", "Demand Forecast")
    p.visual("cardVisual", 20, 70, 300, 120, {"Data": [("demand_forecast", "Forecast Error (MAE)", M)]},
             "Holdout MAE (requests / 30 min)")
    p.visual("cardVisual", 330, 70, 300, 120, {"Data": [("demand_forecast", "Forecast Demand", M)]},
             "Next 30-min forecast (all zones)")
    p.visual("slicer", 640, 70, 620, 120, {"Values": [("dim_zone", "Zone", C)]}, "Zone")
    p.visual("lineChart", 20, 200, 1240, 260, {"Category": [("demand_forecast", "forecast_ts", C)],
                                               "Y": [("demand_forecast", "Holdout Predicted", M),
                                                     ("demand_forecast", "Holdout Actual", M)]},
             "Forecast vs actual (held-out period)")
    p.visual("clusteredBarChart", 20, 470, 1240, 230, {"Category": [("dim_zone", "Zone", C)],
                                                       "Y": [("demand_forecast", "Forecast Demand", M)]},
             "Next 30-minute demand forecast by zone")
    pages.append(p)
    return pages


def build_report() -> None:
    import shutil
    pages = build_pages()
    d = os.path.join(RP, "definition")
    # the generator owns definition/ and the legacy report.json (old PBIR-Legacy layout) must not exist next to it
    if os.path.isdir(d):
        shutil.rmtree(d)
    if os.path.exists(os.path.join(RP, "report.json")):
        os.remove(os.path.join(RP, "report.json"))
    dump = lambda path, obj: write(os.path.join(RP, path), json.dumps(obj, indent=2) + "\n")  # noqa: E731
    dump("definition/version.json", {"$schema": S_VERSION, "version": "2.0.0"})
    dump("definition/report.json", {
        "$schema": S_REPORT,
        "themeCollection": {"baseTheme": {"name": BASE_THEME, "reportVersionAtImport": {
            "visual": "2.11.0", "report": "3.4.0", "page": "2.3.1"}, "type": "SharedResources"}},
        "resourcePackages": [{"name": "SharedResources", "type": "SharedResources", "items": [
            {"name": BASE_THEME, "path": f"BaseThemes/{BASE_THEME}.json", "type": "BaseTheme"}]}],
        "settings": {"useStylableVisualContainerHeader": True, "exportDataMode": "AllowSummarized",
                     "defaultDrillFilterOtherVisuals": True, "allowChangeFilterTypes": True,
                     "useEnhancedTooltips": True, "useDefaultAggregateDisplayName": True}})
    dump("definition/pages/pages.json", {"$schema": S_PAGES, "pageOrder": [p.name for p in pages],
                                         "activePageName": pages[0].name})
    for p in pages:
        for path, obj in p.files().items():
            dump(f"definition/{path}", obj)
    dump("definition.pbir", {"$schema": S_PBIR, "version": "4.0",
                             "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}})
    dump(".platform", {"$schema": S_PLATFORM, "metadata": {"type": "Report", "displayName": NAME},
                       "config": {"version": "2.0", "logicalId": str(uuid.uuid5(NS, "report"))}})
    write(os.path.join(HERE, f"{NAME}.pbip"), json.dumps(
        {"$schema": S_PBIP, "version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
         "settings": {"enableAutoRecovery": True}}, indent=2) + "\n")


if __name__ == "__main__":
    build_model()
    build_report()
    print(f"[powerbi] wrote {NAME}.pbip, {NAME}.SemanticModel/, {NAME}.Report/ in {HERE}")
