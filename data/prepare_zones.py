"""Build data/zone_grid.json from the TLC taxi-zone shapefile (point-in-polygon, pre-computed).

Usage: python -m data.prepare_zones
"""
import glob
import json
import math
import sys

import geopandas as gpd
import numpy as np

from common.config import TLC_DIR, ZONE_GRID_PATH

CELL = 0.002  # degrees (~220 m)


def build(shp_path: str, out_path: str, cell: float = CELL) -> dict:
    zones = gpd.read_file(shp_path).to_crs(4326)
    zones = zones[["LocationID", "geometry"]].dissolve(by="LocationID").reset_index()
    zones = zones[zones.geometry.notna() & ~zones.geometry.is_empty]

    minx, miny, maxx, maxy = zones.total_bounds
    lat0 = math.floor(miny / cell) * cell
    lon0 = math.floor(minx / cell) * cell
    n_i = int(math.ceil((maxy - lat0) / cell))
    n_j = int(math.ceil((maxx - lon0) / cell))
    ii, jj = np.meshgrid(np.arange(n_i), np.arange(n_j), indexing="ij")
    ii, jj = ii.ravel(), jj.ravel()
    lats = lat0 + (ii + 0.5) * cell
    lons = lon0 + (jj + 0.5) * cell
    pts = gpd.GeoDataFrame({"i": ii, "j": jj}, geometry=gpd.points_from_xy(lons, lats), crs=4326)

    joined = gpd.sjoin(pts, zones, predicate="within", how="inner")
    joined = joined[~joined.index.duplicated(keep="first")]
    cells = {f"{int(i)}_{int(j)}": int(z) for i, j, z in zip(joined["i"], joined["j"], joined["LocationID"])}

    reps = zones.geometry.representative_point()
    centroids = {int(z): [float(p.y), float(p.x)] for z, p in zip(zones["LocationID"], reps)}

    # make sure tiny zones own at least one cell
    owned = set(cells.values())
    for z, (la, lo) in centroids.items():
        if z not in owned:
            cells[f"{int((la - lat0) // cell)}_{int((lo - lon0) // cell)}"] = z

    data = {"cell_size": cell, "origin_lat": lat0, "origin_lon": lon0, "cells": cells, "centroids": centroids}
    with open(out_path, "w") as fh:
        json.dump(data, fh)
    print(f"[zones] {len(centroids)} zones, {len(cells)} grid cells -> {out_path}")
    return data


def main() -> int:
    shp = glob.glob(f"{TLC_DIR}/taxi_zones/**/*.shp", recursive=True)
    if not shp:
        print("taxi_zones.shp not found - run `python -m data.download` first", file=sys.stderr)
        return 1
    build(shp[0], ZONE_GRID_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
