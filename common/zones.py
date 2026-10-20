"""Zone lookup built from the TLC taxi-zone polygons.

`data/prepare_zones.py` pre-computes a lat/lon grid (default 0.002 deg cells) in which every cell is
assigned to the TLC LocationID whose polygon contains the cell centre. This is the PDF's
"point-in-polygon on the TLC zone shapes, pre-computed" step, and it keeps the Spark UDF pure-Python
(no shapely needed on the Spark workers).
"""
import json
import math
import random
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from common.config import ZONE_GRID_PATH

EARTH_KM = 6371.0088


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_KM * math.asin(math.sqrt(a))


class ZoneGrid:
    def __init__(self, data: dict):
        self.cell = data["cell_size"]
        self.lat0 = data["origin_lat"]
        self.lon0 = data["origin_lon"]
        self.cells: Dict[str, int] = {k: int(v) for k, v in data["cells"].items()}
        self.centroids: Dict[int, Tuple[float, float]] = {int(k): tuple(v) for k, v in data["centroids"].items()}
        zc: Dict[int, List[str]] = {}
        for key, z in self.cells.items():
            zc.setdefault(z, []).append(key)
        self.zone_cells = zc
        self.zones = sorted(self.centroids)
        self._neighbors: Dict[int, List[int]] = {}

    # ----- lookups
    def _key(self, lat: float, lon: float) -> str:
        return f"{int((lat - self.lat0) // self.cell)}_{int((lon - self.lon0) // self.cell)}"

    def zone_of(self, lat: Optional[float], lon: Optional[float]) -> int:
        """Zone id for a coordinate, 0 when outside every zone."""
        if lat is None or lon is None:
            return 0
        return self.cells.get(self._key(lat, lon), 0)

    def random_point(self, zone: int, rng: random.Random) -> Tuple[float, float]:
        keys = self.zone_cells.get(zone)
        if not keys:
            lat, lon = self.centroids[zone]
            return lat, lon
        i, j = (int(x) for x in rng.choice(keys).split("_"))
        return (self.lat0 + (i + rng.random()) * self.cell, self.lon0 + (j + rng.random()) * self.cell)

    def distance_km(self, z1: int, z2: int) -> float:
        a, b = self.centroids[z1], self.centroids[z2]
        return haversine_km(a[0], a[1], b[0], b[1])

    def neighbors(self, zone: int, k: int = 15) -> List[int]:
        """Zones sorted by centroid distance, the zone itself first."""
        if zone not in self._neighbors:
            self._neighbors[zone] = sorted(self.zones, key=lambda z: self.distance_km(zone, z))
        return self._neighbors[zone][:k]


@lru_cache(maxsize=1)
def load_grid(path: str = ZONE_GRID_PATH) -> ZoneGrid:
    with open(path, "r", encoding="utf-8") as fh:
        return ZoneGrid(json.load(fh))


def zone_of(lat, lon) -> int:
    """Module-level helper used by the Spark UDF (grid is cached per Python worker)."""
    return load_grid().zone_of(lat, lon)
