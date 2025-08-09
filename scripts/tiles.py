import os
import re
import geopandas as gpd
from shapely.geometry import Point
from typing import Optional, Tuple
from scripts.utils_crs import to_4326, to_28992

# Default path; override by passing load_tiles(path=...) if needed
DEFAULT_TILE_INDEX = "data/amsterdam/mesh/tile_index.fgb"
_DOWNLOAD_COLS = ["cj_download", "gpkg_download", "obj_download"]

_TILES_GDF = None
_ID_COL = None

def _detect_id_col(gdf):
    """Pick a reasonable ID column from common names, else first string-like column."""
    for cand in ["tile_id", "id", "name", "zxy", "tile", "TILE_ID", "TileID"]:
        if cand in gdf.columns:
            return cand
    for c in gdf.columns:
        if gdf[c].dtype == object:
            return c
    return None

def load_tiles(path: str = DEFAULT_TILE_INDEX):
    """Load the FlatGeobuf tile index (CRS expected: EPSG:28992). Caches globally."""
    global _TILES_GDF, _ID_COL
    if _TILES_GDF is None:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Tile index not found at {path}")
        g = gpd.read_file(path)  # Fast and memory-efficient for .fgb
        _TILES_GDF = g
        _ID_COL = _detect_id_col(g)
        if _ID_COL is None:
            raise ValueError(f"Could not detect ID column in {path}. Columns: {list(g.columns)}")
    return _TILES_GDF

def _row_by_dash_id(gdf, dash_id: str):
    """
    Find the row whose download URLs contain the dash-form id (e.g., '8-328-552').
    Matches '/8-328-552.' or '/8-328-552/' or end-of-string.
    """
    pat = rf"/{re.escape(dash_id)}(?:\.|/|$)"
    for col in ["cj_download", "gpkg_download", "obj_download"]:
        if col in gdf.columns:
            mask = gdf[col].astype(str).str.contains(pat, regex=True, na=False)
            hit = gdf[mask]
            if not hit.empty:
                return hit.iloc[0]
    return None

def tile_polygon(tile_id: str, path: str = DEFAULT_TILE_INDEX):
    """
    Accepts either slash-form (e.g., '8/760/72') or dash-form (e.g., '8-328-552').
    Returns shapely Polygon/MultiPolygon in EPSG:28992, or None if not found.
    """
    g = load_tiles(path)
    # Direct match on the detected ID column (slash-form)
    direct = g.loc[g[_ID_COL].astype(str) == str(tile_id)]
    if not direct.empty:
        return direct.iloc[0].geometry
    # Dash-form via download URL columns
    if "-" in str(tile_id):
        row = _row_by_dash_id(g, str(tile_id))
        if row is not None:
            return row.geometry
    return None

def tile_bbox_28992(tile_id: str, margin_m: float = 0.0, path: str = DEFAULT_TILE_INDEX) -> Optional[Tuple[float,float,float,float]]:
    poly = tile_polygon(tile_id, path)
    if poly is None:
        return None
    minx, miny, maxx, maxy = poly.bounds
    if margin_m:
        return (minx - margin_m, miny - margin_m, maxx + margin_m, maxy + margin_m)
    return (minx, miny, maxx, maxy)

def tile_bbox_4326(tile_id: str, margin_m: float = 0.0, path: str = DEFAULT_TILE_INDEX) -> Optional[Tuple[float,float,float,float]]:
    b = tile_bbox_28992(tile_id, margin_m, path)
    if b is None:
        return None
    minx, miny, maxx, maxy = b
    bl = to_4326(minx, miny)
    tr = to_4326(maxx, maxy)
    minlon, minlat = min(bl[0], tr[0]), min(bl[1], tr[1])
    maxlon, maxlat = max(bl[0], tr[0]), max(bl[1], tr[1])
    return (minlon, minlat, maxlon, maxlat)

def point_to_tile(lat: float, lon: float, path: str = DEFAULT_TILE_INDEX) -> Optional[str]:
    """Return the slash-form tile_id containing a WGS84 (lat, lon) point, or None."""
    x, y = to_28992(lon, lat)
    pt = Point(x, y)
    g = load_tiles(path)
    hit = g[g.contains(pt)]
    if hit.empty:
        return None
    return str(hit.iloc[0][_ID_COL])

# --- Fallback: derive bbox from the tile's GPKG if tile_index fails ---

def gpkg_bbox_4326_for_tile(tile_id: str) -> Optional[Tuple[float,float,float,float]]:
    """
    Read the tile's .gpkg, compute bounds in 28992, transform to 4326.
    Expects file at data/amsterdam/mesh/{tile_id}/{tile_id}.gpkg
    """
    gpkg_path = f"data/amsterdam/mesh/{tile_id}/{tile_id}.gpkg"
    if not os.path.exists(gpkg_path):
        return None
    g = gpd.read_file(gpkg_path)  # Should already be EPSG:28992
    if g.empty:
        return None
    minx, miny, maxx, maxy = g.total_bounds
    bl = to_4326(minx, miny)
    tr = to_4326(maxx, maxy)
    minlon, minlat = min(bl[0], tr[0]), min(bl[1], tr[1])
    maxlon, maxlat = max(bl[0], tr[0]), max(bl[1], tr[1])
    return (minlon, minlat, maxlon, maxlat)
