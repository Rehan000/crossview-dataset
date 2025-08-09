# scripts/select_tiles.py
import argparse
import csv
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point, box, shape
from shapely.ops import transform as shp_transform
from pyproj import Transformer

TILE_INDEX = "data/amsterdam/mesh/tile_index.fgb"  # EPSG:28992

def _to_dash(tid: str) -> str:
    return tid.replace("/", "-")

def _to_slash(tid: str) -> str:
    s = tid.replace("-", "/")
    return s if s.count("/") == 2 else tid  # best-effort

def _parse_tile_id(tid: str):
    tid = tid.strip()
    if "-" in tid:
        z, x, y = map(int, tid.split("-"))
        return z, x, y
    if "/" in tid:
        z, x, y = map(int, tid.split("/"))
        return z, x, y
    raise ValueError(f"Bad tile id: {tid}")

def _neighbors(z:int, x:int, y:int, k:int):
    tiles=[]
    for dx in range(-k, k+1):
        for dy in range(-k, k+1):
            tiles.append((z, x+dx, y+dy))
    return tiles

def _project_geom_4326_to_28992(geom):
    t = Transformer.from_crs("EPSG:4326","EPSG:28992", always_xy=True).transform
    return shp_transform(t, geom)

def _load_index():
    g = gpd.read_file(TILE_INDEX)  # EPSG:28992
    if g.crs is None or g.crs.to_epsg() != 28992:
        g = g.set_crs(28992, allow_override=True)
    return g

def _filter_by_geom_28992(gidx, geom):
    return gidx[gidx.geometry.intersects(geom)]

def _write_csv(out_path, tile_ids):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tile_id_dash", "tile_id_slash"])
        for z,x,y in sorted(set(tile_ids)):
            dash = f"{z}-{x}-{y}"
            slash = f"{z}/{x}/{y}"
            w.writerow([dash, slash])
    print(f"[i] Wrote {out_path} ({len(set(tile_ids))} tiles)")

def main():
    ap = argparse.ArgumentParser(description="Select 3DBAG tiles by neighbors/point/bbox/polygon.")
    ap.add_argument("--out", default="data/amsterdam/tiles_selected.csv", help="Output CSV")
    sub = ap.add_subparsers(dest="mode", required=True)

    s1 = sub.add_parser("neighbors", help="Ring of neighbors around a tile")
    s1.add_argument("--tile-id", required=True, help="e.g., 10-430-720 or 10/430/720")
    s1.add_argument("--k", type=int, default=1, help="ring size: 1->3x3, 2->5x5")

    s2 = sub.add_parser("around-point", help="All tiles within radius (meters) of a lat/lon")
    s2.add_argument("--lat", type=float, required=True)
    s2.add_argument("--lon", type=float, required=True)
    s2.add_argument("--radius-m", type=float, default=800)

    s3 = sub.add_parser("bbox", help="All tiles intersecting a WGS84 bbox")
    s3.add_argument("--minlon", type=float, required=True)
    s3.add_argument("--minlat", type=float, required=True)
    s3.add_argument("--maxlon", type=float, required=True)
    s3.add_argument("--maxlat", type=float, required=True)

    s4 = sub.add_parser("polygon", help="All tiles intersecting a polygon (GeoJSON)")
    s4.add_argument("--geojson", required=True, help="Path to a GeoJSON with Polygon/MultiPolygon (EPSG:4326)")

    args = ap.parse_args()
    gidx = _load_index()

    tile_ids = []

    if args.mode == "neighbors":
        z,x,y = _parse_tile_id(args.tile_id)
        # We’ll trust the index to hold the valid range; neighbors can be filtered by intersection below.
        # Build a dummy bbox: union of selected neighbors; later we’ll intersect with the index anyway.
        nb = _neighbors(z,x,y,args.k)
        # Keep only neighbors that actually exist in index (match on tile_id column if present)
        # tile_index uses slash format in 'tile_id' like '10/430/720'
        slash_ids = [f"{z}/{x}/{y}" for (z,x,y) in nb]
        hit = gidx[gidx["tile_id"].isin(slash_ids)]
        if hit.empty:
            print("[w] No neighbors found in index — check z/x/y.")
        else:
            for tid in hit["tile_id"].tolist():
                tz, tx, ty = _parse_tile_id(tid)
                tile_ids.append((tz, tx, ty))

    elif args.mode == "around-point":
        # make a buffer circle (in 28992) around the projected point
        p28992 = _project_geom_4326_to_28992(Point(args.lon, args.lat))
        circle = p28992.buffer(args.radius_m)
        hit = _filter_by_geom_28992(gidx, circle)
        for tid in hit["tile_id"].tolist():
            tz, tx, ty = _parse_tile_id(tid)
            tile_ids.append((tz, tx, ty))

    elif args.mode == "bbox":
        poly_wgs84 = box(args.minlon, args.minlat, args.maxlon, args.maxlat)
        poly_28992 = _project_geom_4326_to_28992(poly_wgs84)
        hit = _filter_by_geom_28992(gidx, poly_28992)
        for tid in hit["tile_id"].tolist():
            tz, tx, ty = _parse_tile_id(tid)
            tile_ids.append((tz, tx, ty))

    elif args.mode == "polygon":
        gj = gpd.read_file(args.geojson)  # expecting EPSG:4326
        if gj.crs is None or gj.crs.to_epsg() != 4326:
            gj = gj.set_crs(4326, allow_override=True)
        geom_28992 = gj.to_crs(28992).unary_union
        hit = _filter_by_geom_28992(gidx, geom_28992)
        for tid in hit["tile_id"].tolist():
            tz, tx, ty = _parse_tile_id(tid)
            tile_ids.append((tz, tx, ty))

    _write_csv(Path(args.out), tile_ids)

if __name__ == "__main__":
    main()
