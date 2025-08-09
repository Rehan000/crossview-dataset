# scripts/verify_mapping.py
import json
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, box
from shapely.strtree import STRtree
from shapely import make_valid
from pyproj import Transformer

from scripts.tiles import tile_polygon

DEFAULT_LAYER = "lod22_2d"
FALLBACK_LAYER = "lod13_2d"

def _load_buildings(gpkg_path: Path, layer: str) -> gpd.GeoDataFrame:
    try:
        gdf = gpd.read_file(gpkg_path, layer=layer)
    except Exception as e:
        print(f"[w] Failed to read layer '{layer}' via pyogrio: {e}; retrying with Fiona…")
        gdf = gpd.read_file(gpkg_path, layer=layer, engine="fiona")
    # Reproject to EPSG:28992 (from EPSG:7415 compound or whatever)
    gdf = gdf.to_crs("EPSG:28992")
    # Clean invalid geometries (make_valid -> explode multiparts -> drop empties)
    gdf["geometry"] = gdf.geometry.apply(lambda g: make_valid(g) if g is not None else None)
    gdf = gdf.explode(index_parts=False, ignore_index=True)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notnull()]
    gdf = gdf[gdf.geometry.is_valid]
    gdf.reset_index(drop=True, inplace=True)
    return gdf

def _load_points(meta_dir: Path) -> gpd.GeoDataFrame:
    pq = meta_dir / "meta_28992.parquet"
    jl = meta_dir / "meta_28992.jsonl"
    if pq.exists():
        df = pd.read_parquet(pq)
        if not {"x_28992","y_28992"}.issubset(df.columns):
            raise SystemExit(f"{pq} missing x_28992/y_28992")
        return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["x_28992"], df["y_28992"]), crs="EPSG:28992")
    if jl.exists():
        rows = []
        with open(jl, "r") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                rows.append(json.loads(line))
        df = pd.DataFrame(rows)
        if not {"x_28992","y_28992"}.issubset(df.columns):
            if not {"lon","lat"}.issubset(df.columns):
                raise SystemExit("Need lon/lat or x_28992/y_28992 in meta.")
            t = Transformer.from_crs("EPSG:4326","EPSG:28992",always_xy=True)
            x,y = t.transform(df["lon"].astype(float).values, df["lat"].astype(float).values)
            df["x_28992"], df["y_28992"] = x,y
        return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["x_28992"], df["y_28992"]), crs="EPSG:28992")
    raise SystemExit("No meta_28992.{parquet|jsonl} found. Run scripts/augment_meta.py first.")

def _distance_to_buildings(points_gdf: gpd.GeoDataFrame, buildings_gdf: gpd.GeoDataFrame) -> np.ndarray:
    """
    Robust nearest distance using STRtree (no dissolve). Returns array of distances (meters).
    """
    geoms = list(buildings_gdf.geometry.values)
    tree = STRtree(geoms)
    # For each point, query nearest building geometry and compute precise distance
    dists = []
    for pt in points_gdf.geometry.values:
        idx = tree.nearest(pt)
        if idx is None:
            dists.append(np.nan)
        else:
            dists.append(pt.distance(geoms[idx]))
    return np.array(dists, dtype=float)

def _load_roads_osm(tile_poly_28992, cache_path: Path | None):
    try:
        import osmnx as ox
    except Exception:
        print("[w] osmnx not installed; skipping road check. (conda install -c conda-forge osmnx)")
        return None
    # small buffer to catch edges; fetch in 4326
    poly_buf = gpd.GeoSeries([tile_poly_28992.buffer(15)], crs="EPSG:28992").to_crs("EPSG:4326")
    west,south,east,north = poly_buf.total_bounds
    G = ox.graph_from_bbox(north, south, east, west, network_type="drive")
    edges = ox.graph_to_gdfs(G, nodes=False, edges=True)
    if isinstance(edges, tuple):  # older osmnx returns (nodes, edges)
        edges = edges[1]
    edges = edges.to_crs("EPSG:28992")
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        edges.to_parquet(cache_path, index=False)
    return edges

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="e.g., 10-430-720")
    ap.add_argument("--mesh-root", default="data/amsterdam/mesh")
    ap.add_argument("--map-root", default="data/amsterdam/mapillary")
    ap.add_argument("--layer", default=DEFAULT_LAYER)
    ap.add_argument("--out-prefix", default="outputs/verify")
    ap.add_argument("--with-roads", action="store_true", help="Fetch OSM roads and compute distance-to-road")
    args = ap.parse_args()

    tile = args.tile_id
    gpkg = Path(args.mesh_root) / tile / f"{tile}.gpkg"
    meta_dir = Path(args.map_root) / tile
    out_prefix = Path(args.out_prefix) / tile
    out_prefix.mkdir(parents=True, exist_ok=True)

    if not gpkg.exists():
        raise SystemExit(f"Missing GPKG: {gpkg}")

    # --- Load data ---
    try:
        buildings = _load_buildings(gpkg, layer=args.layer)
    except SystemExit:
        print(f"[w] Falling back to {FALLBACK_LAYER}")
        buildings = _load_buildings(gpkg, layer=FALLBACK_LAYER)

    tile_poly = tile_polygon(tile)  # EPSG:28992
    if tile_poly is None:
        raise SystemExit(f"Tile polygon not found for {tile}")

    cams = _load_points(meta_dir)

    # --- Inside tile polygon ---
    inside_mask = cams.within(tile_poly)
    cams["inside_tile"] = inside_mask
    n_total = len(cams)
    n_inside = int(inside_mask.sum())
    n_outside = n_total - n_inside
    print(f"[i] Points: {n_total} | inside tile polygon: {n_inside} | outside: {n_outside}")

    # --- Distance to buildings (robust via STRtree) ---
    dists = _distance_to_buildings(cams, buildings)
    cams["dist_to_bldg_m"] = dists
    p50 = float(np.nanpercentile(dists, 50))
    p90 = float(np.nanpercentile(dists, 90))
    pmax = float(np.nanmax(dists))
    print(f"[i] dist→buildings (m): p50={p50:.1f} | p90={p90:.1f} | max={pmax:.1f}")

    # --- Optional: distance to roads ---
    if args.with_roads:
        cache = out_prefix / "osm_roads.parquet"
        roads = _load_roads_osm(tile_poly, cache)
        if roads is not None and not roads.empty:
            rtree = STRtree(list(roads.geometry.values))
            rdists = []
            for pt in cams.geometry.values:
                idx = rtree.nearest(pt)
                rdists.append(pt.distance(roads.geometry.values[idx]) if idx is not None else np.nan)
            cams["dist_to_road_m"] = np.array(rdists, dtype=float)
            rp50 = float(np.nanpercentile(cams["dist_to_road_m"], 50))
            rp90 = float(np.nanpercentile(cams["dist_to_road_m"], 90))
            rpmax = float(np.nanmax(cams["dist_to_road_m"]))
            print(f"[i] dist→roads (m): p50={rp50:.1f} | p90={rp90:.1f} | max={rpmax:.1f}")
        else:
            print("[w] No OSM roads fetched; skipping road distances.")

    # --- Save diagnostics CSV ---
    diag_cols = ["id","lon","lat","x_28992","y_28992","inside_tile","dist_to_bldg_m"]
    if "dist_to_road_m" in cams.columns:
        diag_cols.append("dist_to_road_m")
    for c in ["camera_type","captured_at","captured_at_utc","sequence_id","thumb_1024_url"]:
        if c in cams.columns:
            diag_cols.append(c)
    pd.DataFrame(cams[diag_cols]).to_csv(out_prefix / "diagnostics.csv", index=False)
    print(f"[i] Wrote {out_prefix / 'diagnostics.csv'}")

    # --- Plots ---
    plt.figure(figsize=(7,4))
    plt.hist(cams["dist_to_bldg_m"].dropna(), bins=40)
    plt.title(f"Distance to buildings — {tile}")
    plt.xlabel("meters"); plt.ylabel("count")
    plt.tight_layout()
    plt.savefig(out_prefix / "hist_dist_buildings.png", dpi=200)
    print(f"[i] Wrote {out_prefix / 'hist_dist_buildings.png'}")

    tile_gdf = gpd.GeoDataFrame(geometry=[tile_poly], crs="EPSG:28992")
    fig, ax = plt.subplots(figsize=(8,8))
    tile_gdf.boundary.plot(ax=ax, color="tab:blue", linewidth=1.3)
    buildings.plot(ax=ax, edgecolor="black", facecolor="none", linewidth=0.4)
    cams[cams["inside_tile"]].plot(ax=ax, markersize=6, alpha=0.9)
    if n_outside:
        cams[~cams["inside_tile"]].plot(ax=ax, markersize=6, alpha=0.3)
    ax.set_aspect("equal")
    ax.set_title(f"QA overlay — {tile}")
    plt.tight_layout()
    plt.savefig(out_prefix / "overlay.png", dpi=200)
    print(f"[i] Wrote {out_prefix / 'overlay.png'}")

    # --- Simple pass/fail heuristic ---
    ok_inside = n_inside / max(1, n_total) >= 0.75    # relaxed to 75% since you used margin
    ok_dist   = p90 <= 30.0                           # tune by area density
    print(f"[i] Heuristic: inside≥75%? {ok_inside} | p90≤30m? {ok_dist}")
    if ok_inside and ok_dist:
        print("[✓] Mapping verification looks good.")
    else:
        print("[!] Mapping has potential issues; inspect diagnostics and overlay.")

if __name__ == "__main__":
    main()
