# scripts/plot_tile_overlay.py
import json
import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, box
from pyproj import Transformer

DEFAULT_LAYER = "lod22_2d"
FALLBACK_LAYER = "lod13_2d"


def load_meta_points_jsonl(meta_path: Path):
    """Load (lon, lat) from meta.jsonl."""
    pts_4326 = []
    with open(meta_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            j = json.loads(line)
            lon, lat = j.get("lon"), j.get("lat")
            if lon is None or lat is None:
                continue
            pts_4326.append((lon, lat))
    return pts_4326


def load_points_geodf_from_parquet(pq_path: Path):
    """Load projected points (EPSG:28992) directly from Parquet if available."""
    import pandas as pd
    df = pd.read_parquet(pq_path)
    if not {"x_28992", "y_28992"}.issubset(df.columns):
        raise ValueError(f"{pq_path} missing x_28992/y_28992 columns")
    gpts = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["x_28992"], df["y_28992"]), crs="EPSG:28992")
    return gpts


def load_buildings_gpkg(gpkg_path: Path, layer: str):
    """Read a specific layer from the GPKG and reproject to EPSG:28992."""
    # Try fast path (pyogrio), then Fiona fallback
    try:
        buildings = gpd.read_file(gpkg_path, layer=layer)
    except Exception as e:
        print(f"[w] Could not read '{layer}' via pyogrio: {e}\n[i] Retrying with Fiona engine…")
        buildings = gpd.read_file(gpkg_path, layer=layer, engine="fiona")

    # Reproject to EPSG:28992 (from EPSG:7415 compound or whatever original CRS is)
    try:
        buildings = buildings.to_crs("EPSG:28992")
    except Exception as e:
        raise SystemExit(f"[e] Failed to reproject {gpkg_path} layer '{layer}' to EPSG:28992: {e}")

    return buildings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="e.g., 10-430-720")
    ap.add_argument("--mesh-root", default="data/amsterdam/mesh", help="Directory where the GPKG lives")
    ap.add_argument("--map-root", default="data/amsterdam/mapillary", help="Directory where meta files live")
    ap.add_argument("--layer", default=DEFAULT_LAYER, help=f"GPKG layer to plot (default: {DEFAULT_LAYER})")
    ap.add_argument("--save", default=None, help="Optional path to save PNG (e.g., outputs/overlay.png)")
    ap.add_argument("--point-size", type=float, default=6.0, help="Marker size for points")
    ap.add_argument("--alpha", type=float, default=0.9, help="Alpha for points")
    args = ap.parse_args()

    tile = args.tile_id
    gpkg = Path(args.mesh_root) / tile / f"{tile}.gpkg"
    meta_dir = Path(args.map_root) / tile
    meta_jsonl = meta_dir / "meta.jsonl"
    meta_pq = meta_dir / "meta_28992.parquet"

    if not gpkg.exists():
        raise SystemExit(f"Missing GPKG: {gpkg}")
    if not meta_jsonl.exists() and not meta_pq.exists():
        raise SystemExit(f"Missing metadata: expected {meta_jsonl} or {meta_pq}")

    # --- Load buildings (specific layer, with fallback) ---
    layer = args.layer
    try:
        buildings = load_buildings_gpkg(gpkg, layer=layer)
    except SystemExit:
        if layer != FALLBACK_LAYER:
            print(f"[w] Falling back to layer '{FALLBACK_LAYER}'…")
            buildings = load_buildings_gpkg(gpkg, layer=FALLBACK_LAYER)
        else:
            raise

    # --- Load points: prefer Parquet (already in EPSG:28992), else JSONL (transform) ---
    if meta_pq.exists():
        try:
            gpts = load_points_geodf_from_parquet(meta_pq)
            print(f"[i] Loaded points from {meta_pq} (projected).")
        except Exception as e:
            print(f"[w] Failed to load Parquet: {e}\n[i] Falling back to meta.jsonl…")
            pts_4326 = load_meta_points_jsonl(meta_jsonl)
            t = Transformer.from_crs("EPSG:4326", "EPSG:28992", always_xy=True)
            pts_28992 = [Point(*t.transform(lon, lat)) for lon, lat in pts_4326]
            gpts = gpd.GeoDataFrame(geometry=pts_28992, crs="EPSG:28992")
    else:
        pts_4326 = load_meta_points_jsonl(meta_jsonl)
        t = Transformer.from_crs("EPSG:4326", "EPSG:28992", always_xy=True)
        pts_28992 = [Point(*t.transform(lon, lat)) for lon, lat in pts_4326]
        gpts = gpd.GeoDataFrame(geometry=pts_28992, crs="EPSG:28992")

    # --- Stats: inside vs outside tile bounds ---
    minx, miny, maxx, maxy = buildings.total_bounds
    tile_bbox = gpd.GeoSeries([box(minx, miny, maxx, maxy)], crs="EPSG:28992")
    inside_mask = gpts.within(tile_bbox.iloc[0])
    n_total = len(gpts)
    n_inside = int(inside_mask.sum())
    n_outside = n_total - n_inside
    print(f"[i] Total points: {n_total} | inside tile bbox: {n_inside} | outside: {n_outside}")

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(8, 8))
    buildings.plot(ax=ax, edgecolor="black", facecolor="none", linewidth=0.5)
    # plot inside points brighter, outside lighter (if any)
    if n_outside > 0:
        gpts[~inside_mask].plot(ax=ax, markersize=args.point_size, alpha=0.3)
    if n_inside > 0:
        gpts[inside_mask].plot(ax=ax, markersize=args.point_size, alpha=args.alpha)

    ax.set_aspect("equal")
    ax.set_title(f"Mapillary points over {args.layer} footprints — {tile}")
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    plt.tight_layout()

    if args.save:
        out = Path(args.save)
        out.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out, dpi=200)
        print(f"[i] Saved figure to {out}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
