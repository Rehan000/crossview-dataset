# scripts/make_clean_subset.py
import argparse, json
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.strtree import STRtree
from shapely import make_valid
from pyproj import Transformer

from scripts.tiles import tile_polygon

DEFAULT_LAYER = "lod22_2d"
FALLBACK_LAYER = "lod13_2d"

def _load_buildings(gpkg_path: Path, layer: str) -> gpd.GeoDataFrame:
    try:
        gdf = gpd.read_file(gpkg_path, layer=layer)
    except Exception:
        gdf = gpd.read_file(gpkg_path, layer=layer, engine="fiona")
    gdf = gdf.to_crs("EPSG:28992")
    gdf["geometry"] = gdf.geometry.apply(lambda g: make_valid(g) if g is not None else None)
    gdf = gdf.explode(index_parts=False, ignore_index=True)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notnull() & gdf.geometry.is_valid].reset_index(drop=True)
    return gdf

def _load_points(tile_dir: Path) -> gpd.GeoDataFrame:
    pq = tile_dir / "meta_28992.parquet"
    jl = tile_dir / "meta_28992.jsonl"
    if pq.exists():
        df = pd.read_parquet(pq)
    else:
        # fall back to jsonl (compute projected if missing)
        rows = [json.loads(l) for l in open(jl) if l.strip()]
        df = pd.DataFrame(rows)
        if not {"x_28992","y_28992"}.issubset(df.columns):
            if not {"lon","lat"}.issubset(df.columns):
                raise SystemExit("Need lon/lat or x_28992/y_28992 in meta.")
            t = Transformer.from_crs("EPSG:4326","EPSG:28992",always_xy=True)
            x,y = t.transform(df["lon"].astype(float).values, df["lat"].astype(float).values)
            df["x_28992"], df["y_28992"] = x,y
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["x_28992"], df["y_28992"]), crs="EPSG:28992")
    return gdf

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True)
    ap.add_argument("--mesh-root", default="data/amsterdam/mesh")
    ap.add_argument("--map-root", default="data/amsterdam/mapillary")
    ap.add_argument("--layer", default=DEFAULT_LAYER)
    ap.add_argument("--dist-thresh-m", type=float, default=30.0, help="keep if dist_to_bldg_m <= thresh")
    ap.add_argument("--symlink-images", action="store_true", help="create images_clean/ with symlinks")
    args = ap.parse_args()

    tile = args.tile_id
    gpkg = Path(args.mesh_root) / tile / f"{tile}.gpkg"
    tile_dir = Path(args.map_root) / tile
    if not gpkg.exists(): raise SystemExit(f"Missing {gpkg}")
    if not tile_dir.exists(): raise SystemExit(f"Missing {tile_dir}")

    # load buildings
    try:
        buildings = _load_buildings(gpkg, args.layer)
    except SystemExit:
        buildings = _load_buildings(gpkg, FALLBACK_LAYER)

    # load points
    cams = _load_points(tile_dir)

    # inside tile polygon
    poly = tile_polygon(tile)
    cams["inside_tile"] = cams.within(poly)

    # distance to buildings via STRtree
    geoms = list(buildings.geometry.values)
    tree = STRtree(geoms)
    dists = []
    for pt in cams.geometry.values:
        idx = tree.nearest(pt)
        dists.append(pt.distance(geoms[idx]) if idx is not None else np.nan)
    cams["dist_to_bldg_m"] = np.array(dists, dtype=float)

    # filter
    clean = cams[(cams["inside_tile"]) & (cams["dist_to_bldg_m"] <= args.dist_thresh_m)].copy()
    print(f"[i] total={len(cams)} | inside={int(cams.inside_tile.sum())} | "
          f"clean (inside & dist≤{args.dist_thresh_m}m)={len(clean)}")

    # write outputs
    out_parq = tile_dir / "meta_clean.parquet"
    out_jsonl = tile_dir / "meta_clean.jsonl"
    clean.drop(columns="geometry").to_parquet(out_parq, index=False)
    with open(out_jsonl, "w") as f:
        for r in clean.drop(columns="geometry").to_dict(orient="records"):
            f.write(json.dumps(r) + "\n")
    print(f"[i] Wrote {out_parq}")
    print(f"[i] Wrote {out_jsonl}")

    # optional: symlink thumbnails
    if args.symlink_images:
        img_dir = tile_dir / "images"
        out_img = tile_dir / "images_clean"
        out_img.mkdir(exist_ok=True)
        import os
        for img_id in clean["id"].astype(str):
            src = img_dir / f"{img_id}.jpg"
            if src.exists():
                dst = out_img / f"{img_id}.jpg"
                try:
                    if not dst.exists():
                        os.symlink(src.resolve(), dst)
                except FileExistsError:
                    pass
        print(f"[i] Symlinked thumbnails to {out_img}")

if __name__ == "__main__":
    main()
