# scripts/build_manifest.py
import argparse
import json
from pathlib import Path
from datetime import datetime

import geopandas as gpd
import pandas as pd
import fiona

from scripts.tiles import tile_polygon

def list_layers_safely(gpkg_path: Path) -> list[str]:
    try:
        return list(fiona.listlayers(gpkg_path))
    except Exception:
        return []

def mapillary_counts_and_times(meta_parquet: Path) -> tuple[int | None, list[str | None]]:
    """
    Returns (row_count, [iso_start, iso_end]) for the given parquet.
    Falls back gracefully if file or columns are missing.
    """
    if not meta_parquet.exists():
        return None, [None, None]
    try:
        df = pd.read_parquet(meta_parquet)
    except Exception:
        return None, [None, None]
    if "captured_at_utc" in df.columns:
        s = pd.to_datetime(df["captured_at_utc"], errors="coerce")
    elif "captured_at" in df.columns:
        # Mapillary Graph 'captured_at' often ms since epoch
        s = pd.to_datetime(df["captured_at"], errors="coerce", unit="ms")
    else:
        s = pd.Series(dtype="datetime64[ns]")
    s = s.dropna()
    span = [s.min().isoformat(), s.max().isoformat()] if not s.empty else [None, None]
    return int(len(df)), span

def best_bounds_from_gpkg(gpkg_path: Path) -> tuple[list[float] | None, list[str]]:
    layers = list_layers_safely(gpkg_path)
    if not layers:
        return None, []
    for cand in ("lod22_2d", "lod13_2d", "lod12_2d"):
        if cand in layers:
            try:
                g = gpd.read_file(gpkg_path, layer=cand, engine="fiona").to_crs("EPSG:28992")
                return list(map(float, g.total_bounds)), layers
            except Exception:
                continue
    return None, layers

def count_jpegs(dir_path: Path) -> int | None:
    """
    Count *.jpg files in a directory. Returns None if directory doesn't exist.
    """
    if not dir_path.exists():
        return None
    try:
        return sum(1 for _ in dir_path.glob("*.jpg"))
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="e.g., 10-430-720")

    # Compatibility: either pass one --data-root, or individual roots
    ap.add_argument("--data-root", default=None, help="Base dir (expects mesh/, mapillary/, aerial/ under it)")
    ap.add_argument("--mesh-root", default=None, help="Override mesh root (default: <data-root>/mesh)")
    ap.add_argument("--mapillary-root", default=None, help="Override mapillary root (default: <data-root>/mapillary)")
    ap.add_argument("--aerial-root", default=None, help="Override aerial root (default: <data-root>/aerial)")
    ap.add_argument("--out-root", default="data/amsterdam/processed", help="Where to write <tile>/manifest.json")
    args = ap.parse_args()

    # Resolve roots
    if args.data_root:
        base = Path(args.data_root)
        mesh_root = Path(args.mesh_root) if args.mesh_root else base / "mesh"
        mapillary_root = Path(args.mapillary_root) if args.mapillary_root else base / "mapillary"
        aerial_root = Path(args.aerial_root) if args.aerial_root else base / "aerial"
    else:
        # fall back to Amsterdam defaults if nothing provided
        mesh_root = Path(args.mesh_root) if args.mesh_root else Path("data/amsterdam/mesh")
        mapillary_root = Path(args.mapillary_root) if args.mapillary_root else Path("data/amsterdam/mapillary")
        aerial_root = Path(args.aerial_root) if args.aerial_root else Path("data/amsterdam/aerial")

    tile = args.tile_id
    out_dir = Path(args.out_root) / tile
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Mesh ---
    mesh_dir = mesh_root / tile
    gpkg = mesh_dir / f"{tile}.gpkg"
    cityjson = mesh_dir / f"{tile}.city.json"
    obj_dir = mesh_dir / "obj"

    lods = {}
    for lod in ("LoD12", "LoD13", "LoD22"):
        p = obj_dir / lod
        if p.exists():
            lods[lod] = sorted([str(x) for x in p.glob("*.obj")])

    bounds_28992, gpkg_layers = (None, [])
    if gpkg.exists():
        bounds_28992, gpkg_layers = best_bounds_from_gpkg(gpkg)

    # --- Aerial ---
    aerial_dir = aerial_root / tile
    aerial_tifs = sorted([str(p) for p in aerial_dir.glob("aerial_*m.tif")])
    aerial_pngs = sorted([str(p) for p in aerial_dir.glob("aerial_*m.png")])

    # --- Mapillary ---
    mdir = mapillary_root / tile
    images_dir = mdir / "images"
    images_clean_dir = mdir / "images_clean"
    images_full_dir = mdir / "images_full"
    images_full_clean_dir = mdir / "images_full_clean"

    meta_parquet = mdir / "meta_28992.parquet"
    meta_clean_parquet = mdir / "meta_clean.parquet"
    count_raw, span_raw = mapillary_counts_and_times(meta_parquet)
    count_clean, span_clean = mapillary_counts_and_times(meta_clean_parquet)

    # File counts in each images directory (thumbs & full-res)
    n_images = count_jpegs(images_dir)
    n_images_clean = count_jpegs(images_clean_dir)
    n_images_full = count_jpegs(images_full_dir)
    n_images_full_clean = count_jpegs(images_full_clean_dir)

    # --- Tile polygon ---
    poly = tile_polygon(tile)
    poly_wkt = poly.wkt if poly is not None else None

    manifest = {
        "tile_id": tile,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "crs": "EPSG:28992",
        "tile_polygon_wkt": poly_wkt,
        "mesh": {
            "dir": str(mesh_dir) if mesh_dir.exists() else None,
            "gpkg": str(gpkg) if gpkg.exists() else None,
            "cityjson": str(cityjson) if cityjson.exists() else None,
            "obj": lods,
            "layers": gpkg_layers,
            "bounds_28992": bounds_28992,
        },
        "aerial": {
            "dir": str(aerial_dir) if aerial_dir.exists() else None,
            "tifs": aerial_tifs,
            "pngs": aerial_pngs,
        },
        "mapillary": {
            "dir": str(mdir) if mdir.exists() else None,
            "images_dir": str(images_dir) if images_dir.exists() else None,
            "images_clean_dir": str(images_clean_dir) if images_clean_dir.exists() else None,
            "images_full_dir": str(images_full_dir) if images_full_dir.exists() else None,
            "images_full_clean_dir": str(images_full_clean_dir) if images_full_clean_dir.exists() else None,
            "meta_parquet": str(meta_parquet) if meta_parquet.exists() else None,
            "meta_clean_parquet": str(meta_clean_parquet) if meta_clean_parquet.exists() else None,
            "counts": {
                "raw": count_raw,
                "clean": count_clean,
                "images": n_images,
                "images_clean": n_images_clean,
                "images_full": n_images_full,
                "images_full_clean": n_images_full_clean,
            },
            "time_spans": {"raw": span_raw, "clean": span_clean},
        },
    }

    out_path = out_dir / "manifest.json"
    with open(out_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"[i] Wrote {out_path}")

if __name__ == "__main__":
    main()
