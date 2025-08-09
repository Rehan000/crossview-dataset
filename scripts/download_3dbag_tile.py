# scripts/download_3dbag_tile.py
import os
import re
import argparse
import zipfile
from pathlib import Path

import geopandas as gpd
import requests

TILE_INDEX = "data/amsterdam/mesh/tile_index.fgb"

# Regex to detect dash-form id in URLs and LoD from filenames
RE_DASH_ID_IN_URL = re.compile(r"/([0-9]+-[0-9]+-[0-9]+)\.(?:zip|gpkg|city\.json)$", re.IGNORECASE)
RE_LOD_IN_NAME = re.compile(r"LoD(\d{2})", re.IGNORECASE)  # matches LoD12, LoD13, LoD22, etc.


def _dash_id_from_row(row) -> str | None:
    """Extract dash id like '10-430-720' from any *_download column."""
    for col in ("obj_download", "gpkg_download", "cj_download"):
        val = row.get(col)
        if isinstance(val, str):
            m = RE_DASH_ID_IN_URL.search(val)
            if m:
                return m.group(1)
    return None


def _organize_lod_subdirs(obj_root: Path) -> None:
    """
    Move extracted OBJ/MTL (and any related files) into obj/LoD12, obj/LoD13, obj/LoD22 subdirs
    based on the filename containing 'LoD12', 'LoD13', 'LoD22'. Idempotent and safe to re-run.
    """
    if not obj_root.exists():
        return

    # Create target subdirs
    lod_dirs = {lod: (obj_root / f"LoD{lod}") for lod in ("12", "13", "22")}
    for d in lod_dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    # Walk all files directly under obj_root (and nested, just in case)
    for p in list(obj_root.rglob("*")):
        if p.is_dir():
            continue
        lod_match = RE_LOD_IN_NAME.search(p.name)
        if not lod_match:
            continue  # skip files without LoD tag

        lod = lod_match.group(1)  # '12' | '13' | '22' | ...
        target_dir = obj_root / f"LoD{lod}"
        target_dir.mkdir(parents=True, exist_ok=True)

        dest = target_dir / p.name
        if dest.exists():
            # Already organized; skip
            continue
        try:
            p.replace(dest)  # move
        except Exception:
            # If moving across devices fails, fall back to copy+unlink
            import shutil
            shutil.copy2(p, dest)
            p.unlink(missing_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="Slash or dash form, e.g. 10/430/720 or 10-430-720")
    ap.add_argument("--outdir-base", default="data/amsterdam/mesh", help="Base directory to place the tile folder")
    ap.add_argument("--skip-obj", action="store_true", help="Skip downloading/extracting OBJ zip")
    ap.add_argument("--skip-gpkg", action="store_true", help="Skip downloading GPKG")
    ap.add_argument("--skip-cityjson", action="store_true", help="Skip downloading CityJSON")
    args = ap.parse_args()

    g = gpd.read_file(TILE_INDEX)

    dash = args.tile_id.replace("/", "-")
    # Prefer matching by dash id embedded in download columns
    hit = g[
        g["obj_download"].astype(str).str.contains(dash, na=False) |
        g["gpkg_download"].astype(str).str.contains(dash, na=False) |
        g["cj_download"].astype(str).str.contains(dash, na=False)
    ]
    if hit.empty:
        # Fallback: exact match on slash-form tile_id
        hit = g[g["tile_id"].astype(str) == args.tile_id]
    if hit.empty:
        raise SystemExit(f"Tile {args.tile_id} not found in {TILE_INDEX}")

    row = hit.iloc[0]
    dash_id = _dash_id_from_row(row) or dash

    out_dir = Path(args.outdir_base) / dash_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Prepare paths
    cityjson_path = out_dir / f"{dash_id}.city.json"
    gpkg_path = out_dir / f"{dash_id}.gpkg"
    obj_zip_path = out_dir / f"{dash_id}-obj.zip"
    obj_dir = out_dir / "obj"
    obj_dir.mkdir(exist_ok=True)

    # Download URLs
    urls = {
        "cityjson": row.get("cj_download"),
        "gpkg": row.get("gpkg_download"),
        "objzip": row.get("obj_download"),
    }

    # Download CityJSON
    if not args.skip_cityjson and isinstance(urls["cityjson"], str) and urls["cityjson"]:
        if cityjson_path.exists():
            print(f"[i] Exists: {cityjson_path.name}")
        else:
            print(f"[i] Downloading CityJSON → {cityjson_path}")
            r = requests.get(urls["cityjson"], timeout=180)
            r.raise_for_status()
            cityjson_path.write_bytes(r.content)

    # Download GPKG
    if not args.skip_gpkg and isinstance(urls["gpkg"], str) and urls["gpkg"]:
        if gpkg_path.exists():
            print(f"[i] Exists: {gpkg_path.name}")
        else:
            print(f"[i] Downloading GPKG → {gpkg_path}")
            r = requests.get(urls["gpkg"], timeout=300)
            r.raise_for_status()
            gpkg_path.write_bytes(r.content)

            # Auto-decompress if the server sent a gzipped gpkg
            import gzip, shutil
            with open(gpkg_path, "rb") as f:
                head = f.read(2)
            is_gzip = head == b"\x1f\x8b"
            if is_gzip:
                tmp = gpkg_path.with_suffix(".gpkg.gz")
                gpkg_path.rename(tmp)
                with gzip.open(tmp, "rb") as fin, open(gpkg_path, "wb") as fout:
                    shutil.copyfileobj(fin, fout)
                tmp.unlink()
                print(f"[i] Decompressed gzipped GeoPackage → {gpkg_path.name}")

    # Download & extract OBJ ZIP
    if not args.skip_obj and isinstance(urls["objzip"], str) and urls["objzip"]:
        if obj_zip_path.exists():
            print(f"[i] Exists: {obj_zip_path.name}")
        else:
            print(f"[i] Downloading OBJ ZIP → {obj_zip_path}")
            r = requests.get(urls["objzip"], timeout=600)
            r.raise_for_status()
            obj_zip_path.write_bytes(r.content)

        # Extract (idempotent)
        print(f"[i] Extracting to {obj_dir} …")
        with zipfile.ZipFile(obj_zip_path, "r") as zf:
            # Extract all; if re-running, ZipFile will overwrite into same paths
            zf.extractall(obj_dir)

        # Organize into LoD subdirs
        _organize_lod_subdirs(obj_dir)
        print(f"[✓] Organized OBJ files into {obj_dir}/LoD12, LoD13, LoD22")

    print(f"[✓] Tile ready at {out_dir.resolve()}")

if __name__ == "__main__":
    main()
