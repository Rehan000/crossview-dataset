# scripts/augment_meta.py
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from pyproj import Transformer
import geopandas as gpd


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _parse_timestamp(ts):
    """Return ISO8601 UTC string if possible, else original value."""
    if ts is None or ts == "":
        return None
    try:
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.isdigit()):
            ms = int(ts)
            dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            return dt.isoformat()
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="e.g., 10-430-720")
    ap.add_argument("--map-root", default="data/amsterdam/mapillary", help="Directory where meta.jsonl lives")
    ap.add_argument("--write-geo", action="store_true", help="Also write GeoParquet with geometry column")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite outputs if they exist")
    args = ap.parse_args()

    tile = args.tile_id
    tile_dir = Path(args.map_root) / tile
    meta_jsonl = tile_dir / "meta.jsonl"
    if not meta_jsonl.exists():
        raise SystemExit(f"Missing {meta_jsonl}")

    out_jsonl = tile_dir / "meta_28992.jsonl"
    out_parquet = tile_dir / "meta_28992.parquet"
    out_geoparquet = tile_dir / "meta_28992_geo.parquet"

    if (not args.overwrite) and (
        out_jsonl.exists() or out_parquet.exists() or (args.write_geo and out_geoparquet.exists())
    ):
        print("[i] Outputs exist. Use --overwrite to regenerate.")
        return

    rows = _read_jsonl(meta_jsonl)
    if not rows:
        raise SystemExit(f"No valid rows in {meta_jsonl}")

    df = pd.DataFrame(rows)
    if "id" not in df.columns:
        raise SystemExit("Expected an 'id' column in meta.jsonl.")
    if not {"lon", "lat"}.issubset(df.columns):
        raise SystemExit("Expected 'lon' and 'lat' columns in meta.jsonl.")

    before = len(df)
    df = df.dropna(subset=["lon", "lat"])
    after_drop = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first")
    after_dedup = len(df)

    tr = Transformer.from_crs("EPSG:4326", "EPSG:28992", always_xy=True)
    x, y = tr.transform(df["lon"].astype(float).values, df["lat"].astype(float).values)
    df["x_28992"] = x
    df["y_28992"] = y

    if "captured_at" in df.columns:
        df["captured_at_utc"] = df["captured_at"].map(_parse_timestamp)

    # JSONL (projected)
    with open(out_jsonl, "w") as f:
        for rec in df.to_dict(orient="records"):
            f.write(json.dumps(rec) + "\n")

    # Parquet (fast)
    try:
        df.to_parquet(out_parquet, index=False)
    except Exception as e:
        print(f"[w] Could not write Parquet ({e}). Install pyarrow or fastparquet to enable Parquet export.")
        out_parquet = None

    # Optional GeoParquet
    if args.write_geo:
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["x_28992"], df["y_28992"]), crs="EPSG:28992")
        try:
            gdf.to_parquet(out_geoparquet, index=False)
        except Exception as e:
            print(f"[w] Could not write GeoParquet ({e}).")

    print(f"[i] Input rows: {before}")
    print(f"[i] Kept after dropna(lon/lat): {after_drop}")
    print(f"[i] Kept after dedup(id): {after_dedup}")
    print(f"[i] Wrote {out_jsonl}")
    if out_parquet:
        print(f"[i] Wrote {out_parquet}")
    if args.write_geo:
        print(f"[i] Wrote {out_geoparquet}")

if __name__ == "__main__":
    main()
