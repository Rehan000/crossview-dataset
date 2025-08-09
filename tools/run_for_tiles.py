# tools/run_for_tiles.py
import argparse
import csv
import subprocess
import sys
from pathlib import Path
from time import sleep

# Helpers
def sh(cmd, dry=False):
    print("[cmd]", " ".join(cmd))
    if dry:
        return 0
    return subprocess.call(cmd)

def col_or_fail(row, names):
    for n in names:
        if n in row and row[n]:
            return row[n]
    raise SystemExit(f"CSV missing a tile id column; looked for {names}")

def dash_id(row):
    v = col_or_fail(row, ["tile_id_dash", "tile_id", "dash", "id"])
    return v.replace("/", "-")

def need(path: Path):
    return not path.exists()

def main():
    ap = argparse.ArgumentParser(description="Run the crossview pipeline for many tiles.")
    ap.add_argument("--csv", required=True, help="CSV with tile ids (columns: tile_id_dash or tile_id_slash)")
    ap.add_argument("--steps", default="mesh,mapillary,augment,verify,clean,aerial,manifest",
                    help="Comma list: mesh,mapillary,augment,verify,clean,aerial,manifest")
    ap.add_argument("--data-root", default="data/amsterdam", help="Base data dir")
    ap.add_argument("--mesh-root", default="data/amsterdam/mesh")
    ap.add_argument("--mapillary-root", default="data/amsterdam/mapillary")
    ap.add_argument("--aerial-root", default="data/amsterdam/aerial")
    ap.add_argument("--out-root", default="data/amsterdam/processed")
    ap.add_argument("--max-images", type=int, default=1000, help="Mapillary cap per tile")
    ap.add_argument("--margin-m", type=float, default=15.0, help="Mapillary bbox margin (meters)")
    ap.add_argument("--iphone-only", action="store_true", help="Prefer iPhone shots (may reduce count)")
    ap.add_argument("--aerial-layer", default="Actueel_ortho25", help="PDOK layer: Actueel_ortho25 or Actueel_orthoHR")
    ap.add_argument("--aerial-gsd", type=float, default=0.25, help="Aerial meters/pixel")
    ap.add_argument("--sleep", type=float, default=0.5, help="Seconds between steps (politeness)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--overwrite", action="store_true", help="Force rerun even if outputs exist")
    args = ap.parse_args()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    csv_path = Path(args.csv)

    with open(csv_path, newline="") as f:
        rdr = csv.DictReader(f)
        tiles = [dash_id(row) for row in rdr]

    if not tiles:
        print("[!] No tiles found in CSV.")
        sys.exit(1)

    print(f"[i] Running for {len(tiles)} tiles: {tiles}")

    for t in tiles:
        print("\n" + "="*60)
        print(f"[tile] {t}")
        print("="*60)

        mesh_dir = Path(args.mesh_root) / t
        map_dir  = Path(args.mapillary_root) / t
        aerial_dir = Path(args.aerial_root) / t
        out_dir = Path(args.out_root) / t

        # 1) Mesh
        if "mesh" in steps:
            if args.overwrite or need(mesh_dir / f"{t}.gpkg"):
                sh(["python", "-m", "scripts.download_3dbag_tile", "--tile-id", t], args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] mesh (already present)")

        # 2) Mapillary
        if "mapillary" in steps:
            meta_jsonl = map_dir / "meta.jsonl"
            if args.overwrite or need(meta_jsonl):
                cmd = ["python", "-m", "scripts.fetch_mapillary", "--tile-id", t,
                       "--max-images", str(args.max_images), "--margin-m", str(args.margin_m)]
                if args.iphone_only:
                    cmd.append("--iphone-only")
                sh(cmd, args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] mapillary (meta.jsonl exists)")

        # 3) Augment
        if "augment" in steps:
            meta_pq = map_dir / "meta_28992.parquet"
            if args.overwrite or need(meta_pq):
                cmd = ["python", "-m", "scripts.augment_meta", "--tile-id", t]
                if args.overwrite:
                    cmd.append("--overwrite")
                sh(cmd, args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] augment (meta_28992.parquet exists)")

        # 4) Verify
        if "verify" in steps:
            diag_csv = Path("outputs/verify") / t / "diagnostics.csv"
            if args.overwrite or need(diag_csv):
                sh(["python", "-m", "scripts.verify_mapping", "--tile-id", t], args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] verify (diagnostics exist)")

        # 5) Clean subset
        if "clean" in steps:
            clean_pq = map_dir / "meta_clean.parquet"
            if args.overwrite or need(clean_pq):
                sh(["python", "-m", "scripts.make_clean_subset", "--tile-id", t, "--symlink-images"], args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] clean (meta_clean.parquet exists)")

        # 6) Aerial
        if "aerial" in steps:
            aerial_tif = None
            for p in aerial_dir.glob("aerial_*m.tif"):
                if "raw" not in p.name:
                    aerial_tif = p; break
            if args.overwrite or aerial_tif is None:
                sh([
                    "python", "-m", "scripts.fetch_aerial_nl",
                    "--tile-id", t, "--layer", args.aerial_layer, "--gsd", str(args.aerial_gsd)
                ], args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] aerial (clipped tif exists)")

        # 7) Manifest
        if "manifest" in steps:
            manifest = out_dir / "manifest.json"
            if args.overwrite or need(manifest):
                sh([
                    "python", "-m", "scripts.build_manifest",
                    "--tile-id", t,
                    "--data-root", args.data_root,
                    "--out-root", args.out_root
                ], args.dry_run)
                sleep(args.sleep)
            else:
                print("[skip] manifest (exists)")

    print("\n[✓] Done.")

if __name__ == "__main__":
    main()
