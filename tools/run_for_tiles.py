# tools/run_for_tiles.py
import argparse
import csv
import subprocess
import sys
from pathlib import Path
from time import sleep

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
    ap.add_argument("--csv", required=True, help="CSV with tile ids (tile_id_dash or tile_id_slash column)")
    ap.add_argument("--steps", default="mesh,mapillary,augment,verify,clean,aerial,manifest",
                    help="Comma list: mesh,mapillary,augment,verify,clean,aerial,manifest")
    ap.add_argument("--data-root", default="data/amsterdam", help="Base data dir")

    # roots (can be overridden)
    ap.add_argument("--mesh-root", default="data/amsterdam/mesh")
    ap.add_argument("--mapillary-root", default="data/amsterdam/mapillary")
    ap.add_argument("--aerial-root", default="data/amsterdam/aerial")
    ap.add_argument("--out-root", default="data/amsterdam/processed")

    # Mapillary controls (forwarded)
    ap.add_argument("--max-images", type=int, default=1000, help="Mapillary cap per tile (overall)")
    ap.add_argument("--margin-m", type=float, default=15.0, help="Mapillary bbox margin (meters)")
    ap.add_argument("--subdivide", type=int, default=1, help="Split bbox into NxN sub-bboxes to bypass ~200 cap")
    ap.add_argument("--iphone-only", action="store_true", help="Filter to iPhone camera_type (client-side)")
    ap.add_argument("--no-thumbs", action="store_true", help="Do not download thumbnail JPGs (viewer)")
    ap.add_argument("--thumb-size", type=int, default=256, choices=[256, 1024, 2048],
                    help="Thumbnail size under images/ for the viewer")
    ap.add_argument("--download-full", action="store_true",
                    help="Also download higher-res images under images_full/ for training")
    ap.add_argument("--full-size", type=int, default=1024, choices=[1024, 2048],
                    help="If --download-full, choose 1024 or 2048")
    ap.add_argument("--api-limit", type=int, default=200, help="Per-page API limit (try 100 if timeouts)")
    ap.add_argument("--page-retries", type=int, default=3, help="Retries per page on HTTP errors/timeouts")

    # Clean subset controls
    ap.add_argument("--clean-layer", default="lod22_2d", help="Building layer for clean filter (fallback: lod13_2d)")
    ap.add_argument("--clean-dist", type=float, default=30.0, help="Max distance to buildings (meters)")
    ap.add_argument("--clean-copy", action="store_true", help="Copy clean files instead of symlinking")
    ap.add_argument("--clean-full", action="store_true",
                    help="Also create full-res clean set (images_full_clean from images_full)")

    # Aerial
    ap.add_argument("--aerial-layer", default="Actueel_ortho25", help="PDOK layer: Actueel_ortho25 or Actueel_orthoHR")
    ap.add_argument("--aerial-gsd", type=float, default=0.25, help="Aerial meters/pixel")

    # Misc
    ap.add_argument("--sleep", type=float, default=0.5, help="Seconds between steps (also passed to fetch_mapillary)")
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
                rc = sh([sys.executable, "-m", "scripts.download_3dbag_tile", "--tile-id", t], args.dry_run)
                if rc != 0:
                    print(f"[!] Mesh step failed for {t} (rc={rc}). Skipping this tile.")
                    continue
                sleep(args.sleep)
            else:
                print("[skip] mesh (already present)")

        # 2) Mapillary
        if "mapillary" in steps:
            meta_jsonl = map_dir / "meta.jsonl"
            if args.overwrite or need(meta_jsonl):
                cmd = [
                    sys.executable, "-m", "scripts.fetch_mapillary",
                    "--tile-id", t,
                    "--max-images", str(args.max_images),
                    "--margin-m", str(args.margin_m),
                    "--subdivide", str(args.subdivide),
                    "--sleep", str(args.sleep),
                    "--api-limit", str(args.api_limit),
                    "--page-retries", str(args.page_retries),
                ]
                if args.iphone_only:
                    cmd.append("--iphone-only")
                if args.no_thumbs:
                    cmd.append("--no-thumbs")
                cmd += ["--thumb-size", str(args.thumb_size)]
                if args.download_full:
                    cmd += ["--download-full", "--full-size", str(args.full_size)]
                rc = sh(cmd, args.dry_run)
                if rc != 0:
                    print(f"[!] Mapillary fetch failed for {t} (rc={rc}). Skipping remaining steps for this tile.")
                    continue
                sleep(args.sleep)
            else:
                print("[skip] mapillary (meta.jsonl exists)")

        # 3) Augment
        if "augment" in steps:
            meta_pq = map_dir / "meta_28992.parquet"
            if args.overwrite or need(meta_pq):
                cmd = [sys.executable, "-m", "scripts.augment_meta", "--tile-id", t]
                if args.overwrite:
                    cmd.append("--overwrite")
                rc = sh(cmd, args.dry_run)
                if rc != 0:
                    print(f"[!] Augment step failed for {t} (rc={rc}). Skipping this tile.")
                    continue
                sleep(args.sleep)
            else:
                print("[skip] augment (meta_28992.parquet exists)")

        # 4) Verify
        if "verify" in steps:
            diag_csv = Path("outputs/verify") / t / "diagnostics.csv"
            if args.overwrite or need(diag_csv):
                rc = sh([sys.executable, "-m", "scripts.verify_mapping", "--tile-id", t], args.dry_run)
                if rc != 0:
                    print(f"[!] Verify step failed for {t} (rc={rc}). Skipping this tile.")
                    continue
                sleep(args.sleep)
            else:
                print("[skip] verify (diagnostics exist)")

        # 5) Clean subset (thumbs + optional full-res)
        if "clean" in steps:
            clean_pq = map_dir / "meta_clean.parquet"
            if args.overwrite or need(clean_pq):
                # 5a) Thumbs clean set -> images_clean from images
                cmd = [
                    sys.executable, "-m", "scripts.make_clean_subset",
                    "--tile-id", t,
                    "--mesh-root", args.mesh_root,
                    "--map-root", args.mapillary_root,
                    "--layer", args.clean_layer,
                    "--dist-thresh-m", str(args.clean_dist),
                    "--symlink-images",
                    "--source-dir", "images",
                    "--dest-name", "images_clean",
                ]
                if args.clean_copy:
                    cmd.append("--copy")
                rc = sh(cmd, args.dry_run)
                if rc != 0:
                    print(f"[!] Clean subset (thumbs) failed for {t} (rc={rc}). Skipping this tile.")
                    continue
                sleep(args.sleep)

                # 5b) Optional full-res clean set -> images_full_clean from images_full
                if args.clean_full:
                    cmd2 = [
                        sys.executable, "-m", "scripts.make_clean_subset",
                        "--tile-id", t,
                        "--mesh-root", args.mesh_root,
                        "--map-root", args.mapillary_root,
                        "--layer", args.clean_layer,
                        "--dist-thresh-m", str(args.clean_dist),
                        "--symlink-images",
                        "--source-dir", "images_full",
                        "--dest-name", "images_full_clean",
                    ]
                    if args.clean_copy:
                        cmd2.append("--copy")
                    rc2 = sh(cmd2, args.dry_run)
                    if rc2 != 0:
                        print(f"[!] Clean subset (full-res) failed for {t} (rc={rc2}). Skipping this tile.")
                        continue
                    sleep(args.sleep)
            else:
                print("[skip] clean (meta_clean.parquet exists)")

        # 6) Aerial
        if "aerial" in steps:
            aerial_tif = None
            for p in aerial_dir.glob("aerial_*m.tif"):
                if "raw" not in p.name:
                    aerial_tif = p
                    break
            if args.overwrite or aerial_tif is None:
                rc = sh([
                    sys.executable, "-m", "scripts.fetch_aerial_nl",
                    "--tile-id", t, "--layer", args.aerial_layer, "--gsd", str(args.aerial_gsd)
                ], args.dry_run)
                if rc != 0:
                    print(f"[!] Aerial step failed for {t} (rc={rc}). Skipping this tile.")
                    continue
                sleep(args.sleep)
            else:
                print("[skip] aerial (clipped tif exists)")

        # 7) Manifest
        if "manifest" in steps:
            manifest = out_dir / "manifest.json"
            if args.overwrite or need(manifest):
                rc = sh([
                    sys.executable, "-m", "scripts.build_manifest",
                    "--tile-id", t,
                    "--mesh-root", args.mesh_root,
                    "--mapillary-root", args.mapillary_root,
                    "--aerial-root", args.aerial_root,
                    "--out-root", args.out_root
                ], args.dry_run)
                if rc != 0:
                    print(f"[!] Manifest step failed for {t} (rc={rc}).")
                    continue
                sleep(args.sleep)
            else:
                print("[skip] manifest (exists)")

    print("\n[✓] Done.")

if __name__ == "__main__":
    main()
