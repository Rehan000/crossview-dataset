# scripts/fetch_mapillary.py
import os
import sys
import json
import time
import random
import argparse
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

import requests
from dotenv import load_dotenv
import geopandas as gpd

from scripts.tiles import tile_polygon, tile_bbox_4326

# ───────────────────────────── Config ─────────────────────────────

# Ask Graph API for several thumbnail sizes; we pick at runtime.
FIELDS = ",".join([
    "id",
    "captured_at",
    "camera_type",
    "compass_angle",
    "altitude",
    "computed_geometry",
    "sequence",          # dict or id
    "thumb_256_url",
    "thumb_1024_url",
    "thumb_2048_url",
])
API_URL = "https://graph.mapillary.com/images"


# ───────────────────────────── Utils ─────────────────────────────

def load_token() -> str:
    load_dotenv()
    tok = os.getenv("MAPILLARY_ACCESS_TOKEN") or os.getenv("MAPILLARY_TOKEN")
    if not tok:
        raise SystemExit("Missing MAPILLARY_ACCESS_TOKEN. Put it in .env or export it in your shell.")
    return tok


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_existing_ids(meta_path: Path) -> set:
    if not meta_path.exists():
        return set()
    ids = set()
    with open(meta_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
                iid = str(j.get("id", "")).strip()
                if iid:
                    ids.add(iid)
            except Exception:
                continue
    return ids


def append_jsonl(meta_path: Path, recs: Iterable[dict]) -> int:
    n = 0
    with open(meta_path, "a") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    return n


def download_file(url: Optional[str], out_path: Path, timeout: int = 60) -> bool:
    if not url:
        return False
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        out_path.write_bytes(r.content)
        return True
    except Exception:
        return False


def pick_thumb(record: dict, target: int) -> Optional[str]:
    """
    Choose the closest thumbnail <= target (2048, 1024, 256).
    If none <= target exist, fall back to the smallest available.
    """
    order = [2048, 1024, 256]
    fields = {256: "thumb_256_url", 1024: "thumb_1024_url", 2048: "thumb_2048_url"}
    for sz in order:
        if sz <= target and record.get(fields[sz]):
            return record[fields[sz]]
    for sz in reversed(order):
        if record.get(fields[sz]):
            return record[fields[sz]]
    return None


def bbox_with_margin_4326(tile_id: str, margin_m: float = 0.0) -> Tuple[float, float, float, float]:
    """
    Build a WGS84 bbox for the tile. If margin_m>0, buffer the tile polygon in EPSG:28992
    by that many meters before converting to WGS84 and taking the bounds.
    """
    if margin_m <= 1e-6:
        return tile_bbox_4326(tile_id)
    poly_28992 = tile_polygon(tile_id)
    if poly_28992 is None:
        return tile_bbox_4326(tile_id)
    if margin_m > 0:
        poly_28992 = poly_28992.buffer(margin_m)
    g = gpd.GeoSeries([poly_28992], crs=28992).to_crs(4326)
    minx, miny, maxx, maxy = g.total_bounds
    return (minx, miny, maxx, maxy)


def subdivide_bbox(minlon: float, minlat: float, maxlon: float, maxlat: float, n: int) -> List[Tuple[float, float, float, float]]:
    """Split [minlon,minlat,maxlon,maxlat] into an n×n grid of sub-bboxes in lon/lat."""
    if n <= 1:
        return [(minlon, minlat, maxlon, maxlat)]
    lons = [minlon + (maxlon - minlon) * i / n for i in range(n)] + [maxlon]
    lats = [minlat + (maxlat - minlat) * j / n for j in range(n)] + [maxlat]
    cells = []
    for j in range(n):
        for i in range(n):
            cells.append((lons[i], lats[j], lons[i+1], lats[j+1]))
    return cells


def page_iter(
    initial_url: str,
    sleep_s: float = 0.35,
    session: Optional[requests.Session] = None,
    retries: int = 3,
    backoff_base: float = 1.0,
) -> Iterable[dict]:
    """
    Iterate results for one API URL, following paging.next if provided.
    Some bbox queries do not provide paging.next at all (hard ~200 cap).
    Retries on transient/timeout HTTP errors with exponential backoff + jitter.
    """
    url = initial_url
    sess = session or requests.Session()
    while url:
        attempt = 0
        while True:
            try:
                r = sess.get(url, timeout=60)
                r.raise_for_status()
                break
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                msg = ""
                try:
                    msg = e.response.json().get("error", {}).get("message", "")
                except Exception:
                    pass
                if attempt < retries and (status in (400, 408, 429, 500, 502, 503, 504)):
                    wait = backoff_base * (2 ** attempt) + random.uniform(0, 0.5)
                    print(f"[warn] HTTP {status} on page; retrying in {wait:.1f}s. {msg}")
                    time.sleep(wait)
                    attempt += 1
                    continue
                raise
        j = r.json()
        data = j.get("data", []) or []
        for it in data:
            yield it
        next_url = (j.get("paging") or {}).get("next")
        if not next_url or not data:
            break
        url = next_url
        time.sleep(sleep_s)


# ───────────────────────────── Core ─────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Fetch Mapillary images for a tile with bbox subdivision and pagination.")
    ap.add_argument("--tile-id", required=True, help="Tile id in dash or slash form, e.g. 10-430-720")
    ap.add_argument("--max-images", type=int, default=1200, help="Max images to retrieve overall (0 = no cap)")
    ap.add_argument("--margin-m", type=float, default=15.0, help="Margin around tile bbox in meters")
    ap.add_argument("--subdivide", type=int, default=1, help="Split bbox into NxN grid to break ~200 cap (try 2 or 3)")
    ap.add_argument("--iphone-only", action="store_true", help="Filter for iPhone camera_type (client-side)")
    ap.add_argument("--no-thumbs", action="store_true", help="Do not download thumbnail JPGs")

    # Sizes / downloads
    ap.add_argument("--thumb-size", type=int, default=256, choices=[256, 1024, 2048],
                    help="Thumbnail size to save under images/ (viewer).")
    ap.add_argument("--download-full", action="store_true",
                    help="Also download higher-res images for training under images_full/.")
    ap.add_argument("--full-size", type=int, default=1024, choices=[1024, 2048],
                    help="If --download-full, pick 1024 or 2048.")

    # API pacing / robustness
    ap.add_argument("--sleep", type=float, default=0.35, help="Seconds to sleep between API pages / cells")
    ap.add_argument("--api-limit", type=int, default=200, help="Per-page limit for Graph API (try 100 if timeouts).")
    ap.add_argument("--page-retries", type=int, default=3, help="Retries per page on HTTP errors/timeouts.")

    ap.add_argument("--out-root", default="data/amsterdam/mapillary", help="Output root directory")
    args = ap.parse_args()

    token = load_token()
    tile = args.tile_id.replace("/", "-")
    out_dir = Path(args.out_root) / tile
    img_dir = out_dir / "images"               # viewer thumbnails
    img_full_dir = out_dir / "images_full"     # training images
    meta_path = out_dir / "meta.jsonl"
    ensure_dir(img_dir)
    if args.download_full:
        ensure_dir(img_full_dir)

    # Build bbox (WGS84) and cells
    minlon, minlat, maxlon, maxlat = bbox_with_margin_4326(tile, args.margin_m)
    print(f"[i] BBOX (WGS84) for {tile}: ({minlon}, {minlat}, {maxlon}, {maxlat})")
    cells = subdivide_bbox(minlon, minlat, maxlon, maxlat, max(1, args.subdivide))
    if len(cells) > 1:
        print(f"[i] Subdividing bbox into {args.subdivide}×{args.subdivide} = {len(cells)} cells")

    # Existing & session dedupe
    existing_ids = read_existing_ids(meta_path)
    if existing_ids:
        print(f"[i] Found {len(existing_ids)} existing records in {meta_path}. Will skip duplicates.")
    seen_ids = set(existing_ids)

    # Prepare one session with auth header; avoids putting token in URL
    sess = requests.Session()
    sess.headers.update({"Authorization": f"OAuth {token}"})

    saved_total = 0

    for ci, (a, b, c, d) in enumerate(cells, start=1):
        if args.max_images > 0 and saved_total >= args.max_images:
            break

        base_url = (
            f"{API_URL}"
            f"?bbox={a},{b},{c},{d}"
            f"&fields={FIELDS}"
            f"&image_type=photo"
            f"&limit={args.api_limit}"
        )
        print(f"[i] Cell {ci}/{len(cells)} bbox=({a:.6f},{b:.6f},{c:.6f},{d:.6f})")

        new_meta = []
        page_count = 0

        try:
            for it in page_iter(
                base_url,
                sleep_s=args.sleep,
                session=sess,
                retries=args.page_retries,
                backoff_base=max(0.8, args.sleep),
            ):
                page_count += 1

                # Normalize fields
                geom = (it.get("computed_geometry") or {}).get("coordinates") or [None, None]
                lon, lat = (geom + [None, None])[:2]

                seq = it.get("sequence")
                if isinstance(seq, dict):
                    seq_id = seq.get("id")
                elif isinstance(seq, str):
                    seq_id = seq
                else:
                    seq_id = None

                camtype = it.get("camera_type") or ""
                if args.iphone_only and "iphone" not in str(camtype).lower():
                    continue

                rec = {
                    "id": it.get("id"),
                    "lon": lon,
                    "lat": lat,
                    "captured_at": it.get("captured_at"),
                    "camera_type": camtype,
                    "compass_angle": it.get("compass_angle"),
                    "altitude": it.get("altitude"),
                    "sequence_id": seq_id,
                    "thumb_256_url": it.get("thumb_256_url"),
                    "thumb_1024_url": it.get("thumb_1024_url"),
                    "thumb_2048_url": it.get("thumb_2048_url"),
                    "thumb_saved_size": None,
                    "full_saved_size": None,
                    "tile_id": tile,
                }

                iid = str(rec["id"])
                if not iid or iid in seen_ids:
                    continue

                # Viewer thumbnail
                if not args.no_thumbs:
                    thumb_url = pick_thumb(rec, args.thumb_size)
                    if thumb_url:
                        jpg_path = img_dir / f"{iid}.jpg"
                        if not jpg_path.exists() and download_file(thumb_url, jpg_path):
                            rec["thumb_saved_size"] = args.thumb_size

                # Optional training image
                if args.download_full:
                    full_url = pick_thumb(rec, args.full_size)
                    if full_url:
                        full_jpg = img_full_dir / f"{iid}.jpg"
                        if not full_jpg.exists() and download_file(full_url, full_jpg):
                            rec["full_saved_size"] = args.full_size

                new_meta.append(rec)
                seen_ids.add(iid)
                saved_total += 1

                if args.max_images > 0 and saved_total >= args.max_images:
                    break

        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            print(f"[warn] Cell {ci}: giving up after retries (HTTP {sc}). Skipping this cell.")

        # Write cell's chunk to disk immediately (safer on long runs)
        if new_meta:
            n = append_jsonl(meta_path, new_meta)
            print(f"[i] Cell {ci}: appended {n} records (pages seen: {page_count}) | total so far: {saved_total}")
        else:
            print(f"[i] Cell {ci}: no new records (pages seen: {page_count})")

        time.sleep(args.sleep)  # politeness between cells

    print(f"[i] Saved {saved_total} new records.")
    if not args.no_thumbs:
        print(f"[i] Thumbnails in: {img_dir} (~{args.thumb_size}px wide)")
    if args.download_full:
        print(f"[i] Training images in: {img_full_dir} (~{args.full_size}px wide)")
    print(f"[i] Metadata at {meta_path} (unique ids total ≈ {len(seen_ids)})")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user", file=sys.stderr)
        sys.exit(130)
