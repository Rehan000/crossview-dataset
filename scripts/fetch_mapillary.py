import os
import json
import time
import argparse
import requests
from pathlib import Path

# Optional: auto-load .env if python-dotenv is installed
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from scripts.tiles import tile_bbox_4326, gpkg_bbox_4326_for_tile

API_URL = "https://graph.mapillary.com/images"
FIELDS = ",".join([
    "id",
    "camera_type",
    "computed_geometry",  # lon/lat
    "geometry",           # fallback
    "compass_angle",
    "sequence",           # may be a string id or an object
    "captured_at",
    "thumb_1024_url"
])

def fetch_page(access_token, bbox4326, after=None, limit=200, timeout=30):
    headers = {"Authorization": f"OAuth {access_token}"}
    params = {
        "fields": FIELDS,
        "bbox": ",".join(map(str, bbox4326)),  # minLon,minLat,maxLon,maxLat
        "limit": limit,
    }
    if after:
        params["after"] = after
    r = requests.get(API_URL, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _get_lonlat(item):
    # Prefer computed_geometry
    geom = (item.get("computed_geometry") or {}).get("coordinates")
    if geom and len(geom) == 2:
        return geom[0], geom[1]
    # Fallback to geometry
    geom2 = (item.get("geometry") or {}).get("coordinates")
    if geom2 and len(geom2) == 2:
        return geom2[0], geom2[1]
    return None, None

def _get_sequence_id(item):
    seq = item.get("sequence")
    if isinstance(seq, dict):
        return seq.get("id")
    # Sometimes API returns just the string id
    if isinstance(seq, str):
        return seq
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="Accepts '10/430/720' or '10-430-720'")
    ap.add_argument("--outdir", default="data/amsterdam/mapillary", help="Output base dir")
    ap.add_argument("--max-images", type=int, default=2000)
    ap.add_argument("--iphone-only", action="store_true", help="Filter camera_type containing 'iphone'")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between requests (politeness)")
    ap.add_argument("--margin-m", type=float, default=0.0, help="Expand tile bbox by meters (projected) before WGS84 transform")
    ap.add_argument("--access-token", default=os.environ.get("MAPILLARY_TOKEN"),
                    help="Mapillary API token. Defaults to MAPILLARY_TOKEN env var / .env")
    args = ap.parse_args()

    if not args.access_token:
        raise SystemExit("No access token. Set MAPILLARY_TOKEN in .env or pass --access-token.")

    tile = args.tile_id
    out_dir = Path(args.outdir) / tile
    img_dir = out_dir / "images"
    out_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)
    meta_path = out_dir / "meta.jsonl"

    # Derive bbox from tile index; fallback to the tile's own GPKG
    bbox = tile_bbox_4326(tile, margin_m=args.margin_m)
    if bbox is None:
        print(f"[w] Tile {tile} not found via tile_index; trying GPKG fallback…")
        bbox = gpkg_bbox_4326_for_tile(tile)
    if bbox is None:
        raise SystemExit(f"Could not derive bbox for {tile}. Check tile_index.fgb and {tile}.gpkg exist.")
    print(f"[i] BBOX (WGS84) for {tile}: {bbox}")

    total = 0
    after = None
    seen_ids = set()

    # idempotent append if file exists
    existing = 0
    if meta_path.exists():
        with open(meta_path, "r") as f:
            for line in f:
                try:
                    j = json.loads(line)
                    if "id" in j:
                        seen_ids.add(j["id"])
                except Exception:
                    pass
        existing = len(seen_ids)
        print(f"[i] Found {existing} existing records in {meta_path}. Will skip duplicates.")

    with open(meta_path, "a") as meta_f:
        while total + existing < args.max_images:
            # basic retry loop for transient errors
            for attempt in range(3):
                try:
                    data = fetch_page(args.access_token, bbox, after=after, limit=200)
                    break
                except requests.HTTPError as e:
                    status = e.response.status_code if e.response is not None else "?"
                    print(f"[w] HTTP {status}: {e}. Attempt {attempt+1}/3")
                    time.sleep(1.5 * (attempt + 1))
                except requests.RequestException as e:
                    print(f"[w] Network error: {e}. Attempt {attempt+1}/3")
                    time.sleep(1.5 * (attempt + 1))
            else:
                print("[e] Failed after retries; stopping.")
                break

            items = data.get("data", [])
            if not items:
                print("[i] No more results.")
                break

            for it in items:
                img_id = it.get("id")
                if not img_id or img_id in seen_ids:
                    continue

                camtype = (it.get("camera_type") or "").lower()
                if args.iphone_only and "iphone" not in camtype:
                    continue

                lon, lat = _get_lonlat(it)
                if lon is None or lat is None:
                    continue

                record = {
                    "id": img_id,
                    "camera_type": it.get("camera_type"),
                    "compass_angle": it.get("compass_angle"),
                    "sequence_id": _get_sequence_id(it),
                    "captured_at": it.get("captured_at"),
                    "lon": lon, "lat": lat,
                    "thumb_1024_url": it.get("thumb_1024_url"),
                    "tile_id": tile,
                    "source": "mapillary"
                }
                meta_f.write(json.dumps(record) + "\n")

                url = record["thumb_1024_url"]
                if url:
                    img_path = img_dir / f"{img_id}.jpg"
                    if not img_path.exists():
                        try:
                            r = requests.get(url, timeout=30)
                            r.raise_for_status()
                            img_path.write_bytes(r.content)
                        except Exception as e:
                            print(f"[w] img {img_id} download failed: {e}")

                seen_ids.add(img_id)
                total += 1
                if total + existing >= args.max_images:
                    break

            # pagination
            paging = data.get("paging", {})
            cursors = paging.get("cursors", {})
            after = cursors.get("after")
            if not after:
                break
            time.sleep(args.sleep)

    print(f"[i] Saved {total} new images to {img_dir}")
    print(f"[i] Metadata at {meta_path} (total lines now ≈ {total + existing})")

if __name__ == "__main__":
    main()
