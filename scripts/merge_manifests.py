# scripts/merge_manifests.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

def _safe_time(ts: Optional[str]) -> Optional[pd.Timestamp]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return pd.to_datetime(ts, utc=True, errors="coerce")
    except Exception:
        return None

def _span_minmax(
    existing: Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]],
    cand: Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    emin, emax = existing
    cmin, cmax = cand
    if cmin is not None:
        emin = cmin if emin is None or cmin < emin else emin
    if cmax is not None:
        emax = cmax if emax is None or cmax > emax else emax
    return emin, emax

def _fmt_span(span: Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]) -> List[Optional[str]]:
    smin, smax = span
    f = lambda t: t.isoformat().replace("+00:00", "Z") if t is not None else None
    return [f(smin), f(smax)]

def _sum_opt(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)

def _union_bounds(existing: Optional[List[float]], cand: Optional[List[float]]) -> Optional[List[float]]:
    # bounds = [minx, miny, maxx, maxy]
    if not cand or len(cand) != 4:
        return existing
    if existing is None:
        return [float(cand[0]), float(cand[1]), float(cand[2]), float(cand[3])]
    return [
        min(existing[0], float(cand[0])),
        min(existing[1], float(cand[1])),
        max(existing[2], float(cand[2])),
        max(existing[3], float(cand[3])),
    ]

def _count_jpegs(dir_path: Optional[str]) -> Optional[int]:
    if not dir_path:
        return None
    p = Path(dir_path)
    if not p.exists():
        return None
    try:
        return sum(1 for _ in p.glob("*.jpg"))
    except Exception:
        return None

def _iter_tile_manifests(root: Path) -> List[Path]:
    # Expect structure: <root>/<tile_id>/manifest.json
    out: List[Path] = []
    for sub in sorted(root.iterdir()):
        if not sub.is_dir():
            continue
        mf = sub / "manifest.json"
        if mf.exists():
            out.append(mf)
    return out

def main():
    ap = argparse.ArgumentParser(description="Merge per-tile manifests into a single index.json")
    ap.add_argument("--root", required=True, help="Processed root (contains <tile>/manifest.json)")
    ap.add_argument("--out", default=None, help="Output index path (default: <root>/index.json)")
    ap.add_argument("--skip-tiles", action="store_true", help="Only write summary (omit per-tile entries)")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"Root not found: {root}")

    out_path = Path(args.out) if args.out else (root / "index.json")

    manifests = _iter_tile_manifests(root)
    if not manifests:
        raise SystemExit(f"No manifests found under {root}")

    tiles: List[Dict[str, Any]] = []

    # Global aggregations
    tiles_count = 0
    created_span = (None, None)  # across tile manifests
    raw_span = (None, None)
    clean_span = (None, None)

    sum_raw = None
    sum_clean = None
    sum_images = None
    sum_images_clean = None
    sum_images_full = None
    sum_images_full_clean = None

    union_bounds_28992: Optional[List[float]] = None

    for mf in manifests:
        try:
            data = json.loads(mf.read_text(encoding="utf-8"))
        except Exception:
            print(f"[warn] Failed to read {mf}, skipping.")
            continue

        tile_id = data.get("tile_id") or mf.parent.name
        crs = data.get("crs", "EPSG:28992")

        # Manifest creation times
        created_at = _safe_time(data.get("created_at"))
        created_span = _span_minmax(created_span, (created_at, created_at))

        # Mesh bounds union
        mesh = data.get("mesh") or {}
        union_bounds_28992 = _union_bounds(union_bounds_28992, mesh.get("bounds_28992"))

        # Mapillary counts/time spans (favor what's in manifest, but fall back to dir counts if missing)
        mp = data.get("mapillary") or {}
        counts = (mp.get("counts") or {})
        times = (mp.get("time_spans") or {})

        c_raw = counts.get("raw")
        c_clean = counts.get("clean")

        # If image dir counts are missing in manifest, compute now
        img_dir = mp.get("images_dir")
        img_clean_dir = mp.get("images_clean_dir")
        img_full_dir = mp.get("images_full_dir")
        img_full_clean_dir = mp.get("images_full_clean_dir")

        c_images = counts.get("images")
        c_images_clean = counts.get("images_clean")
        c_images_full = counts.get("images_full")
        c_images_full_clean = counts.get("images_full_clean")

        if c_images is None:
            c_images = _count_jpegs(img_dir)
        if c_images_clean is None:
            c_images_clean = _count_jpegs(img_clean_dir)
        if c_images_full is None:
            c_images_full = _count_jpegs(img_full_dir)
        if c_images_full_clean is None:
            c_images_full_clean = _count_jpegs(img_full_clean_dir)

        # Time spans
        raw_s = times.get("raw") or [None, None]
        clean_s = times.get("clean") or [None, None]
        raw_span = _span_minmax(raw_span, (_safe_time(raw_s[0]), _safe_time(raw_s[1])))
        clean_span = _span_minmax(clean_span, (_safe_time(clean_s[0]), _safe_time(clean_s[1])))

        # Global sums
        sum_raw = _sum_opt(sum_raw, c_raw)
        sum_clean = _sum_opt(sum_clean, c_clean)
        sum_images = _sum_opt(sum_images, c_images)
        sum_images_clean = _sum_opt(sum_images_clean, c_images_clean)
        sum_images_full = _sum_opt(sum_images_full, c_images_full)
        sum_images_full_clean = _sum_opt(sum_images_full_clean, c_images_full_clean)

        # Per-tile summary (compact)
        tile_entry = {
            "tile_id": tile_id,
            "manifest_path": str(mf),
            "crs": crs,
            "has_mesh": bool(data.get("mesh", {}).get("gpkg")),
            "has_aerial": bool((data.get("aerial") or {}).get("tifs")),
            "has_mapillary": bool(data.get("mapillary")),
            "mapillary": {
                "counts": {
                    "raw": c_raw,
                    "clean": c_clean,
                    "images": c_images,
                    "images_clean": c_images_clean,
                    "images_full": c_images_full,
                    "images_full_clean": c_images_full_clean,
                },
                "time_spans": {
                    "raw": raw_s,
                    "clean": clean_s,
                },
            },
        }
        tiles.append(tile_entry)
        tiles_count += 1

    # Build index.json
    index: Dict[str, Any] = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "root": str(root),
        "version": 1,
        "summary": {
            "tiles": tiles_count,
            "mapillary_counts": {
                "raw": sum_raw,
                "clean": sum_clean,
                "images": sum_images,
                "images_clean": sum_images_clean,
                "images_full": sum_images_full,
                "images_full_clean": sum_images_full_clean,
            },
            "time_spans": {
                "manifests_created": _fmt_span(created_span),
                "raw": _fmt_span(raw_span),
                "clean": _fmt_span(clean_span),
            },
            "bounds_28992": union_bounds_28992,
        },
    }

    if not args.skip_tiles:
        index["tiles"] = tiles

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(index, indent=2), encoding="utf-8")
    print(f"[i] Wrote {out_path}")

if __name__ == "__main__":
    main()
