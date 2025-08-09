# scripts/fetch_aerial_nl.py
import argparse
import math
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import imageio.v2 as iio
import numpy as np
import rasterio
from rasterio.transform import from_bounds
import requests
import rioxarray  # noqa: F401  (registers .rio)
import xarray as xr
from shapely.geometry import mapping
from scripts.tiles import tile_polygon

DEFAULT_WMS = "https://service.pdok.nl/hwh/luchtfotorgb/wms/v1_0"
# Common PDOK layers:
#  - Actueel_ortho25  (25 cm GSD)
#  - Actueel_orthoHR  (8–10 cm GSD in many areas)

def fetch_wms_jpeg(
    wms_url: str,
    layer: str,
    bbox_28992: tuple[float, float, float, float],
    width: int,
    height: int,
    fmt: str = "image/jpeg",
    timeout: int = 120,
) -> bytes:
    """Request a WMS 1.3.0 GetMap image for EPSG:28992."""
    minx, miny, maxx, maxy = bbox_28992
    params = {
        "service": "WMS",
        "request": "GetMap",
        "version": "1.3.0",
        "layers": layer,
        "styles": "",
        "format": fmt,
        "transparent": "false",
        "crs": "EPSG:28992",
        "bbox": f"{minx},{miny},{maxx},{maxy}",
        "width": str(width),
        "height": str(height),
    }
    r = requests.get(wms_url, params=params, timeout=timeout)
    r.raise_for_status()
    # crude guard against XML error payloads
    if r.headers.get("Content-Type", "").startswith(("text/xml", "application/xml", "text/html")):
        raise RuntimeError(f"WMS returned XML/HTML error: {r.text[:400]}")
    return r.content

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tile-id", required=True, help="e.g., 10-430-720")
    ap.add_argument("--outdir", default="data/amsterdam/aerial", help="base output dir")
    ap.add_argument("--wms-url", default=DEFAULT_WMS, help="PDOK WMS endpoint")
    ap.add_argument("--layer", default="Actueel_ortho25", help="PDOK layer: Actueel_ortho25 or Actueel_orthoHR")
    ap.add_argument("--gsd", type=float, default=0.25, help="target ground sampling distance in meters/pixel")
    ap.add_argument("--max-size", type=int, default=8000, help="max pixels for width/height to avoid huge requests")
    ap.add_argument("--buffer-m", type=float, default=0.0, help="optional buffer in meters around tile bbox")
    args = ap.parse_args()

    tile = args.tile_id
    outdir = Path(args.outdir) / tile
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) Tile polygon (EPSG:28992) and bbox (with optional buffer)
    poly = tile_polygon(tile)
    if poly is None:
        raise SystemExit(f"Tile polygon not found for {tile}")
    minx, miny, maxx, maxy = poly.bounds
    if args.buffer_m > 0:
        minx -= args.buffer_m; miny -= args.buffer_m
        maxx += args.buffer_m; maxy += args.buffer_m
    bbox = (minx, miny, maxx, maxy)

    # 2) Compute output raster size from desired GSD
    width  = int(math.ceil((maxx - minx) / args.gsd))
    height = int(math.ceil((maxy - miny) / args.gsd))
    # Clamp to avoid excessive server load; adjust effective GSD accordingly
    if width > args.max_size or height > args.max_size:
        scale = max(width / args.max_size, height / args.max_size)
        width  = int(width / scale)
        height = int(height / scale)
        # We keep bbox; this effectively increases pixel size (coarser GSD).

    print(f"[i] Requesting WMS {args.layer} at ~{args.gsd} m/px -> {width}x{height}px")

    # 3) Fetch WMS image (JPEG)
    jpeg_bytes = fetch_wms_jpeg(args.wms_url, args.layer, bbox, width, height, fmt="image/jpeg")

    # 4) Decode to array; force RGB
    img = iio.imread(BytesIO(jpeg_bytes))
    if img.ndim == 2:  # grayscale
        img = np.stack([img, img, img], axis=-1)
    if img.shape[2] == 4:  # drop alpha
        img = img[:, :, :3]

    # 5) Write GeoTIFF (EPSG:28992), then clip to exact tile polygon
    geotiff_path = outdir / f"aerial_{args.layer}_{args.gsd:.2f}m_raw.tif"
    clipped_path = outdir / f"aerial_{args.layer}_{args.gsd:.2f}m.tif"
    png_path     = outdir / f"aerial_{args.layer}_{args.gsd:.2f}m.png"

    transform = from_bounds(minx, miny, maxx, maxy, img.shape[1], img.shape[0])
    profile = {
        "driver": "GTiff",
        "height": img.shape[0],
        "width": img.shape[1],
        "count": 3,
        "dtype": rasterio.uint8,
        "crs": "EPSG:28992",
        "transform": transform,
        "compress": "deflate",
    }
    with rasterio.open(geotiff_path, "w", **profile) as dst:
        for b in range(3):
            dst.write(img[:, :, b], b + 1)
    print(f"[i] Wrote {geotiff_path}")

    # Clip to exact polygon
    da = rioxarray.open_rasterio(geotiff_path)  # (band, y, x)
    clipped = da.rio.clip([mapping(poly)], crs="EPSG:28992")
    clipped.rio.to_raster(clipped_path, compress="deflate")
    print(f"[i] Wrote {clipped_path}")

    # Save PNG quicklook (channel-last)
    png_arr = np.moveaxis(clipped.values, 0, -1)  # (H,W,3)
    iio.imwrite(png_path, png_arr.astype(np.uint8))
    print(f"[i] Wrote {png_path}")

    print("[✓] Aerial orthophoto ready.")

if __name__ == "__main__":
    main()
