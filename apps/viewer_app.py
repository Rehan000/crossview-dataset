# apps/viewer_app.py
import os
import json
import base64
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import Transformer
from PIL import Image

import streamlit as st
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium

# Mesh snapshot (CPU-only, no OpenGL)
import trimesh
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Poly3DCollection

# ----------------------------- Paths & Constants -----------------------------
DATA_ROOT = Path("data/amsterdam")
PROCESSED_ROOT = DATA_ROOT / "processed"
MESH_ROOT = DATA_ROOT / "mesh"
MAP_ROOT = DATA_ROOT / "mapillary"
AERIAL_ROOT = DATA_ROOT / "aerial"

PDOK_AERIAL_URL = (
    "https://service.pdok.nl/hwh/luchtfotorgb/wmts/v1_0"
    "?service=WMTS&request=GetTile&version=1.0.0"
    "&layer=Actueel_ortho25&style=default&format=image/jpeg"
    "&TileMatrixSet=EPSG:3857&TileMatrix={z}&TileCol={x}&TileRow={y}"
)

st.set_page_config(page_title="CrossView Viewer", layout="wide")
st.title("CrossView Viewer — Amsterdam")


# ----------------------------- Small helpers -----------------------------
def list_tiles():
    if not PROCESSED_ROOT.exists():
        return []
    return sorted([p.name for p in PROCESSED_ROOT.iterdir() if p.is_dir()])


def load_manifest(tile_id: str) -> dict:
    m = PROCESSED_ROOT / tile_id / "manifest.json"
    return json.loads(m.read_text()) if m.exists() else {}


def load_buildings(tile_id: str, prefer=("lod22_2d", "lod13_2d", "lod12_2d")) -> gpd.GeoDataFrame:
    gpkg = MESH_ROOT / tile_id / f"{tile_id}.gpkg"
    if not gpkg.exists():
        return gpd.GeoDataFrame(geometry=[])
    for layer in prefer:
        try:
            g = gpd.read_file(gpkg, layer=layer, engine="fiona").to_crs(4326)
            g["layer"] = layer
            return g
        except Exception:
            continue
    try:
        return gpd.read_file(gpkg, engine="fiona").to_crs(4326)
    except Exception:
        return gpd.GeoDataFrame(geometry=[])


def load_mapillary_points(tile_id: str) -> pd.DataFrame:
    base = MAP_ROOT / tile_id
    for name in ("meta_clean.parquet", "meta_28992.parquet"):
        p = base / name
        if p.exists():
            df = pd.read_parquet(p)
            # Ensure lon/lat for web map
            if not {"lon", "lat"}.issubset(df.columns):
                if {"x_28992", "y_28992"}.issubset(df.columns):
                    t = Transformer.from_crs(28992, 4326, always_xy=True)
                    lon, lat = t.transform(df["x_28992"].values, df["y_28992"].values)
                    df["lon"], df["lat"] = lon, lat
                else:
                    df["lon"], df["lat"] = None, None
            # Local thumbnail path
            img_dir = base / "images"
            if img_dir.exists():
                df["thumb_local"] = df["id"].astype(str).map(lambda i: str((img_dir / f"{i}.jpg").resolve()))
            else:
                df["thumb_local"] = None
            return df
    return pd.DataFrame(columns=["id", "lon", "lat"])


def tile_center_from_manifest(tile_id: str):
    man = load_manifest(tile_id)
    wkt = man.get("tile_polygon_wkt")
    if wkt:
        g = gpd.GeoSeries.from_wkt([wkt], crs=28992).to_crs(4326)
        c = g.iloc[0].centroid
        return c.y, c.x
    b = load_buildings(tile_id)
    if not b.empty:
        if hasattr(b, "union_all"):
            c = b.union_all().centroid
        else:
            c = b.unary_union.centroid  # fallback for older Shapely
        return c.y, c.x
    return (52.3728, 4.8936)


def encode_thumb_base64(path: str, max_w: int = 320) -> str | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        im = Image.open(p).convert("RGB")
        w, h = im.size
        if w > max_w:
            im = im.resize((max_w, int(h * (max_w / w))))
        buf = BytesIO()
        im.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return None


def haversine_deg(lat1, lon1, lat2, lon2):
    """Great-circle distance in meters between points in degrees."""
    R = 6371000.0
    p1 = np.radians(lat1); p2 = np.radians(lat2)
    dlat = p2 - p1
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(p1)*np.cos(p2)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def nearest_point(df: pd.DataFrame, lat: float, lon: float, max_dist_m: float = 25.0):
    """Return (row, dist_m) of nearest Mapillary record to clicked lat/lon within max distance."""
    if df.empty or not {"lat", "lon"}.issubset(df.columns):
        return None, None
    coords = df[["lat", "lon"]].to_numpy(dtype=float)
    dists = haversine_deg(lat, lon, coords[:, 0], coords[:, 1])
    idx = int(np.argmin(dists))
    d = float(dists[idx])
    if d <= max_dist_m:
        return df.iloc[idx], d
    return None, None


# ----------------------------- Folium map -----------------------------
def build_folium_map(tile_id: str, buildings: gpd.GeoDataFrame, cams: pd.DataFrame, zoom=16):
    center = tile_center_from_manifest(tile_id)
    m = folium.Map(location=center, zoom_start=zoom, tiles=None, control_scale=True)

    # PDOK aerial base
    folium.TileLayer(
        tiles=PDOK_AERIAL_URL,
        name="PDOK Aerial (Actueel_ortho25)",
        attr="PDOK Luchtfoto RGB",
        overlay=False,
        control=True,
        show=True,
    ).add_to(m)

    # Buildings overlay
    if not buildings.empty:
        gj = folium.GeoJson(
            data=json.loads(buildings.to_json()),
            name=f"Buildings ({buildings.get('layer', 'lod22_2d').iloc[0] if 'layer' in buildings.columns and len(buildings)>0 else '2D'})",
            style_function=lambda feat: {"color": "#000000", "weight": 1, "fill": False},
            control=True,
        )
        gj.add_to(m)

    # Mapillary points (clustered) — popup contains inline base64 thumbnail
    if not cams.empty and {"lon", "lat"}.issubset(cams.columns):
        cams_ok = cams.dropna(subset=["lon", "lat"]).copy()
        mc = MarkerCluster(name="Mapillary").add_to(m)
        for _, r in cams_ok.iterrows():
            b64 = encode_thumb_base64(str(r.get("thumb_local"))) if r.get("thumb_local") else None
            if b64:
                popup_html = f"<img src='data:image/jpeg;base64,{b64}' width='320'/>"
            else:
                popup_html = f"<b>ID:</b> {r.get('id','?')}"
                if r.get("captured_at_utc"):
                    popup_html += f"<br><b>Time:</b> {r.get('captured_at_utc')}"
            folium.Marker(
                location=(float(r["lat"]), float(r["lon"])),
                icon=folium.Icon(color="red", icon="camera", prefix="fa"),
                popup=folium.Popup(popup_html, max_width=340),
                tooltip=str(r.get("id", "")),
            ).add_to(mc)

    folium.LayerControl(collapsed=False).add_to(m)
    return m


# ----------------------------- Mesh snapshot (matplotlib) -----------------------------
def render_mesh_snapshot_matplotlib(tile_id: str, lod="LoD22", width=900, height=700, elev=25, azim=-60):
    obj_dir = MESH_ROOT / tile_id / "obj" / lod
    if not obj_dir.exists():
        return None, f"No {lod} directory found."
    objs = sorted(obj_dir.glob("*.obj"))
    if not objs:
        return None, f"No OBJ files found in {obj_dir}"
    obj_path = objs[0]

    try:
        mesh = trimesh.load_mesh(obj_path, force="mesh")
        if hasattr(mesh, "geometry"):  # Scene
            parts = [g for g in mesh.geometry.values() if isinstance(g, trimesh.Trimesh)]
            if not parts:
                return None, "No Trimesh geometries found in scene."
            mesh = trimesh.util.concatenate(parts)
        if not isinstance(mesh, trimesh.Trimesh):
            return None, "Loaded mesh is not a Trimesh."

        verts = mesh.vertices
        faces = mesh.faces

        # center & normalize
        center = verts.mean(axis=0)
        v = verts - center
        scale = np.percentile(np.linalg.norm(v, axis=1), 95)
        if scale > 0:
            v /= scale

        tris = v[faces]

        fig = plt.figure(figsize=(width/100, height/100), dpi=100)
        ax = fig.add_subplot(111, projection="3d")
        ax.add_collection3d(
            Poly3DCollection(tris, facecolor=(0.82, 0.84, 0.9, 1.0), edgecolor=(0.2, 0.2, 0.2, 0.15), linewidths=0.2)
        )
        ax.set_xlim(-1, 1); ax.set_ylim(-1, 1); ax.set_zlim(-1, 1)
        ax.set_axis_off()
        ax.view_init(elev=elev, azim=azim)

        out_dir = Path("outputs/mesh_snapshots") / tile_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{lod}.png"
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0)
        plt.close(fig)
        return str(out_path), None
    except Exception as e:
        return None, f"Failed to render mesh: {e}"


# ----------------------------- UI -----------------------------
tiles = list_tiles()
if not tiles:
    st.warning("No manifests found under data/amsterdam/processed. Run build_manifest first.")
    st.stop()

default_idx = tiles.index("10-430-720") if "10-430-720" in tiles else 0
tile_id = st.sidebar.selectbox("Tile", tiles, index=default_idx)

tab_map, tab_mesh = st.tabs(["🗺️ Map (Folium + PDOK)", "🧱 3D Mesh Preview"])

# ----------------------------- Map tab -----------------------------
with tab_map:
    st.subheader("Aerial + Buildings + Mapillary")
    buildings = load_buildings(tile_id)
    cams = load_mapillary_points(tile_id)
    fmap = build_folium_map(tile_id, buildings, cams, zoom=16)

    # Capture map interactions
    map_state = st_folium(fmap, width=None, height=700, returned_objects=[])

    # Side details panel on click
    st.markdown("### Selection")
    col_l, col_r = st.columns([1, 1], gap="large")

    with col_l:
        if map_state:
            # Markers usually populate 'last_object_clicked' in recent streamlit-folium;
            # otherwise, fall back to 'last_map_click'
            clicked = map_state.get("last_object_clicked") or map_state.get("last_map_click")
            if clicked and "lat" in clicked and "lng" in clicked:
                clat, clon = float(clicked["lat"]), float(clicked["lng"])
                row, dist_m = nearest_point(cams, clat, clon, max_dist_m=35.0)
                if row is not None:
                    st.write(f"**Nearest point** (≈{dist_m:.1f} m)")
                    # Show key metadata
                    fields = ["id", "captured_at_utc", "image_quality", "camera_type",
                              "sequence_id", "compass_angle", "altitude", "thumb_local"]
                    for f in fields:
                        if f in row and pd.notna(row[f]):
                            st.write(f"**{f}:** {row[f]}")
                else:
                    st.info("Click near a pin to see details here.")
            else:
                st.info("Click a pin (or near one) on the map to see details.")
        else:
            st.info("Interact with the map to select a point.")

    with col_r:
        # Larger image preview (from the selected/nearest row)
        if map_state:
            clicked = map_state.get("last_object_clicked") or map_state.get("last_map_click")
            if clicked and "lat" in clicked and "lng" in clicked:
                clat, clon = float(clicked["lat"]), float(clicked["lng"])
                row, _ = nearest_point(cams, clat, clon, max_dist_m=35.0)
                if row is not None and row.get("thumb_local") and Path(str(row.get("thumb_local"))).exists():
                    st.image(str(row.get("thumb_local")), caption=f"id: {row.get('id')}", use_container_width=True)
                else:
                    st.info("No local thumbnail for this point.")

# ----------------------------- Mesh tab -----------------------------
with tab_mesh:
    st.subheader("LoD OBJ snapshot (CPU-only)")
    lod = st.selectbox("LoD folder", ["LoD22", "LoD13", "LoD12"], index=0)
    elev = st.slider("Elevation", 0, 80, 25)
    azim = st.slider("Azimuth", -180, 180, -60)
    if st.button("Render snapshot"):
        png_path, err = render_mesh_snapshot_matplotlib(tile_id, lod=lod, elev=elev, azim=azim)
        if err:
            st.error(err)
        elif png_path:
            st.image(png_path, caption=f"{tile_id} — {lod}", use_container_width=True)
