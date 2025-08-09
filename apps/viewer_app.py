# apps/viewer_app.py
import json
from pathlib import Path

import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyproj import Transformer

# New: folium stack
import folium
from folium.plugins import MarkerCluster

# Optional (for 3D preview)
try:
    import trimesh
    import pyrender
    HAS_3D = True
except Exception:
    HAS_3D = False

DATA_ROOT = Path("data/amsterdam")
PROCESSED_ROOT = DATA_ROOT / "processed"
MESH_ROOT = DATA_ROOT / "mesh"
MAP_ROOT = DATA_ROOT / "mapillary"
AERIAL_ROOT = DATA_ROOT / "aerial"

# PDOK aerial WMTS (EPSG:3857) via WMTS GetTile template
PDOK_AERIAL_URL = (
    "https://service.pdok.nl/hwh/luchtfotorgb/wmts/v1_0"
    "?service=WMTS&request=GetTile&version=1.0.0"
    "&layer=Actueel_ortho25&style=default&format=image/jpeg"
    "&TileMatrixSet=EPSG:3857&TileMatrix={z}&TileCol={x}&TileRow={y}"
)

st.set_page_config(page_title="CrossView Viewer", layout="wide")
st.title("CrossView Viewer — Amsterdam")

def list_tiles():
    if not PROCESSED_ROOT.exists():
        return []
    return sorted([p.name for p in PROCESSED_ROOT.iterdir() if p.is_dir()])

def load_manifest(tile_id: str) -> dict:
    mpath = PROCESSED_ROOT / tile_id / "manifest.json"
    if mpath.exists():
        return json.loads(mpath.read_text())
    return {}

def load_buildings(tile_id: str, prefer=("lod22_2d","lod13_2d","lod12_2d")) -> gpd.GeoDataFrame:
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
            # Ensure lon/lat; compute from x_28992/y_28992 if needed
            if not {"lon","lat"}.issubset(df.columns):
                if {"x_28992","y_28992"}.issubset(df.columns):
                    t = Transformer.from_crs(28992, 4326, always_xy=True)
                    lon, lat = t.transform(df["x_28992"].values, df["y_28992"].values)
                    df["lon"], df["lat"] = lon, lat
                else:
                    df["lon"], df["lat"] = None, None
            # local thumb path (if downloaded)
            img_dir = base / "images"
            if img_dir.exists():
                df["thumb_local"] = df["id"].astype(str).map(lambda i: str((img_dir / f"{i}.jpg").resolve()))
            else:
                df["thumb_local"] = None
            return df
    return pd.DataFrame(columns=["id","lon","lat"])

def tile_center_from_manifest(tile_id: str):
    man = load_manifest(tile_id)
    wkt = man.get("tile_polygon_wkt")
    if wkt:
        g = gpd.GeoSeries.from_wkt([wkt], crs=28992).to_crs(4326)
        c = g.iloc[0].centroid
        return c.y, c.x
    # fallback: center from buildings
    b = load_buildings(tile_id)
    if not b.empty:
        c = b.union_all().centroid
        return c.y, c.x
    return (52.3728, 4.8936)  # central AMS fallback

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

    # Buildings overlay (as GeoJSON, outline only)
    if not buildings.empty:
        gj = folium.GeoJson(
            data=json.loads(buildings.to_json()),
            name=f"Buildings ({buildings.get('layer', 'lod22_2d').iloc[0] if 'layer' in buildings.columns and len(buildings)>0 else '2D'})",
            style_function=lambda feat: {"color": "#000000", "weight": 1, "fill": False},
            control=True,
        )
        gj.add_to(m)

    # Mapillary points (clustered)
    if not cams.empty and {"lon","lat"}.issubset(cams.columns):
        cams_ok = cams.dropna(subset=["lon","lat"]).copy()
        mc = MarkerCluster(name="Mapillary").add_to(m)
        for _, r in cams_ok.iterrows():
            popup_html = f"<b>ID:</b> {r.get('id')}<br>"
            if r.get("captured_at_utc"):
                popup_html += f"<b>Time:</b> {r.get('captured_at_utc')}<br>"
            if r.get("camera_type"):
                popup_html += f"<b>Cam:</b> {r.get('camera_type')}<br>"
            if r.get("thumb_local") and Path(str(r.get('thumb_local'))).exists():
                # Use relative file path in <img>; Streamlit serves local files from absolute paths too
                popup_html += f"<img src='file://{r.get('thumb_local')}' width='240'/>"
            folium.Marker(
                location=(float(r["lat"]), float(r["lon"])),
                icon=folium.Icon(color="red", icon="camera", prefix="fa"),
                popup=folium.Popup(popup_html, max_width=260),
            ).add_to(mc)

    folium.LayerControl(collapsed=False).add_to(m)
    return m

def render_mesh_snapshot(tile_id: str, lod="LoD22", width=900, height=700, bg=(255,255,255,255)):
    """
    Render a simple PNG snapshot of the LoD OBJ using trimesh+pyrender.
    Returns path to the PNG if successful, else None.
    """
    if not HAS_3D:
        return None, "Install trimesh and pyrender to enable 3D preview: conda install -c conda-forge trimesh pyrender"
    obj_dir = MESH_ROOT / tile_id / "obj" / lod
    if not obj_dir.exists():
        return None, f"No {lod} directory found."
    # Pick the first OBJ
    objs = sorted(obj_dir.glob("*.obj"))
    if not objs:
        return None, f"No OBJ files found in {obj_dir}"
    obj_path = objs[0]
    try:
        mesh = trimesh.load_mesh(obj_path, force="mesh")
        if mesh.is_empty:
            return None, "OBJ loaded but mesh is empty."
        if not isinstance(mesh, trimesh.Trimesh) and hasattr(mesh, "dump"):
            # e.g., Scene or list — try to merge
            try:
                mesh = trimesh.util.concatenate(mesh.dump())
            except Exception:
                pass
        scene = pyrender.Scene(bg_color=bg, ambient_light=(0.4, 0.4, 0.4, 1.0))
        # Convert to pyrender.Mesh
        pm = pyrender.Mesh.from_trimesh(mesh, smooth=False)
        scene.add(pm)
        # Camera
        camera = pyrender.PerspectiveCamera(yfov=45.0 * 3.14159/180.0)
        # Position camera a bit off to show perspective
        cam_node = scene.add(camera, pose=trimesh.transformations.translation_matrix([0, -500, 300]))
        # Light
        light = pyrender.DirectionalLight(color=[1.0, 1.0, 1.0], intensity=3.0)
        scene.add(light, pose=trimesh.transformations.translation_matrix([0, -200, 500]))
        # Offscreen renderer
        r = pyrender.OffscreenRenderer(viewport_width=width, viewport_height=height)
        color, _ = r.render(scene)
        out_dir = Path("outputs/mesh_snapshots") / tile_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{lod}.png"
        import imageio.v2 as iio
        iio.imwrite(out_path, color)
        return str(out_path), None
    except Exception as e:
        return None, f"Failed to render mesh: {e}"

# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────
tiles = list_tiles()
if not tiles:
    st.warning("No manifests found under data/amsterdam/processed. Run build_manifest first.")
    st.stop()

# Default to 10-430-720 if present
default_idx = tiles.index("10-430-720") if "10-430-720" in tiles else 0
tile_id = st.sidebar.selectbox("Tile", tiles, index=default_idx)

tab_map, tab_mesh = st.tabs(["🗺️ Map (Folium + PDOK)", "🧱 3D Mesh Preview"])

with tab_map:
    st.subheader("Aerial + Buildings + Mapillary")
    buildings = load_buildings(tile_id)
    cams = load_mapillary_points(tile_id)
    fmap = build_folium_map(tile_id, buildings, cams, zoom=16)
    # Render folium in Streamlit
    from streamlit_folium import st_folium
    st_folium(fmap, width=None, height=700)

    # Simple gallery (clean subset preferred)
    st.markdown("### Mapillary gallery")
    if cams.empty:
        st.info("No Mapillary metadata found.")
    else:
        if "captured_at_utc" in cams.columns:
            cams = cams.sort_values("captured_at_utc", ascending=False)
        count = st.slider("How many images to show", 8, 48, 24, step=4)
        subset = cams.head(count).copy()
        ncols = 4
        rows = (len(subset) + ncols - 1) // ncols
        for r in range(rows):
            cols = st.columns(ncols)
            for c in range(ncols):
                idx = r * ncols + c
                if idx >= len(subset):
                    break
                row = subset.iloc[idx]
                cap = f"id: {row.get('id')}"
                if row.get("captured_at_utc"):
                    cap += f"\n{row.get('captured_at_utc')}"
                thumb = row.get("thumb_local")
                if thumb and Path(thumb).exists():
                    cols[c].image(thumb, caption=cap, use_container_width=True)  # ✅ no deprecation
                else:
                    cols[c].markdown(cap)

with tab_mesh:
    st.subheader("LoD22 OBJ snapshot")
    if not HAS_3D:
        st.info("To enable 3D preview, install: `conda install -c conda-forge trimesh pyrender`")
    lod = st.selectbox("LoD folder", ["LoD22", "LoD13", "LoD12"], index=0)
    if st.button("Render snapshot"):
        png_path, err = render_mesh_snapshot(tile_id, lod=lod)
        if err:
            st.error(err)
        elif png_path:
            st.image(png_path, caption=f"{tile_id} — {lod}", use_container_width=True)
