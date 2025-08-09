# CrossView Dataset Builder

This repository contains a pipeline to automatically build a **multi-modal geospatial dataset** combining:

1. **3D Meshes** from the Dutch 3D BAG (LoD12, LoD13, LoD22)
2. **Street-level imagery** from Mapillary
3. **Aerial imagery** from PDOK WMS services

The system downloads, processes, verifies, and integrates these modalities for specific Dutch tile IDs (e.g., `10-430-720`) and produces a clean, ready-to-use dataset with metadata manifests.

---

## 📂 Project Structure

```
crossview-dataset/
│
├── apps/                 # Visualization tools (Streamlit/Folium viewers)
├── scripts/              # Individual data acquisition and processing scripts
├── tools/                # Orchestrators to run multiple steps for multiple tiles
├── data/amsterdam/       # Default output root for downloaded & processed data
│   ├── mesh/             # 3D BAG meshes & GeoPackages
│   ├── mapillary/        # Mapillary images & metadata
│   ├── aerial/           # Aerial orthophotos
│   ├── processed/        # Final processed manifests
│   └── tiles_XxX.csv     # Tile selection files
├── outputs/verify/       # Mapping verification diagnostics
├── environment.yml       # Conda environment definition
└── README.md
```

---

## ⚙️ Setup

### 1. Install Conda environment
```bash
conda env create -f environment.yml
conda activate crossview
```

### 2. Configure Mapillary Access Token
Create a `.env` file in the repository root:
```
MAPILLARY_ACCESS_TOKEN=your_mapillary_access_token_here
```

---

## 🔄 Pipeline Overview

The dataset generation pipeline consists of the following steps:

1. **Select tiles** — choose tile IDs to process.
2. **Download 3D Mesh** — fetch LoD12, LoD13, LoD22 models and GeoPackages from 3D BAG.
3. **Fetch Mapillary imagery** — download street-level images within tile bounds.
4. **Augment metadata** — reproject coordinates & store in multiple formats.
5. **Verify mapping** — ensure imagery corresponds to correct building footprints.
6. **Clean subset** — filter images to those inside tile & within max distance to buildings.
7. **Fetch aerial imagery** — download PDOK aerial orthophotos.
8. **Build manifest** — create JSON manifest linking all modalities.

---

## 📍 Step-by-Step Usage

### **1. Selecting Tiles**
You can select neighboring tiles around a starting tile:
```bash
python -m scripts.select_tiles neighbors   --tile-id 10-430-720 --k 2 > data/amsterdam/tiles_5x5.csv
```
This creates a CSV with tile IDs for a 5x5 grid around the target tile.

---

### **2. Running the Pipeline for Multiple Tiles**
Use the orchestrator to run all steps for all tiles in a CSV:
```bash
python -m tools.run_for_tiles   --csv data/amsterdam/tiles_5x5.csv   --steps mesh,mapillary,augment,verify,clean,aerial,manifest   --max-images 1200   --margin-m 20   --aerial-layer Actueel_ortho25   --aerial-gsd 0.25   --sleep 0.5
```

---

### **3. Running Steps Individually**
You can run each step separately:

#### **Download 3D Mesh**
```bash
python -m scripts.download_3dbag_tile --tile-id 10-430-720
```

#### **Fetch Mapillary Imagery**
```bash
python -m scripts.fetch_mapillary --tile-id 10-430-720 --max-images 2000 --margin-m 20
```

#### **Augment Metadata**
```bash
python -m scripts.augment_meta --tile-id 10-430-720
```

#### **Verify Mapping**
```bash
python -m scripts.verify_mapping --tile-id 10-430-720
```

#### **Make Clean Subset**
```bash
python -m scripts.make_clean_subset --tile-id 10-430-720 --symlink-images
```

#### **Fetch Aerial Imagery**
```bash
python -m scripts.fetch_aerial_nl --tile-id 10-430-720 --layer Actueel_ortho25 --gsd 0.25
```

#### **Build Manifest**
```bash
python -m scripts.build_manifest   --tile-id 10-430-720   --mesh-root data/amsterdam/mesh   --mapillary-root data/amsterdam/mapillary   --aerial-root data/amsterdam/aerial   --out-root data/amsterdam/processed
```

---

## 📊 Verification Outputs
After running `verify_mapping`, you will find:
```
outputs/verify/{tile_id}/
  ├── diagnostics.csv            # Statistics for distances & coverage
  ├── hist_dist_buildings.png    # Histogram of distances
  └── overlay.png                 # Visual overlay of points and buildings
```

---

## 🛰 Modalities

### **3D Mesh (3D BAG)**
- LoD12, LoD13, LoD22 OBJ models
- GPKG files with building footprints

### **Street-level Imagery (Mapillary)**
- Filtered to be inside the tile polygon & near building footprints
- Metadata stored as `.jsonl` and `.parquet`

### **Aerial Imagery (PDOK WMS)**
- Orthophotos at 0.25 m/px
- Stored as `.tif` and `.png`

---

## 👀 Visualization

### **Streamlit App**
```bash
streamlit run apps/viewer_app.py
```
- Displays building footprints, image points, and aerial imagery overlay
- Uses PDOK aerial tiles as base map

---

## 🗺 Example Tile Folder Structure
```
data/amsterdam/
  ├── mesh/10-430-720/
  │   ├── 10-430-720.gpkg
  │   ├── obj/LoD12/
  │   ├── obj/LoD13/
  │   └── obj/LoD22/
  ├── mapillary/10-430-720/
  │   ├── images/
  │   ├── meta.jsonl
  │   ├── meta_28992.parquet
  │   ├── meta_clean.parquet
  │   └── images_clean/
  ├── aerial/10-430-720/
  │   ├── aerial_Actueel_ortho25_0.25m.tif
  │   └── aerial_Actueel_ortho25_0.25m.png
  └── processed/10-430-720/
      └── manifest.json
```

---

## 📌 Notes
- Mapillary imagery depends on coverage in the selected tile — some tiles may return few or no images.
- The verification step is critical to ensure dataset quality.
- Satellite imagery modality was removed due to insufficient resolution for our use case.

---

## 📜 License
MIT License — see `LICENSE` for details.

---

## 🙌 Acknowledgements
- [3D BAG](https://3dbag.nl/)
- [Mapillary](https://www.mapillary.com/)
- [PDOK](https://www.pdok.nl/)