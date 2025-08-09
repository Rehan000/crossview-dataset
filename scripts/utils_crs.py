from pyproj import Transformer

# Thread-safe in recent pyproj
_T_4326_TO_28992 = Transformer.from_crs("EPSG:4326", "EPSG:28992", always_xy=True)
_T_28992_TO_4326 = Transformer.from_crs("EPSG:28992", "EPSG:4326", always_xy=True)

def to_28992(lon: float, lat: float):
    """WGS84 (lon, lat) -> RD New (x, y) in meters."""
    return _T_4326_TO_28992.transform(lon, lat)

def to_4326(x: float, y: float):
    """RD New (x, y) -> WGS84 (lon, lat)."""
    return _T_28992_TO_4326.transform(x, y)
