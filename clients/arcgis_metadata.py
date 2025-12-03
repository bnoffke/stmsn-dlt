"""
ArcGIS REST API metadata extractor.

Fetches spatial metadata from ArcGIS FeatureServer/MapServer endpoints
including CRS, geometry type, extent, and field schemas.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any
import requests


@dataclass
class SpatialMetadata:
    """Spatial metadata extracted from ArcGIS service."""

    dataset_name: str
    source_url: str
    non_spatial: bool = False  # Flag for non-spatial datasets

    # Spatial reference
    crs_epsg: Optional[int] = None
    crs_wkid: Optional[int] = None
    crs_wkt: Optional[str] = None

    # Geometry info
    geometry_type: Optional[str] = None  # Point, MultiLineString, Polygon, etc.
    geometry_column: str = "geometry"

    # Spatial extent
    xmin: Optional[float] = None
    ymin: Optional[float] = None
    xmax: Optional[float] = None
    ymax: Optional[float] = None

    # Additional metadata
    has_z: bool = False
    has_m: bool = False

    # Timestamps
    extracted_at: Optional[str] = None

    # Validation tracking
    validation_status: Optional[str] = None  # 'passed', 'failed', 'skipped', 'no_baseline'
    validation_timestamp: Optional[str] = None
    validation_details: Optional[str] = None  # JSON string with validation results

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for dlt."""
        return asdict(self)


class ArcGISMetadataExtractor:
    """Extract metadata from ArcGIS REST API services."""

    GEOMETRY_TYPE_MAP = {
        "esriGeometryPoint": "Point",
        "esriGeometryMultipoint": "MultiPoint",
        "esriGeometryPolyline": "MultiLineString",
        "esriGeometryPolygon": "Polygon",
    }

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()

    def extract_metadata(
        self, layer_url: str, dataset_name: str
    ) -> SpatialMetadata:
        """
        Extract spatial metadata from ArcGIS service.

        Args:
            layer_url: URL to the layer query endpoint
            dataset_name: Name of the dataset (for tracking)

        Returns:
            SpatialMetadata object with extracted information
        """
        # Get base service URL (remove /query if present)
        service_url = layer_url.rstrip("/").replace("/query", "")

        # Fetch service definition
        params = {"f": "json"}
        response = self.session.get(
            service_url, params=params, timeout=self.timeout
        )
        response.raise_for_status()
        service_info = response.json()
        # Extract metadata
        metadata = SpatialMetadata(
            dataset_name=dataset_name,
            source_url=layer_url,
            extracted_at=datetime.now().isoformat(),
        )

        # Spatial reference (CRS)
        if "sourceSpatialReference" in service_info:
            sr = service_info["sourceSpatialReference"]
            metadata.crs_epsg = sr.get("latestWkid")
            metadata.crs_wkid = sr.get("wkid")
            metadata.crs_wkt = sr.get("wkt")

        # Geometry type
        geom_type_esri = service_info.get("geometryType")
        if geom_type_esri:
            metadata.geometry_type = self.GEOMETRY_TYPE_MAP.get(
                geom_type_esri, geom_type_esri
            )

        # Extent (bounding box)
        if "extent" in service_info:
            extent = service_info["extent"]
            metadata.xmin = extent.get("xmin")
            metadata.ymin = extent.get("ymin")
            metadata.xmax = extent.get("xmax")
            metadata.ymax = extent.get("ymax")

        # Z and M values
        metadata.has_z = service_info.get("hasZ", False)
        metadata.has_m = service_info.get("hasM", False)

        return metadata
