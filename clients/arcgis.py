"""
ArcGIS REST API client for data extraction.

Provides a reusable HTTP client for interacting with ArcGIS REST API endpoints
with built-in pagination, retry logic, and rate limiting.
"""

import time
from typing import Any, Dict, Iterator, Optional, List
from urllib.parse import urlencode
from dataclasses import dataclass, field

import requests
from shapely.geometry import shape, Polygon, LineString, MultiLineString, Point


@dataclass
class MetricConfig:
    """Configuration for a validation metric."""
    name: str
    aggregate: str  # 'sum', 'avg', 'min', 'max', 'count'
    api_field: str  # Field name in ArcGIS API JSON response
    tolerance_percent: Optional[float] = None


@dataclass
class MetricsAccumulator:
    """
    Accumulates metrics while iterating through records.

    This collects validation metrics in-flight during data fetch
    to avoid making separate API calls for validation.
    """
    metric_configs: List[MetricConfig] = field(default_factory=list)
    row_count: int = 0

    # Accumulated values by metric name
    _sums: Dict[str, float] = field(default_factory=dict)
    _counts: Dict[str, int] = field(default_factory=dict)
    _mins: Dict[str, float] = field(default_factory=dict)
    _maxs: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize accumulator storage for each metric."""
        for metric in self.metric_configs:
            if metric.aggregate == 'sum':
                self._sums[metric.name] = 0.0
            elif metric.aggregate == 'avg':
                self._sums[metric.name] = 0.0
                self._counts[metric.name] = 0
            elif metric.aggregate == 'min':
                self._mins[metric.name] = float('inf')
            elif metric.aggregate == 'max':
                self._maxs[metric.name] = float('-inf')
            elif metric.aggregate == 'count':
                self._counts[metric.name] = 0

    def add_record(self, record: Dict[str, Any]) -> None:
        """
        Process a single record and update metric accumulators.

        Args:
            record: Feature dictionary with properties
        """
        self.row_count += 1

        for metric in self.metric_configs:
            value = record.get(metric.api_field)

            # Skip null/None values
            if value is None:
                continue

            try:
                # Convert to float for numeric operations
                numeric_value = float(value)

                if metric.aggregate == 'sum':
                    self._sums[metric.name] += numeric_value
                elif metric.aggregate == 'avg':
                    self._sums[metric.name] += numeric_value
                    self._counts[metric.name] += 1
                elif metric.aggregate == 'min':
                    self._mins[metric.name] = min(self._mins[metric.name], numeric_value)
                elif metric.aggregate == 'max':
                    self._maxs[metric.name] = max(self._maxs[metric.name], numeric_value)
                elif metric.aggregate == 'count':
                    self._counts[metric.name] += 1

            except (ValueError, TypeError):
                # Skip non-numeric values
                pass

    def get_results(self) -> Dict[str, Any]:
        """
        Get final computed metrics.

        Returns:
            Dictionary with metric names and their computed values
        """
        results = {'row_count': self.row_count}

        for metric in self.metric_configs:
            if metric.aggregate == 'sum':
                results[metric.name] = self._sums[metric.name]
            elif metric.aggregate == 'avg':
                count = self._counts[metric.name]
                results[metric.name] = self._sums[metric.name] / count if count > 0 else 0.0
            elif metric.aggregate == 'min':
                val = self._mins[metric.name]
                results[metric.name] = val if val != float('inf') else None
            elif metric.aggregate == 'max':
                val = self._maxs[metric.name]
                results[metric.name] = val if val != float('-inf') else None
            elif metric.aggregate == 'count':
                results[metric.name] = self._counts[metric.name]

        return results


class ArcGISClient:
    """Client for extracting data from ArcGIS REST API with pagination and retry logic."""

    def __init__(
        self,
        base_url: str,
        page_size: int = 1000,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        request_delay: float = 0.1,
        convert_to_wkb: bool = True,
        output_crs: Optional[int] = None,
    ):
        """
        Initialize ArcGIS REST API client.

        Args:
            base_url: Base URL of the ArcGIS layer query endpoint
            page_size: Number of records per page (resultRecordCount)
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Delay in seconds between retries
            request_delay: Delay in seconds between successful requests (rate limiting)
            convert_to_wkb: Convert geometry to WKB hex string (default: True)
            output_crs: Optional CRS EPSG code to request from ArcGIS (e.g., 8193).
                       If specified, uses ArcGIS JSON format with native CRS.
                       If None, uses GeoJSON format (WGS84/EPSG:4326).
        """
        self.base_url = base_url
        self.page_size = page_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        self.convert_to_wkb = convert_to_wkb
        self.output_crs = output_crs
        self.session = requests.Session()

    def _arcgis_geom_to_shapely(self, geom_data: Dict, geom_type: str):
        """
        Convert ArcGIS JSON geometry to Shapely geometry.

        Args:
            geom_data: ArcGIS JSON geometry object
            geom_type: ArcGIS geometry type (e.g., "esriGeometryPolygon")

        Returns:
            Shapely geometry object

        Raises:
            ValueError: If geometry type is not supported
        """
        if geom_type == "esriGeometryPolygon":
            rings = geom_data.get("rings", [])
            if not rings:
                return None
            # First ring is exterior, rest are holes
            exterior = rings[0]
            holes = rings[1:] if len(rings) > 1 else None
            return Polygon(exterior, holes)

        elif geom_type == "esriGeometryPolyline":
            paths = geom_data.get("paths", [])
            if not paths:
                return None
            if len(paths) == 1:
                return LineString(paths[0])
            else:
                return MultiLineString(paths)

        elif geom_type == "esriGeometryPoint":
            x = geom_data.get("x")
            y = geom_data.get("y")
            if x is None or y is None:
                return None
            return Point(x, y)

        else:
            raise ValueError(f"Unsupported geometry type: {geom_type}")

    def fetch_features(
        self,
        layer_name: str,
        max_records: Optional[int] = None,
        metrics_accumulator: Optional[MetricsAccumulator] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch all features from an ArcGIS layer with automatic pagination.

        Args:
            layer_name: Name of the layer (for logging)
            max_records: Optional limit on total records to fetch (for testing)
            metrics_accumulator: Optional MetricsAccumulator to collect validation metrics
                               in-flight during data fetch

        Yields:
            Individual feature dictionaries with geometry (as WKB hex if convert_to_wkb=True)
            and properties
        """
        offset = 0
        total_fetched = 0

        while True:
            # Build query parameters
            params = {
                "where": "1=1",  # Select all records
                "outFields": "*",  # All fields
                "resultRecordCount": self.page_size,
                "resultOffset": offset,
            }

            # Choose format based on CRS requirements
            if self.output_crs:
                # Use ArcGIS JSON format with native CRS
                params["f"] = "json"
                params["outSR"] = str(self.output_crs)
            else:
                # Use GeoJSON format (automatically WGS84/EPSG:4326)
                params["f"] = "geojson"

            url = f"{self.base_url}?{urlencode(params)}"

            # Retry logic
            for attempt in range(self.max_retries):
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    break
                except (requests.RequestException, ValueError) as e:
                    if attempt == self.max_retries - 1:
                        print(
                            f"Error fetching {layer_name} at offset {offset} "
                            f"after {self.max_retries} attempts: {e}"
                        )
                        raise
                    print(
                        f"Retry {attempt + 1}/{self.max_retries} for {layer_name} "
                        f"at offset {offset}: {e}"
                    )
                    time.sleep(self.retry_delay * (attempt + 1))

            # Parse features (format depends on API response type)
            features = data.get("features", [])

            if not features:
                # No more data
                break

            # Extract geometry type for ArcGIS JSON format
            geom_type = data.get("geometryType")  # e.g., "esriGeometryPolygon"

            # Yield individual features
            for feature in features:
                # Check if we've hit the record limit
                if max_records is not None and total_fetched >= max_records:
                    print(
                        f"Reached limit of {max_records} records for {layer_name} "
                        "(sampling mode)"
                    )
                    return

                # Handle different response formats
                if self.output_crs:
                    # ArcGIS JSON format: attributes are separate from geometry
                    record = {**feature.get("attributes", {})}
                else:
                    # GeoJSON format: properties are separate from geometry
                    record = {**feature.get("properties", {})}

                # Handle geometry conversion
                geom_data = feature.get("geometry")
                if geom_data and self.convert_to_wkb:
                    try:
                        if self.output_crs:
                            # Convert ArcGIS JSON geometry → Shapely → WKB hex
                            geom = self._arcgis_geom_to_shapely(geom_data, geom_type)
                        else:
                            # Convert GeoJSON geometry → Shapely → WKB hex
                            geom = shape(geom_data)

                        if geom:
                            record["geometry"] = geom.wkb_hex
                        else:
                            record["geometry"] = None
                    except Exception as e:
                        print(
                            f"Warning: Failed to convert geometry for {layer_name}: {e}"
                        )
                        record["geometry"] = None
                else:
                    # Keep as original dict (for backward compatibility)
                    record["geometry"] = geom_data

                # Accumulate metrics if configured
                if metrics_accumulator is not None:
                    metrics_accumulator.add_record(record)

                yield record
                total_fetched += 1

            print(f"Fetched {total_fetched} records from {layer_name}...")

            # Move to next page
            offset += len(features)

            # Rate limiting
            time.sleep(self.request_delay)

            # If we got fewer records than page_size, we're done
            if len(features) < self.page_size:
                break

        print(f"Completed {layer_name}: {total_fetched} total records")
