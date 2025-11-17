#!/usr/bin/env python3
"""
ArcGIS REST API to GeoParquet Pipeline using dlt

Extracts spatial data from ArcGIS REST API endpoints and stores as yearly-partitioned
GeoParquet files in Google Cloud Storage.

Usage:
    # Run with default config (GCS destination)
    uv run pipelines/arcgis.py

    # Test locally with DuckDB
    uv run pipelines/arcgis.py --dry-run

    # Extract specific datasets only
    uv run pipelines/arcgis.py --datasets parcels streets

    # Use custom config file
    uv run pipelines/arcgis.py --config path/to/config.yaml
"""

import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlencode

import dlt
import requests
import yaml


class ArcGISClient:
    """Client for extracting data from ArcGIS REST API with pagination and retry logic."""

    def __init__(
        self,
        base_url: str,
        page_size: int = 1000,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        request_delay: float = 0.1,
    ):
        """
        Initialize ArcGIS REST API client.

        Args:
            base_url: Base URL of the ArcGIS layer query endpoint
            page_size: Number of records per page (resultRecordCount)
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Delay in seconds between retries
            request_delay: Delay in seconds between successful requests (rate limiting)
        """
        self.base_url = base_url
        self.page_size = page_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        self.session = requests.Session()

    def fetch_features(
        self, layer_name: str, max_records: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch all features from an ArcGIS layer with automatic pagination.

        Args:
            layer_name: Name of the layer (for logging)
            max_records: Optional limit on total records to fetch (for testing)

        Yields:
            Individual feature dictionaries with geometry and properties
        """
        offset = 0
        total_fetched = 0

        while True:
            # Build query parameters
            params = {
                "where": "1=1",  # Select all records
                "outFields": "*",  # All fields
                "f": "geojson",  # GeoJSON format
                "resultRecordCount": self.page_size,
                "resultOffset": offset,
            }

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

            # Parse GeoJSON features
            features = data.get("features", [])

            if not features:
                # No more data
                break

            # Yield individual features
            for feature in features:
                # Check if we've hit the record limit
                if max_records is not None and total_fetched >= max_records:
                    print(
                        f"Reached limit of {max_records} records for {layer_name} "
                        "(sampling mode)"
                    )
                    return

                # Flatten GeoJSON structure: merge properties with geometry
                record = {
                    "geometry": feature.get("geometry"),
                    **feature.get("properties", {}),
                }
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


def create_arcgis_resource(
    dataset_config: Dict[str, Any], current_year: int, max_records: Optional[int] = None
):
    """
    Create a dlt resource for an ArcGIS dataset.

    Args:
        dataset_config: Dataset configuration from YAML (name, layer_url, etc.)
        current_year: Current year for partitioning
        max_records: Optional limit on records to fetch (for testing/sampling)

    Returns:
        dlt resource configured for the dataset
    """
    name = dataset_config["name"]
    layer_url = dataset_config["layer_url"]
    geometry_type = dataset_config.get("geometry_type", "unknown")

    @dlt.resource(name=name, write_disposition="replace")
    def extract_dataset() -> Iterator[Dict[str, Any]]:
        """Extract data from ArcGIS REST API and add year partition."""
        client = ArcGISClient(base_url=layer_url)

        for record in client.fetch_features(layer_name=name, max_records=max_records):
            # Add partition column
            record["year"] = current_year
            # Add geometry type for downstream processing
            record["_geometry_type"] = geometry_type
            yield record

    return extract_dataset


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load dataset configuration from YAML file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def run_pipeline(
    config_path: Path,
    dry_run: bool = False,
    dataset_filter: Optional[List[str]] = None,
) -> None:
    """
    Run the ArcGIS extraction pipeline.

    Args:
        config_path: Path to dataset configuration YAML
        dry_run: If True, use DuckDB destination for local testing
        dataset_filter: Optional list of dataset names to extract (None = all)
    """
    # Load configuration
    config = load_config(config_path)
    datasets = config.get("arcgis_datasets", [])

    if not datasets:
        print("No datasets configured in YAML file")
        return

    # Filter datasets if specified
    if dataset_filter:
        datasets = [d for d in datasets if d["name"] in dataset_filter]
        if not datasets:
            print(f"No datasets matched filter: {dataset_filter}")
            return

    # Get current year for partitioning
    current_year = datetime.now().year

    # Set record limit for dry-run mode (sampling)
    max_records = 1000 if dry_run else None

    # Create dlt pipeline
    if dry_run:
        print("Running in DRY-RUN mode with DuckDB destination")
        print(f"Sampling mode: limiting to {max_records} records per dataset")
        pipeline = dlt.pipeline(
            pipeline_name="arcgis_test",
            destination="duckdb",
            dataset_name="arcgis_data",
        )
    else:
        print("Running with GCS filesystem destination")
        pipeline = dlt.pipeline(
            pipeline_name="arcgis_pipeline",
            destination="filesystem",
            dataset_name="arcgis",
        )

    # Create resources for all configured datasets
    resources = []
    for dataset_config in datasets:
        resource = create_arcgis_resource(dataset_config, current_year, max_records)
        resources.append(resource)
        print(f"Configured resource: {dataset_config['name']}")

    # Run pipeline
    print(f"\nStarting extraction for {len(resources)} dataset(s)...")
    print(f"Year partition: {current_year}")
    print("-" * 60)

    load_info = pipeline.run(resources)

    # Print results
    print("-" * 60)
    print("Pipeline completed successfully!")
    print(f"Loaded {len(load_info.loads_ids)} load(s)")
    print(f"Pipeline name: {load_info.pipeline.pipeline_name}")

    if dry_run:
        print("\nDRY-RUN: Data loaded to local DuckDB")
        print("To query data, use:")
        print("  import dlt")
        print("  pipeline = dlt.pipeline(pipeline_name='arcgis_test', destination='duckdb')")
        print("  dataset = pipeline.dataset()")
        print("  print(dataset.parcels.df())  # Example for parcels")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract ArcGIS data to GeoParquet using dlt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config
  uv run pipelines/arcgis.py

  # Test locally with DuckDB
  uv run pipelines/arcgis.py --dry-run

  # Extract specific datasets
  uv run pipelines/arcgis.py --datasets parcels streets

  # Use custom config
  uv run pipelines/arcgis.py --config my_config.yaml
        """,
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/arcgis_datasets.yaml"),
        help="Path to dataset configuration YAML (default: config/arcgis_datasets.yaml)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test mode: use DuckDB destination instead of GCS",
    )

    parser.add_argument(
        "--datasets",
        nargs="+",
        help="Extract only specified datasets (default: all)",
    )

    args = parser.parse_args()

    # Validate config file exists
    if not args.config.exists():
        print(f"Error: Config file not found: {args.config}")
        return 1

    try:
        run_pipeline(
            config_path=args.config,
            dry_run=args.dry_run,
            dataset_filter=args.datasets,
        )
        return 0
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
