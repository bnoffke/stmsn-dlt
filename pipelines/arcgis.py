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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import dlt
import yaml

from clients.arcgis import ArcGISClient, MetricsAccumulator, MetricConfig
from clients.arcgis_metadata import ArcGISMetadataExtractor
from utils.geoparquet_finalizer import GeoParquetFinalizer
from utils.data_validator import DataValidator, ValidationError
import json

# Global variable to collect validation results across resources
_validation_results = []


def create_validation_resource():
    """
    Create a dlt resource for validation results tracking.

    This resource stores validation history for all datasets,
    allowing audit trail of data quality checks.

    Returns:
        dlt resource for validation results
    """

    @dlt.resource(
        name="validation_results",
        write_disposition="append",  # Keep historical records
    )
    def extract_validation_results() -> Iterator[Dict[str, Any]]:
        """Yield collected validation results."""
        for result in _validation_results:
            yield result

    return extract_validation_results


def create_metadata_resource(datasets: List[Dict[str, Any]]):
    """
    Create a dlt resource for spatial metadata registry.

    This resource extracts metadata from all configured datasets
    and stores it in a tracking table for the geoparquet finalizer.

    Args:
        datasets: List of dataset configurations

    Returns:
        dlt resource for metadata extraction
    """

    @dlt.resource(
        name="spatial_metadata_registry",
        write_disposition="replace",
        primary_key="dataset_name",
    )
    def extract_metadata() -> Iterator[Dict[str, Any]]:
        """Extract spatial metadata from all ArcGIS services."""
        extractor = ArcGISMetadataExtractor()

        for dataset_config in datasets:
            name = dataset_config["name"]
            layer_url = dataset_config["layer_url"]
            non_spatial = dataset_config.get("non_spatial", False)

            print(f"Extracting metadata for {name}...")

            if non_spatial:
                print(f"  Dataset marked as non-spatial, will skip geoparquet conversion")

            try:
                metadata = extractor.extract_metadata(layer_url, name)
                metadata.non_spatial = non_spatial
                yield metadata.to_dict()
            except Exception as e:
                print(f"Warning: Failed to extract metadata for {name}: {e}")
                # Yield minimal metadata so we don't fail the entire pipeline
                yield {
                    "dataset_name": name,
                    "source_url": layer_url,
                    "extracted_at": datetime.now().isoformat(),
                    "geometry_type": dataset_config.get(
                        "geometry_type"
                    ),  # fallback to config
                    "non_spatial": non_spatial,
                }

    return extract_metadata


def create_arcgis_resource(
    dataset_config: Dict[str, Any],
    jurisdiction: str,
    crs: Optional[int] = None,
    max_records: Optional[int] = None,
    validator: Optional[DataValidator] = None,
    skip_validation: bool = False,
    current_year: Optional[int] = None,
):
    """
    Create a dlt resource for an ArcGIS dataset with optional validation.

    Args:
        dataset_config: Dataset configuration from YAML (name, layer_url, validation config, etc.)
        jurisdiction: Jurisdiction name for validation baseline queries
        crs: CRS EPSG code to request from ArcGIS (e.g., 8193 for Madison)
        max_records: Optional limit on records to fetch (for testing/sampling)
        validator: Optional DataValidator instance for validation
        skip_validation: If True, skip validation even if configured
        current_year: Current year for filtering validation baseline data

    Returns:
        dlt resource configured for the dataset

    Raises:
        ValidationError: If validation is enabled and fails
    """
    name = dataset_config["name"]
    layer_url = dataset_config["layer_url"]
    validation_config = dataset_config.get("validation", {})

    # Check if validation is enabled for this dataset
    validation_enabled = (
        validation_config.get("enabled", True)  # Default to enabled if validation block exists
        and validator is not None
        and not skip_validation
        and max_records is None  # Don't validate in sampling mode
    )

    @dlt.resource(
        name=name,
        write_disposition="replace",
        columns={"geometry": {"data_type": "text"}},  # Treat WKB hex as text
    )
    def extract_dataset() -> Iterator[Dict[str, Any]]:
        """Extract data from ArcGIS REST API with WKB geometry and optional validation."""
        client = ArcGISClient(base_url=layer_url, convert_to_wkb=True, output_crs=crs)

        # Setup metrics accumulator if validation is enabled
        metrics_accumulator = None
        metric_configs_list = []

        if validation_enabled and validation_config.get("metrics"):
            # Parse metric configurations from YAML
            for metric_cfg in validation_config["metrics"]:
                metric_configs_list.append(
                    MetricConfig(
                        name=metric_cfg["parquet_column"],
                        aggregate=metric_cfg["aggregate"],
                        api_field=metric_cfg["api_field"],
                        tolerance_percent=metric_cfg.get("tolerance_percent"),
                    )
                )

            metrics_accumulator = MetricsAccumulator(metric_configs=metric_configs_list)
            print(f"Validation enabled for {name} with {len(metric_configs_list)} metric(s)")

        # Buffer records for validation
        buffered_records = []

        # Fetch features with optional metrics collection
        for record in client.fetch_features(
            layer_name=name,
            max_records=max_records,
            metrics_accumulator=metrics_accumulator,
        ):
            if validation_enabled:
                # Buffer records for validation
                buffered_records.append(record)
            else:
                # No validation - yield immediately
                yield record

        # Perform validation if enabled
        if validation_enabled:
            print(f"\nData fetch completed for {name}. Running validation...")

            # Get tolerance from config FIRST (for fallback)
            tolerance_percent = validation_config.get("tolerance_percent", 5.0)

            # Check if there are custom metrics to validate
            if metrics_accumulator is None:
                # No custom metrics - validate row count only
                print(f"Validating row count only for {name} (no custom metrics configured)")
                incoming_metrics = {"row_count": len(buffered_records)}
                validation_metric_configs = []  # Empty list = only row count validation
            else:
                # Get accumulated custom metrics
                incoming_metrics = metrics_accumulator.get_results()
                print(f"Incoming metrics: {incoming_metrics}")

                # Prepare metric configs for validator
                validation_metric_configs = [
                    {
                        "name": mc.name,
                        "aggregate": mc.aggregate,
                        "parquet_column": mc.name,  # Use normalized name for parquet queries
                        "tolerance_percent": mc.tolerance_percent or tolerance_percent,  # Fallback to dataset-level
                    }
                    for mc in metric_configs_list
                ]

            # Validate against baseline
            validation_status = "skipped"
            try:
                all_passed, validation_results_list = validator.validate_dataset(
                    jurisdiction=jurisdiction,
                    dataset_name=name,
                    incoming_metrics=incoming_metrics,
                    metric_configs=validation_metric_configs,
                    year=current_year,
                )

                # Check if validation was skipped (no baseline data)
                if all_passed and len(validation_results_list) == 0:
                    # No baseline data - first load
                    validation_status = "no_baseline"
                    _validation_results.append({
                        "dataset_name": name,
                        "jurisdiction": jurisdiction,
                        "validation_timestamp": datetime.now().isoformat(),
                        "status": validation_status,
                        "results": json.dumps({"message": "No baseline data found - first load"}),
                    })
                    print(f"✓ No baseline found for {name}. Proceeding with first load.")

                elif not all_passed:
                    # Validation failed - collect results and raise error
                    validation_status = "failed"
                    _validation_results.append({
                        "dataset_name": name,
                        "jurisdiction": jurisdiction,
                        "validation_timestamp": datetime.now().isoformat(),
                        "status": validation_status,
                        "results": json.dumps([
                            {
                                "metric": r.metric_name,
                                "baseline": r.baseline_value,
                                "incoming": r.incoming_value,
                                "percent_diff": r.percent_difference,
                                "passed": r.passed,
                            }
                            for r in validation_results_list
                        ]),
                    })
                    raise ValidationError(dataset_name=name, results=validation_results_list)

                else:
                    # Validation passed
                    validation_status = "passed"
                    _validation_results.append({
                        "dataset_name": name,
                        "jurisdiction": jurisdiction,
                        "validation_timestamp": datetime.now().isoformat(),
                        "status": validation_status,
                        "results": json.dumps([
                            {
                                "metric": r.metric_name,
                                "baseline": r.baseline_value,
                                "incoming": r.incoming_value,
                                "percent_diff": r.percent_difference,
                                "passed": r.passed,
                            }
                            for r in validation_results_list
                        ]),
                    })
                    print(f"✓ Validation passed for {name}. Proceeding with load.")

            except Exception as e:
                # Unexpected error during validation - re-raise
                print(f"✗ Validation error for {name}: {e}")
                raise

            # Validation completed (passed, failed was raised, or no baseline) - yield buffered records
            for record in buffered_records:
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
    skip_geoparquet: bool = False,
    local_output: Optional[Path] = None,
    skip_validation: bool = False,
) -> None:
    """
    Run the ArcGIS extraction pipeline.

    Args:
        config_path: Path to dataset configuration YAML
        dry_run: If True, use DuckDB destination for local testing
        dataset_filter: Optional list of dataset names to extract (None = all)
        skip_geoparquet: If True, skip geoparquet conversion (write standard parquet only)
        local_output: If provided, write to local filesystem path (auto-limits to 1000 records)
        skip_validation: If True, skip data validation even if configured
    """
    # Load configuration
    config = load_config(config_path)
    jurisdictions = config.get("jurisdictions", {})

    if not jurisdictions:
        print("No jurisdictions configured in YAML file")
        return

    # Get current year for partitioning
    current_year = datetime.now().year

    # Set record limit for dry-run or local-output mode (sampling)
    max_records = 1000 if (dry_run or local_output) else None

    # Flatten datasets with jurisdiction info for filtering
    all_datasets = []
    for jurisdiction_name, jurisdiction_config in jurisdictions.items():
        jurisdiction_crs = jurisdiction_config.get("crs")
        for dataset in jurisdiction_config.get("datasets", []):
            # Add jurisdiction context to each dataset
            dataset_with_context = {
                **dataset,
                "jurisdiction": jurisdiction_name,
                "jurisdiction_crs": jurisdiction_crs,
            }
            all_datasets.append(dataset_with_context)

    # Filter datasets if specified
    if dataset_filter:
        all_datasets = [d for d in all_datasets if d["name"] in dataset_filter]
        if not all_datasets:
            print(f"No datasets matched filter: {dataset_filter}")
            return

    # Group datasets by jurisdiction
    datasets_by_jurisdiction = {}
    for dataset in all_datasets:
        jurisdiction = dataset["jurisdiction"]
        if jurisdiction not in datasets_by_jurisdiction:
            datasets_by_jurisdiction[jurisdiction] = {
                "crs": dataset["jurisdiction_crs"],
                "datasets": []
            }
        datasets_by_jurisdiction[jurisdiction]["datasets"].append(dataset)

    print(f"Found {len(datasets_by_jurisdiction)} jurisdiction(s): {', '.join(datasets_by_jurisdiction.keys())}")

    # Process each jurisdiction separately
    for jurisdiction, jurisdiction_data in datasets_by_jurisdiction.items():
        print(f"\n{'=' * 60}")
        print(f"Processing jurisdiction: {jurisdiction}")
        print(f"CRS: EPSG:{jurisdiction_data['crs']}")
        print(f"{'=' * 60}")

        jurisdiction_crs = jurisdiction_data["crs"]
        jurisdiction_datasets = jurisdiction_data["datasets"]

        # Create dlt pipeline
        if dry_run:
            print("Running in DRY-RUN mode with DuckDB destination")
            print(f"Sampling mode: limiting to {max_records} records per dataset")
            pipeline = dlt.pipeline(
                pipeline_name=f"arcgis_{jurisdiction}_test",
                destination="duckdb",
                dataset_name=jurisdiction,
            )
        elif local_output:
            print("Running with LOCAL FILESYSTEM destination")
            print(f"Sampling mode: limiting to {max_records} records per dataset")

            # Create local path structure: {local_output}/arcgis
            # Dataset name (jurisdiction) will be appended by dlt
            base_bucket_url = f"{local_output.absolute()}/arcgis"

            print(f"Local output path: {base_bucket_url}/{jurisdiction}")

            pipeline = dlt.pipeline(
                pipeline_name=f"arcgis_{jurisdiction}_local",
                destination=dlt.destinations.filesystem(bucket_url=base_bucket_url),
                dataset_name=jurisdiction,
            )

            # Full path for finalizer (includes jurisdiction)
            jurisdiction_data_path = f"{base_bucket_url}/{jurisdiction}"
        else:
            print("Running with GCS filesystem destination")

            # Get base bucket URL from dlt secrets
            # Expected format in secrets.toml: bucket_url = "gs://${BUCKET_NAME}/bronze"
            base_gcs_bucket_url = dlt.secrets.get("destination.filesystem.bucket_url")

            # Inject source (arcgis) into path
            # Dataset name (jurisdiction) will be appended by dlt
            # Result: gs://${BUCKET_NAME}/bronze/arcgis/{jurisdiction}
            arcgis_bucket_url = f"{base_gcs_bucket_url.rstrip('/')}/arcgis"

            print(f"Bucket URL: {arcgis_bucket_url}/{jurisdiction}")

            pipeline = dlt.pipeline(
                pipeline_name=f"arcgis_{jurisdiction}",
                destination=dlt.destinations.filesystem(bucket_url=arcgis_bucket_url),
                dataset_name=jurisdiction,
            )

            # Full path for finalizer (includes jurisdiction)
            jurisdiction_data_path = f"{arcgis_bucket_url}/{jurisdiction}"

        # Initialize validator if not in dry-run/local mode
        validator = None
        if not dry_run and not local_output and not skip_validation:
            try:
                # Get GCS bucket name from dlt secrets
                base_gcs_bucket_url = dlt.secrets.get("destination.filesystem.bucket_url")
                # Extract bucket name from gs://bucket/path format
                bucket_name = base_gcs_bucket_url.replace("gs://", "").split("/")[0]

                # Create validator (uses gcloud auth automatically)
                validator = DataValidator(
                    gcs_bucket=bucket_name,
                    default_tolerance_percent=5.0,
                )
                print(f"Validation enabled (bucket: {bucket_name})")
                print("  Using gcloud application-default credentials for GCS access")
            except Exception as e:
                print(f"Warning: Could not initialize validator: {e}")
                print("Proceeding without validation")
        elif skip_validation:
            print("Validation skipped (--skip-validation flag)")
        else:
            print("Validation disabled (dry-run or local-output mode)")

        # Clear validation results for this jurisdiction
        global _validation_results
        _validation_results = []

        # Create metadata registry resource
        metadata_resource = create_metadata_resource(jurisdiction_datasets)

        # Create data resources for all datasets in this jurisdiction
        resources = [metadata_resource]  # Start with metadata
        for dataset_config in jurisdiction_datasets:
            resource = create_arcgis_resource(
                dataset_config=dataset_config,
                jurisdiction=jurisdiction,
                crs=jurisdiction_crs,
                max_records=max_records,
                validator=validator,
                skip_validation=skip_validation,
                current_year=current_year,
            )
            resources.append(resource)
            print(f"Configured resource: {dataset_config['name']}")

        # Add validation results resource if validation was enabled
        if validator is not None and not skip_validation:
            validation_resource = create_validation_resource()
            resources.append(validation_resource)

        # Run pipeline (loads both metadata and data)
        print(f"\nStarting extraction for {len(resources)} resource(s) (including metadata)...")
        print(f"Year partition: {current_year}")
        print("-" * 60)

        load_info = pipeline.run(resources, loader_file_format="parquet")

        # Print results
        print("-" * 60)
        print(f"Jurisdiction '{jurisdiction}' completed successfully!")
        print(f"Loaded {len(load_info.loads_ids)} load(s)")
        print(f"Pipeline name: {load_info.pipeline.pipeline_name}")

        # Post-processing: Convert to geoparquet
        if not dry_run and not skip_geoparquet:
            print("\n" + "=" * 60)
            print("Starting geoparquet conversion...")
            print("=" * 60)

            try:
                finalizer = GeoParquetFinalizer(jurisdiction_data_path, jurisdiction)
                finalizer.process_all_datasets(in_place=True)
            except Exception as e:
                print(f"\nWarning: Geoparquet conversion failed: {e}")
                print("Standard parquet files are still available")
                import traceback
                traceback.print_exc()

    if dry_run:
        print("\n" + "=" * 60)
        print("DRY-RUN: Data loaded to local DuckDB")
        print("To query data, use:")
        print("  import dlt")
        print("  pipeline = dlt.pipeline(pipeline_name='arcgis_madison_test', destination='duckdb')")
        print("  dataset = pipeline.dataset()")
        print("  print(dataset.parcels.df())  # Example for parcels")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract ArcGIS data to GeoParquet using dlt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config (production GCS)
  uv run pipelines/arcgis.py

  # Test with DuckDB (SQL queries)
  uv run pipelines/arcgis.py --dry-run

  # Test with local filesystem (actual parquet files, 1000 records)
  uv run pipelines/arcgis.py --local-output ./test_output --datasets parcels

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

    parser.add_argument(
        "--skip-geoparquet",
        action="store_true",
        help="Skip geoparquet conversion (write standard parquet only)",
    )

    parser.add_argument(
        "--local-output",
        type=Path,
        help="Write to local filesystem path for testing (auto-limits to 1000 records)",
    )

    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip data validation even if configured in YAML",
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
            skip_geoparquet=args.skip_geoparquet,
            local_output=args.local_output,
            skip_validation=args.skip_validation,
        )
        return 0
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
