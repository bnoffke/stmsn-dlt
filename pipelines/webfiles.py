#!/usr/bin/env python3
"""
Web Files Pipeline - Download Excel/CSV files from web URLs to GCS.

Extracts data from web-hosted files and stores as hive-partitioned
Parquet files in Google Cloud Storage.

Usage:
    # Run with default config (GCS destination)
    uv run pipelines/webfiles.py

    # Test locally with DuckDB
    uv run pipelines/webfiles.py --dry-run

    # Force reload (ignore load history)
    uv run pipelines/webfiles.py --force

    # Download specific URL
    uv run pipelines/webfiles.py --url "https://example.com/file.xlsx" --dataset tax_roll
"""

import argparse
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import dlt
import yaml

from clients.webfile import WebFileClient, DiscoveredFile
from utils.file_parsers import read_file, FileParseError
from utils.load_history import LoadHistoryManager, LoadHistoryRecord


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load dataset configuration from YAML file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def create_load_history_resource(history_manager: LoadHistoryManager):
    """
    Create a dlt resource for load history tracking.

    Args:
        history_manager: LoadHistoryManager with pending records

    Returns:
        dlt resource for load history
    """

    @dlt.resource(
        name="load_history",
        write_disposition="append",  # Keep historical records
    )
    def extract_load_history() -> Iterator[Dict[str, Any]]:
        """Yield pending load history records."""
        for record in history_manager.get_pending_records():
            yield record.to_dict()

    return extract_load_history


def create_data_resource(
    dataset_name: str,
    file_path: Path,
    file_type: str,
    excel_config: Optional[Dict] = None,
    partition_values: Optional[Dict[str, str]] = None,
):
    """
    Create a dlt resource for a data file.

    Args:
        dataset_name: Name for the resource/table
        file_path: Local path to the downloaded file
        file_type: File type (xlsx, csv, etc.)
        excel_config: Optional Excel-specific configuration
        partition_values: Partition values to add to each record

    Returns:
        Tuple of (dlt resource, row count)
    """
    # Parse file
    excel_kwargs = {}
    if excel_config:
        excel_kwargs["sheet"] = excel_config.get("sheet", 0)
        excel_kwargs["header_detection"] = excel_config.get("header_detection", "auto")

    df, row_count = read_file(file_path, file_type, **excel_kwargs)

    # Add partition columns to data
    if partition_values:
        for field, value in partition_values.items():
            df[field] = value

    # Convert to records
    records = df.to_dict(orient="records")

    @dlt.resource(
        name=dataset_name,
        write_disposition="append",
    )
    def extract_data() -> Iterator[Dict[str, Any]]:
        """Yield data records."""
        for record in records:
            yield record

    return extract_data, row_count


def build_layout(partition_config: List[Dict]) -> str:
    """
    Build dlt layout string from partition configuration.

    Args:
        partition_config: List of partition field configs

    Returns:
        Layout string like "{table_name}/year={year}/month={month}/{file_id}.{ext}"
    """
    parts = ["{table_name}"]

    for config in partition_config:
        field = config["field"]
        # Use lowercase field name in path
        parts.append(f"{field}={{{field}}}")

    parts.append("{file_id}.{ext}")

    return "/".join(parts)


def run_pipeline(
    config_path: Path,
    dry_run: bool = False,
    force: bool = False,
    explicit_url: Optional[str] = None,
    dataset_filter: Optional[List[str]] = None,
    local_output: Optional[Path] = None,
) -> None:
    """
    Run the web files extraction pipeline.

    Args:
        config_path: Path to dataset configuration YAML
        dry_run: If True, use DuckDB destination for local testing
        force: If True, ignore load history and reprocess files
        explicit_url: If provided, download this specific URL
        dataset_filter: Optional list of dataset names to extract (None = all)
        local_output: If provided, write to local filesystem path
    """
    # Load configuration
    config = load_config(config_path)
    jurisdictions = config.get("jurisdictions", {})

    if not jurisdictions:
        print("No jurisdictions configured in YAML file")
        return

    # Flatten datasets with jurisdiction info for filtering
    all_datasets = []
    for jurisdiction_name, jurisdiction_config in jurisdictions.items():
        for dataset in jurisdiction_config.get("datasets", []):
            dataset_with_context = {
                **dataset,
                "jurisdiction": jurisdiction_name,
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
            datasets_by_jurisdiction[jurisdiction] = []
        datasets_by_jurisdiction[jurisdiction].append(dataset)

    print(f"Found {len(datasets_by_jurisdiction)} jurisdiction(s): {', '.join(datasets_by_jurisdiction.keys())}")

    # Initialize web client
    web_client = WebFileClient()

    # Process each jurisdiction
    for jurisdiction, jurisdiction_datasets in datasets_by_jurisdiction.items():
        print(f"\n{'=' * 60}")
        print(f"Processing jurisdiction: {jurisdiction}")
        print(f"{'=' * 60}")

        # Get GCS bucket for history manager
        gcs_bucket = None
        history_manager = None

        if not dry_run and not local_output:
            base_gcs_bucket_url = dlt.secrets.get("destination.filesystem.bucket_url")
            gcs_bucket = base_gcs_bucket_url.replace("gs://", "").split("/")[0]

            # Initialize history manager
            history_manager = LoadHistoryManager(gcs_bucket, "webfiles")

            if not force:
                print("\nLoading existing history...")
                history_manager.load_existing_history(
                    jurisdiction=jurisdiction,
                    dataset_name=dataset_filter[0] if dataset_filter and len(dataset_filter) == 1 else None,
                )

        # Collect resources to run
        # First, collect all files from all datasets with their config
        all_files_with_config = []

        for dataset_config in jurisdiction_datasets:
            dataset_name = dataset_config["name"]
            base_url = dataset_config["base_url"]
            file_type = dataset_config["file_type"]
            url_patterns = dataset_config.get("url_patterns", [])
            partition_config = dataset_config.get("partition", [])
            excel_config = dataset_config.get("excel", {})

            print(f"\n--- Dataset: {dataset_name} ---")

            # Discover or use explicit URL
            if explicit_url:
                # Extract filename and partition values from explicit URL
                filename = explicit_url.split("/")[-1]
                partition_values = {}
                for config in partition_config:
                    import re
                    match = re.search(config["pattern"], filename)
                    if match:
                        partition_values[config["field"]] = match.group(0)

                files_to_process = [
                    DiscoveredFile(
                        url=explicit_url,
                        filename=filename,
                        partition_values=partition_values,
                    )
                ]
                print(f"Using explicit URL: {explicit_url}")
            else:
                # Discover available files
                print(f"Discovering files from {base_url}...")
                discovered = web_client.discover_files(
                    base_url=base_url,
                    url_patterns=url_patterns,
                    partition_config=partition_config,
                )
                print(f"Discovered {len(discovered)} file(s)")

                # Filter out already-loaded files
                if history_manager and not force:
                    files_to_process = [
                        f for f in discovered
                        if not history_manager.is_already_loaded(f.url)
                    ]
                    skipped = len(discovered) - len(files_to_process)
                    if skipped > 0:
                        print(f"Skipping {skipped} file(s) already in load history")
                else:
                    files_to_process = discovered

            if not files_to_process:
                print("No new files to process")
                continue

            print(f"Found {len(files_to_process)} file(s) to process")

            # Add files with their config to the collection
            for discovered_file in files_to_process:
                all_files_with_config.append((discovered_file, dataset_config))

        if not all_files_with_config:
            print("\nNo files to load for this jurisdiction")
            continue

        # Group files by partition values
        files_by_partition = defaultdict(list)
        for discovered_file, dataset_config in all_files_with_config:
            # Create hashable key from partition values
            partition_key = tuple(sorted(discovered_file.partition_values.items()))
            files_by_partition[partition_key].append((discovered_file, dataset_config))

        print(f"\n--- Found {len(files_by_partition)} unique partition combination(s) ---")

        # Process each partition group separately
        for partition_key, partition_files in files_by_partition.items():
            partition_values = dict(partition_key)
            print(f"\n--- Processing partition: {partition_values} ---")
            print(f"Files in this partition: {len(partition_files)}")

            resources = []
            total_files = 0

            # Process each file in this partition group
            for discovered_file, dataset_config in partition_files:
                dataset_name = dataset_config["name"]
                file_type = dataset_config["file_type"]
                excel_config = dataset_config.get("excel", {})

                print(f"\nFile: {discovered_file.filename}")
                print(f"Dataset: {dataset_name}")
                print(f"Partitions: {discovered_file.partition_values}")

                try:
                    # Download file
                    temp_dir = Path(tempfile.gettempdir())
                    local_path, file_hash = web_client.download_file(
                        discovered_file.url, temp_dir
                    )

                    # Create resource for this file
                    resource, row_count = create_data_resource(
                        dataset_name=dataset_name,
                        file_path=local_path,
                        file_type=file_type,
                        excel_config=excel_config,
                        partition_values=discovered_file.partition_values,
                    )

                    print(f"Parsed {row_count:,} rows")

                    resources.append(resource)
                    total_files += 1

                    # Record in history
                    if history_manager:
                        history_manager.record_load(
                            dataset_name=dataset_name,
                            jurisdiction=jurisdiction,
                            source_url=discovered_file.url,
                            filename=discovered_file.filename,
                            partition_values=discovered_file.partition_values,
                            row_count=row_count,
                            file_hash=file_hash,
                        )

                    # Clean up temp file
                    try:
                        os.remove(local_path)
                    except Exception:
                        pass

                except FileParseError as e:
                    print(f"ERROR parsing file: {e}")
                    continue
                except Exception as e:
                    print(f"ERROR processing file: {e}")
                    continue

            if not resources:
                print("No files successfully processed for this partition")
                continue

            # Add load history resource if we have a history manager
            if history_manager and history_manager.get_pending_records():
                resources.append(create_load_history_resource(history_manager))

            # Create and run dlt pipeline for this partition group
            print(f"\n--- Loading {total_files} file(s) to destination ---")

            # Build layout from first dataset's partition config
            # (assumes all datasets in jurisdiction have same partition structure)
            first_dataset = jurisdiction_datasets[0]
            partition_config = first_dataset.get("partition", [{"field": "year"}])
            layout = build_layout(partition_config)
            print(f"Layout: {layout}")
            print(f"Partition placeholders: {partition_values}")

            if dry_run:
                print("Running in DRY-RUN mode with DuckDB destination")
                pipeline = dlt.pipeline(
                    pipeline_name=f"webfiles_{jurisdiction}_test",
                    destination="duckdb",
                    dataset_name=jurisdiction,
                )
            elif local_output:
                print("Running with LOCAL FILESYSTEM destination")
                base_bucket_url = f"{local_output.absolute()}/webfiles"
                print(f"Local output path: {base_bucket_url}/{jurisdiction}")

                pipeline = dlt.pipeline(
                    pipeline_name=f"webfiles_{jurisdiction}_local",
                    destination=dlt.destinations.filesystem(
                        bucket_url=base_bucket_url,
                        layout=layout,
                        extra_placeholders=partition_values,
                    ),
                    dataset_name=jurisdiction,
                )
            else:
                print("Running with GCS filesystem destination")
                base_gcs_bucket_url = dlt.secrets.get("destination.filesystem.bucket_url")
                webfiles_bucket_url = f"{base_gcs_bucket_url.rstrip('/')}/webfiles"
                print(f"Bucket URL: {webfiles_bucket_url}/{jurisdiction}")

                pipeline = dlt.pipeline(
                    pipeline_name=f"webfiles_{jurisdiction}",
                    destination=dlt.destinations.filesystem(
                        bucket_url=webfiles_bucket_url,
                        layout=layout,
                        extra_placeholders=partition_values,
                    ),
                    dataset_name=jurisdiction,
                )

            # Run pipeline
            try:
                load_info = pipeline.run(resources, loader_file_format="parquet")
                print(f"\nLoad completed successfully!")
                print(load_info)

                # Clear pending history after successful load
                if history_manager:
                    history_manager.clear_pending()

            except Exception as e:
                print(f"Pipeline load failed: {e}")
                raise

    # Clean up
    web_client.close()

    if dry_run:
        print("\n" + "=" * 60)
        print("DRY-RUN: Data loaded to local DuckDB")
        print("To query data, use:")
        print("  import dlt")
        print("  pipeline = dlt.pipeline(pipeline_name='webfiles_madison_test', destination='duckdb')")
        print("  dataset = pipeline.dataset()")
        print("  print(dataset.tax_roll.df())")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download web-hosted Excel/CSV files to GCS using dlt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default config (production GCS)
  uv run pipelines/webfiles.py

  # Test with DuckDB
  uv run pipelines/webfiles.py --dry-run

  # Force reload (ignore history)
  uv run pipelines/webfiles.py --force

  # Download specific URL
  uv run pipelines/webfiles.py --url "https://example.com/2024taxroll.xlsx" --datasets tax_roll

  # Test with local filesystem
  uv run pipelines/webfiles.py --local-output ./test_output --datasets tax_roll
        """,
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/webfiles_datasets.yaml"),
        help="Path to dataset configuration YAML (default: config/webfiles_datasets.yaml)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test mode: use DuckDB destination instead of GCS",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reload: ignore load history and reprocess all files",
    )

    parser.add_argument(
        "--url",
        type=str,
        help="Download a specific URL (bypasses discovery)",
    )

    parser.add_argument(
        "--datasets",
        nargs="+",
        help="Extract only specified datasets (default: all)",
    )

    parser.add_argument(
        "--local-output",
        type=Path,
        help="Write to local filesystem path for testing",
    )

    args = parser.parse_args()

    # Validate config file exists
    if not args.config.exists():
        print(f"Error: Config file not found: {args.config}")
        return 1

    # Require --datasets when using --url
    if args.url and not args.datasets:
        print("Error: --datasets is required when using --url")
        return 1

    try:
        run_pipeline(
            config_path=args.config,
            dry_run=args.dry_run,
            force=args.force,
            explicit_url=args.url,
            dataset_filter=args.datasets,
            local_output=args.local_output,
        )
        return 0
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
