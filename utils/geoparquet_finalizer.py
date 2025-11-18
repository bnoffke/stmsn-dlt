#!/usr/bin/env python3
"""
GeoParquet Finalizer for ArcGIS Pipeline

Converts standard parquet files to geoparquet format using metadata
extracted during pipeline execution.

Can be run:
1. Automatically after dlt pipeline completes (via pipeline hook)
2. Manually as a separate script
3. As a batch job for existing files

Usage:
    # Process latest run
    uv run pipelines/geoparquet_finalizer.py --jurisdiction madison

    # Process specific path
    uv run pipelines/geoparquet_finalizer.py --path gs://bucket/bronze/arcgis/madison

    # Dry run (local testing)
    uv run pipelines/geoparquet_finalizer.py --dry-run --jurisdiction madison
"""

import argparse
from pathlib import Path
from typing import Dict, List, Optional
import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkb
import dlt
import tempfile
import os


class GeoParquetFinalizer:
    """Convert standard parquet to geoparquet with proper spatial metadata."""

    def __init__(self, bucket_url: str, jurisdiction: str):
        self.bucket_url = bucket_url.rstrip("/")
        self.jurisdiction = jurisdiction

    @staticmethod
    def read_parquet_normalized(file_path: str) -> pd.DataFrame:
        """
        Read parquet file/directory with dictionary-encoded columns normalized to base types.

        This prevents PyArrow schema merge errors when reading partitioned datasets where
        different files have inconsistent dictionary encoding (e.g., int64 vs dictionary<int32>).

        When given a directory path (common for GCS partitioned datasets), this method reads
        each parquet file individually, normalizes its schema, then concatenates them. This
        bypasses PyArrow's automatic schema merging which fails on dictionary type mismatches.

        Args:
            file_path: Path to parquet file or directory (local or GCS path)

        Returns:
            DataFrame with all dictionary types cast to their base types
        """
        from pyarrow import fs

        # Determine filesystem type and normalize path
        if file_path.startswith("gs://"):
            filesystem = fs.GcsFileSystem()
            path = file_path[5:]  # Remove gs:// prefix
        else:
            filesystem = fs.LocalFileSystem()
            path = file_path

        # Check if path is a file or directory
        file_info = filesystem.get_file_info(path)

        if file_info.type == fs.FileType.Directory:
            # Directory: read all parquet files individually to avoid schema merge conflicts
            selector = fs.FileSelector(path, recursive=True)
            file_infos = filesystem.get_file_info(selector)

            # Find all parquet files
            parquet_files = [
                f"gs://{fi.path}" if isinstance(filesystem, fs.GcsFileSystem) else fi.path
                for fi in file_infos
                if fi.path.endswith(".parquet") and fi.type == fs.FileType.File
            ]

            if not parquet_files:
                raise ValueError(f"No parquet files found in {file_path}")

            # Read each file individually and normalize its schema
            normalized_tables = []
            for pq_file in parquet_files:
                # Read single file bypassing Hive partition inference
                # (use ParquetFile API to avoid year column conflicts)
                table = pq.ParquetFile(pq_file).read()

                # Normalize dictionary encoding in this table
                normalized_fields = []
                for field in table.schema:
                    if pa.types.is_dictionary(field.type):
                        # Cast dictionary type to its base value type
                        normalized_fields.append(
                            pa.field(field.name, field.type.value_type, nullable=field.nullable)
                        )
                    else:
                        normalized_fields.append(field)

                # Cast to normalized schema
                normalized_schema = pa.schema(normalized_fields)
                normalized_table = table.cast(normalized_schema)
                normalized_tables.append(normalized_table)

            # Concatenate all normalized tables (now they have compatible schemas)
            combined_table = pa.concat_tables(normalized_tables)
            return combined_table.to_pandas()

        else:
            # Single file: read and normalize directly
            # (use ParquetFile API to bypass Hive partition inference)
            table = pq.ParquetFile(file_path).read()

            # Normalize dictionary encoding
            normalized_fields = []
            for field in table.schema:
                if pa.types.is_dictionary(field.type):
                    normalized_fields.append(
                        pa.field(field.name, field.type.value_type, nullable=field.nullable)
                    )
                else:
                    normalized_fields.append(field)

            normalized_schema = pa.schema(normalized_fields)
            return table.cast(normalized_schema).to_pandas()

    def read_metadata_registry(self) -> pd.DataFrame:
        """
        Read the spatial_metadata_registry table from parquet.

        Returns:
            DataFrame with metadata for all datasets
        """
        # Construct path to metadata registry
        # Example: gs://bucket/bronze/arcgis/madison/spatial_metadata_registry/year=2025/*.parquet
        metadata_path = f"{self.bucket_url}/spatial_metadata_registry"

        print(f"Reading metadata registry from: {metadata_path}")

        # Read all parquet files in metadata registry with dictionary normalization
        try:
            metadata_df = self.read_parquet_normalized(metadata_path)
            print(f"Found {len(metadata_df)} dataset(s) in metadata registry")
            return metadata_df
        except Exception as e:
            print(f"Error reading metadata registry: {e}")
            raise

    def find_dataset_files(self, dataset_name: str) -> List[str]:
        """
        Find all parquet files for a dataset.

        Args:
            dataset_name: Name of the dataset (table name)

        Returns:
            List of file paths
        """
        # Example path: gs://bucket/bronze/arcgis/madison/parcels/year=2025/*.parquet
        dataset_path = f"{self.bucket_url}/{dataset_name}"

        print(f"  Searching for files in: {dataset_path}")

        # Use pyarrow filesystem to list files
        from pyarrow import fs

        # Parse GCS path
        if dataset_path.startswith("gs://"):
            filesystem = fs.GcsFileSystem()
            path = dataset_path[5:]  # Remove gs://
        else:
            filesystem = fs.LocalFileSystem()
            path = dataset_path

        try:
            # Find all parquet files recursively
            selector = fs.FileSelector(path, recursive=True)
            file_infos = filesystem.get_file_info(selector)

            files = [
                (
                    f"gs://{file_info.path}"
                    if isinstance(filesystem, fs.GcsFileSystem)
                    else file_info.path
                )
                for file_info in file_infos
                if file_info.path.endswith(".parquet") and file_info.type == fs.FileType.File
            ]

            return files
        except Exception as e:
            print(f"  Warning: Error listing files: {e}")
            return []

    def convert_to_geoparquet(
        self,
        input_file: str,
        output_file: str,
        metadata: Dict,
    ) -> None:
        """
        Convert a standard parquet file to geoparquet.

        Args:
            input_file: Path to input parquet file
            output_file: Path to output geoparquet file
            metadata: Spatial metadata dict from registry
        """
        # Read parquet file with dictionary normalization
        df = self.read_parquet_normalized(input_file)

        # Check if geometry column exists
        if "geometry" not in df.columns:
            print(f"    Warning: No geometry column found in {input_file}, skipping")
            return

        # Convert WKB hex to shapely geometries
        print(f"    Converting WKB to geometries...")
        df["geometry"] = df["geometry"].apply(
            lambda x: wkb.loads(bytes.fromhex(x)) if x and isinstance(x, str) else None
        )

        # Determine CRS
        crs = None
        if metadata.get("crs_epsg"):
            crs = f"EPSG:{metadata['crs_epsg']}"
        elif metadata.get("crs_wkid"):
            crs = f"EPSG:{metadata['crs_wkid']}"
        elif metadata.get("crs_wkt"):
            crs = metadata["crs_wkt"]

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=crs)

        # Write as geoparquet
        print(f"    Writing geoparquet...")
        gdf.to_parquet(output_file, compression="snappy", index=False)

    def process_dataset(
        self, dataset_name: str, metadata: Dict, in_place: bool = True
    ) -> None:
        """
        Process all files for a dataset.

        Args:
            dataset_name: Name of the dataset
            metadata: Spatial metadata for the dataset
            in_place: If True, replace original files; if False, write to new location
        """
        print(f"\n{'=' * 60}")
        print(f"Processing dataset: {dataset_name}")
        print(f"  CRS: EPSG:{metadata.get('crs_epsg')} (EPSG)")
        print(f"  Geometry type: {metadata.get('geometry_type')}")
        print(f"  Extent: [{metadata.get('xmin')}, {metadata.get('ymin')}, {metadata.get('xmax')}, {metadata.get('ymax')}]")

        # Find all parquet files
        files = self.find_dataset_files(dataset_name)
        print(f"  Found {len(files)} file(s)")

        if not files:
            print(f"  No files found for {dataset_name}, skipping")
            return

        for file_path in files:
            print(f"  Processing: {Path(file_path).name}")
            try:
                if in_place:
                    # Create temp file, then replace original
                    with tempfile.NamedTemporaryFile(
                        suffix=".parquet", delete=False
                    ) as tmp:
                        temp_path = tmp.name

                    self.convert_to_geoparquet(file_path, temp_path, metadata)

                    # Upload/move back to original location
                    from pyarrow import fs

                    if file_path.startswith("gs://"):
                        # GCS: upload temp file
                        filesystem = fs.GcsFileSystem()
                        with open(temp_path, "rb") as f:
                            with filesystem.open_output_stream(file_path[5:]) as out:
                                out.write(f.read())
                        # Clean up temp file
                        os.unlink(temp_path)
                    else:
                        # Local: replace file (moves temp file, no cleanup needed)
                        Path(temp_path).replace(Path(file_path))

                else:
                    # Write to separate geoparquet directory
                    output_path = file_path.replace("/arcgis/", "/arcgis_geoparquet/")
                    self.convert_to_geoparquet(file_path, output_path, metadata)

                print(f"    ✓ Converted to geoparquet")

            except Exception as e:
                print(f"    ✗ Error processing {file_path}: {e}")
                import traceback

                traceback.print_exc()
                continue

    def process_all_datasets(self, in_place: bool = True) -> None:
        """
        Process all datasets using metadata registry.

        Args:
            in_place: If True, replace original files; if False, write to new location
        """
        print(f"\n{'=' * 60}")
        print(f"GeoParquet Finalizer - Jurisdiction: {self.jurisdiction}")
        print(f"Bucket URL: {self.bucket_url}")
        print(f"Mode: {'In-place replacement' if in_place else 'Copy to new location'}")
        print(f"{'=' * 60}")

        # Read metadata registry
        metadata_df = self.read_metadata_registry()

        # Process each dataset
        for _, row in metadata_df.iterrows():
            dataset_name = row["dataset_name"]
            metadata = row.to_dict()

            self.process_dataset(dataset_name, metadata, in_place=in_place)

        print(f"\n{'=' * 60}")
        print("GeoParquet finalization complete!")
        print(f"{'=' * 60}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Convert parquet to geoparquet using extracted metadata"
    )

    parser.add_argument(
        "--jurisdiction", help="Jurisdiction to process (e.g., madison)"
    )

    parser.add_argument(
        "--path", help="Explicit bucket path (overrides jurisdiction)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process local DuckDB output instead of GCS",
    )

    parser.add_argument(
        "--no-in-place",
        action="store_true",
        help="Write to new location instead of replacing original files",
    )

    parser.add_argument(
        "--dataset", help="Process only specified dataset (default: all)"
    )

    args = parser.parse_args()

    # Determine bucket URL
    if args.dry_run:
        # For local testing, read from DuckDB pipeline output
        if not args.jurisdiction:
            print("Error: --jurisdiction required for dry-run mode")
            return 1

        # Get the dlt pipeline data directory
        pipeline_name = f"arcgis_{args.jurisdiction}_test"
        pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

        # For DuckDB, we need to export tables to parquet first
        print(
            "Dry-run mode: This will process parquet files in the DuckDB pipeline directory"
        )
        print(f"Pipeline: {pipeline_name}")

        # Use the pipeline's dataset storage location
        dataset_name = f"arcgis_{args.jurisdiction}"
        storage_path = Path.home() / ".dlt" / "pipelines" / pipeline_name / dataset_name

        if not storage_path.exists():
            print(f"Error: DuckDB pipeline data not found at {storage_path}")
            print(f"Have you run the pipeline with --dry-run first?")
            return 1

        bucket_url = str(storage_path)
        jurisdiction = args.jurisdiction

    elif args.path:
        bucket_url = args.path
        jurisdiction = Path(args.path).name
    elif args.jurisdiction:
        # Get base bucket URL from dlt config
        try:
            base_bucket_url = dlt.secrets.get("destination.filesystem.bucket_url")
            bucket_url = f"{base_bucket_url.rstrip('/')}/arcgis/{args.jurisdiction}"
            jurisdiction = args.jurisdiction
        except Exception as e:
            print(f"Error reading dlt secrets: {e}")
            print("Make sure .dlt/secrets.toml is configured with destination.filesystem.bucket_url")
            return 1
    else:
        print("Error: Must specify --jurisdiction, --path, or --dry-run")
        return 1

    # Create finalizer
    finalizer = GeoParquetFinalizer(bucket_url, jurisdiction)

    # Process datasets
    try:
        if args.dataset:
            # Process single dataset
            metadata_df = finalizer.read_metadata_registry()
            dataset_rows = metadata_df[metadata_df["dataset_name"] == args.dataset]

            if dataset_rows.empty:
                print(f"Error: Dataset '{args.dataset}' not found in metadata registry")
                print(f"Available datasets: {', '.join(metadata_df['dataset_name'].tolist())}")
                return 1

            metadata = dataset_rows.iloc[0].to_dict()
            finalizer.process_dataset(
                args.dataset, metadata, in_place=not args.no_in_place
            )
        else:
            # Process all datasets
            finalizer.process_all_datasets(in_place=not args.no_in_place)

        return 0

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
