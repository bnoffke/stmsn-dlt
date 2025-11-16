#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "google-cloud-storage>=2.10.0"
# ]
# ///
"""
Upload parquet files from local directory structure to GCS bucket.
Supports optional Hive partitioning for year-based directories.
Can reorganize files with YYYYMM date patterns in filenames into proper partitions.
"""

import os
import sys
import re
from pathlib import Path
from google.cloud import storage
from typing import List, Tuple, Optional


def get_gcs_config():
    """Get GCS configuration from environment variables."""
    required_vars = ['BUCKET_NAME', 'PROJECT_ID']
    config = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            raise ValueError(f"Missing required environment variable: {var}")
        config[var] = value
    
    # Region is optional
    config['REGION'] = os.getenv('REGION', 'us-central1')
    
    return config


def should_use_hive_partitioning(directory_name: str) -> bool:
    """
    Determine if a directory appears to be a year partition.
    Returns True if the directory name is a 4-digit year.
    """
    return directory_name.isdigit() and len(directory_name) == 4


def extract_date_from_filename(filename: str) -> Optional[Tuple[str, str]]:
    """
    Extract year and month from filename patterns like '202001CSV.parquet'.
    
    Args:
        filename: The filename to parse
    
    Returns:
        Tuple of (year, month) if pattern matches, None otherwise
    """
    # Match YYYYMM pattern at start of filename
    match = re.match(r'^(\d{4})(\d{2})', filename)
    if match:
        year = match.group(1)
        month = match.group(2)
        # Validate month is 01-12
        if 1 <= int(month) <= 12:
            return (year, month)
    return None


def build_gcs_path(local_path: Path, base_local_dir: Path, 
                   parent_prefix: str, use_hive: bool = False,
                   reorganize_dates: bool = False) -> str:
    """
    Build the GCS destination path from the local path.
    
    Args:
        local_path: Full local file path
        base_local_dir: Base directory being uploaded
        parent_prefix: Parent directory prefix in GCS (e.g., "legacy")
        use_hive: Whether to convert year directories to Hive partitioning format
        reorganize_dates: Whether to extract dates from filenames and create partitions
    
    Returns:
        GCS destination path
    """
    # Get relative path from base directory
    relative_path = local_path.relative_to(base_local_dir)
    parts = list(relative_path.parts)
    
    # Track if we reorganized based on filename date
    reorganized = False
    
    # Check if we should reorganize based on filename date pattern
    if reorganize_dates and len(parts) >= 1:
        filename = parts[-1]
        date_parts = extract_date_from_filename(filename)
        
        if date_parts:
            year, month = date_parts
            # Remove the date prefix from filename (e.g., '202001CSV.parquet' -> 'CSV.parquet')
            clean_filename = re.sub(r'^\d{6}', '', filename)
            if not clean_filename or clean_filename.startswith('.'):
                # If nothing left or starts with extension, keep original name
                clean_filename = filename
            
            # Build new path with year/month partitions
            if use_hive:
                new_parts = parts[:-1] + [f'year={year}', f'month={month}', clean_filename]
            else:
                new_parts = parts[:-1] + [year, month, clean_filename]
            parts = new_parts
            reorganized = True
    
    # Apply Hive partitioning to year directories (if we didn't already reorganize)
    if use_hive and not reorganized and len(parts) >= 2:
        # Check if any part looks like a year directory
        new_parts = []
        for part in parts[:-1]:  # Don't modify the filename
            if should_use_hive_partitioning(part):
                new_parts.append(f"year={part}")
            else:
                new_parts.append(part)
        new_parts.append(parts[-1])  # Add filename
        parts = new_parts
    
    # Combine with parent prefix
    gcs_path = os.path.join(parent_prefix, *parts)
    return gcs_path


def find_parquet_files(base_dir: Path) -> List[Path]:
    """
    Recursively find all .parquet files in the directory structure.
    
    Args:
        base_dir: Base directory to search
    
    Returns:
        List of Path objects for all parquet files found
    """
    parquet_files = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(Path(root) / file)
    return parquet_files


def upload_files_to_gcs(local_base_dir: str, bucket_name: str, 
                       parent_prefix: str = "legacy", 
                       use_hive_partitioning: bool = False,
                       reorganize_dates: bool = False,
                       dry_run: bool = False,
                       project_id: str = None):
    """
    Upload parquet files from local directory to GCS bucket.
    
    Args:
        local_base_dir: Local directory containing the data
        bucket_name: GCS bucket name
        parent_prefix: Parent directory in GCS (default: "legacy")
        use_hive_partitioning: Convert year directories to year=YYYY format
        reorganize_dates: Extract dates from filenames and create year/month partitions
        dry_run: If True, only print what would be uploaded
        project_id: GCS project ID
    """
    base_path = Path(local_base_dir)
    
    if not base_path.exists():
        raise ValueError(f"Local directory does not exist: {local_base_dir}")
    
    # Find all parquet files
    print(f"Scanning for parquet files in {local_base_dir}...")
    parquet_files = find_parquet_files(base_path)
    
    if not parquet_files:
        print("No parquet files found!")
        return
    
    print(f"Found {len(parquet_files)} parquet file(s)")
    
    if reorganize_dates:
        print("Extracting dates from filenames to create partitions (YYYYMM -> year/month/)")
    if use_hive_partitioning:
        print("Using Hive partitioning format (year=YYYY, month=MM)")
    
    # Initialize GCS client
    if not dry_run:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
    
    # Upload files
    upload_count = 0
    error_count = 0
    
    for local_file in parquet_files:
        try:
            gcs_path = build_gcs_path(
                local_file, 
                base_path, 
                parent_prefix, 
                use_hive_partitioning,
                reorganize_dates
            )
            
            if dry_run:
                print(f"[DRY RUN] Would upload:")
                print(f"  Local:  {local_file}")
                print(f"  GCS:    gs://{bucket_name}/{gcs_path}")
            else:
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(str(local_file))
                print(f"✓ Uploaded: {gcs_path}")
                upload_count += 1
                
        except Exception as e:
            print(f"✗ Error uploading {local_file}: {e}")
            error_count += 1
    
    # Summary
    print("\n" + "="*60)
    if dry_run:
        print(f"DRY RUN COMPLETE: Would upload {len(parquet_files)} file(s)")
    else:
        print(f"UPLOAD COMPLETE")
        print(f"  Successful: {upload_count}")
        print(f"  Errors:     {error_count}")
        print(f"  Total:      {len(parquet_files)}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Upload parquet files to GCS with optional Hive partitioning"
    )
    parser.add_argument(
        "local_dir",
        help="Local directory containing the data to upload"
    )
    parser.add_argument(
        "--hive-partitioning",
        action="store_true",
        help="Use Hive partitioning format (year=YYYY) for year directories"
    )
    parser.add_argument(
        "--reorganize-dates",
        action="store_true",
        help="Extract YYYYMM dates from filenames and create year/month partitions"
    )
    parser.add_argument(
        "--parent-prefix",
        default="legacy",
        help="Parent directory prefix in GCS (default: legacy)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be uploaded without actually uploading"
    )
    
    args = parser.parse_args()
    
    try:
        # Get configuration from environment
        config = get_gcs_config()
        
        print("="*60)
        print("GCS Upload Configuration")
        print("="*60)
        print(f"Project:          {config['PROJECT_ID']}")
        print(f"Bucket:           {config['BUCKET_NAME']}")
        print(f"Region:           {config['REGION']}")
        print(f"Local Directory:  {args.local_dir}")
        print(f"GCS Prefix:       {args.parent_prefix}/")
        print(f"Hive Partitioning: {args.hive_partitioning}")
        print(f"Reorganize Dates:  {args.reorganize_dates}")
        print(f"Dry Run:          {args.dry_run}")
        print("="*60)
        print()
        
        # Upload files
        upload_files_to_gcs(
            local_base_dir=args.local_dir,
            bucket_name=config['BUCKET_NAME'],
            parent_prefix=args.parent_prefix,
            use_hive_partitioning=args.hive_partitioning,
            reorganize_dates=args.reorganize_dates,
            dry_run=args.dry_run,
            project_id=config['PROJECT_ID']
        )
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()