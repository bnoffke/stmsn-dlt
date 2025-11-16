#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "geopandas",
#     "shapely",
# ]
# ///
"""
Convert CSV, TSV, GeoJSON, and Shapefile files to Parquet/GeoParquet format.

Usage:
    uv run convert_to_parquet.py <directory> [--force] [--verbose]
    
Options:
    --force     Overwrite existing parquet files
    --verbose   Show detailed progress information
"""

import sys
import warnings
import re
from pathlib import Path
import pandas as pd
import geopandas as gpd


def normalize_filename(file_path: Path) -> Path:
    """Normalize filename to lowercase with underscores instead of spaces."""
    stem = file_path.stem.lower().replace(' ', '_')
    return file_path.parent / f"{stem}.parquet"


def extract_problem_column(error_msg: str) -> str | None:
    """Extract the column name from a parquet conversion error message."""
    # Pattern: "Conversion failed for column XYZ with type object"
    match = re.search(r"Conversion failed for column (\w+)", error_msg)
    if match:
        return match.group(1)
    return None


def convert_csv_to_parquet(file_path: Path, verbose: bool = False) -> None:
    """Convert CSV file to Parquet, handling mixed types and encoding issues."""
    output_path = normalize_filename(file_path)
    print(f"Converting {file_path.name}")
    
    encodings = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            if verbose:
                print(f"  Trying encoding={encoding}")
            
            # Attempt 1: Normal type inference
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
            
            df.to_parquet(output_path, engine='pyarrow', index=False)
            if verbose:
                print(f"  ✓ Success with inferred types")
            return
            
        except UnicodeDecodeError:
            if verbose:
                print(f"  × Encoding {encoding} failed, trying next...")
            continue
            
        except Exception as e:
            error_msg = str(e)
            problem_column = extract_problem_column(error_msg)
            
            if problem_column:
                # Attempt 2: Convert only the problematic column to string
                if verbose:
                    print(f"  ! Column '{problem_column}' has mixed types, converting to string")
                
                try:
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                        df = pd.read_csv(
                            file_path, 
                            encoding=encoding, 
                            dtype={problem_column: str},
                            low_memory=False
                        )
                    
                    # Try to write - might fail on another column
                    df.to_parquet(output_path, engine='pyarrow', index=False)
                    if verbose:
                        print(f"  ✓ Success ('{problem_column}' as string)")
                    return
                    
                except Exception as e2:
                    # Multiple problematic columns - need broader approach
                    error_msg2 = str(e2)
                    problem_column2 = extract_problem_column(error_msg2)
                    
                    if problem_column2 and problem_column2 != problem_column:
                        # Attempt 3: Convert both columns
                        if verbose:
                            print(f"  ! Multiple problem columns detected")
                        
                        try:
                            with warnings.catch_warnings():
                                warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                                df = pd.read_csv(
                                    file_path,
                                    encoding=encoding,
                                    dtype={problem_column: str, problem_column2: str},
                                    low_memory=False
                                )
                            
                            df.to_parquet(output_path, engine='pyarrow', index=False)
                            if verbose:
                                print(f"  ✓ Success (multiple columns as strings)")
                            return
                        except:
                            pass
            
            # Attempt 4: Last resort - all columns as strings
            if verbose:
                print(f"  ! Multiple issues detected, converting all to strings")
            
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                    df = pd.read_csv(file_path, encoding=encoding, dtype=str, low_memory=False)
                
                df.to_parquet(output_path, engine='pyarrow', index=False)
                if verbose:
                    print(f"  ✓ Success (all columns as strings)")
                return
                
            except UnicodeDecodeError:
                continue
    
    raise Exception(f"Failed with all encoding attempts: {encodings}")


def convert_tsv_to_parquet(file_path: Path, verbose: bool = False) -> None:
    """Convert TSV file to Parquet, handling mixed types and encoding issues."""
    output_path = normalize_filename(file_path)
    print(f"Converting {file_path.name}")
    
    encodings = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            if verbose:
                print(f"  Trying encoding={encoding}")
            
            # Attempt 1: Normal type inference
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                df = pd.read_csv(file_path, sep='\t', encoding=encoding, low_memory=False)
            
            df.to_parquet(output_path, engine='pyarrow', index=False)
            if verbose:
                print(f"  ✓ Success with inferred types")
            return
            
        except UnicodeDecodeError:
            if verbose:
                print(f"  × Encoding {encoding} failed, trying next...")
            continue
            
        except Exception as e:
            error_msg = str(e)
            problem_column = extract_problem_column(error_msg)
            
            if problem_column:
                # Attempt 2: Convert only the problematic column to string
                if verbose:
                    print(f"  ! Column '{problem_column}' has mixed types, converting to string")
                
                try:
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                        df = pd.read_csv(
                            file_path,
                            sep='\t',
                            encoding=encoding,
                            dtype={problem_column: str},
                            low_memory=False
                        )
                    
                    df.to_parquet(output_path, engine='pyarrow', index=False)
                    if verbose:
                        print(f"  ✓ Success ('{problem_column}' as string)")
                    return
                    
                except Exception as e2:
                    error_msg2 = str(e2)
                    problem_column2 = extract_problem_column(error_msg2)
                    
                    if problem_column2 and problem_column2 != problem_column:
                        if verbose:
                            print(f"  ! Multiple problem columns detected")
                        
                        try:
                            with warnings.catch_warnings():
                                warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                                df = pd.read_csv(
                                    file_path,
                                    sep='\t',
                                    encoding=encoding,
                                    dtype={problem_column: str, problem_column2: str},
                                    low_memory=False
                                )
                            
                            df.to_parquet(output_path, engine='pyarrow', index=False)
                            if verbose:
                                print(f"  ✓ Success (multiple columns as strings)")
                            return
                        except:
                            pass
            
            # Attempt 4: Last resort - all columns as strings
            if verbose:
                print(f"  ! Multiple issues detected, converting all to strings")
            
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                    df = pd.read_csv(file_path, sep='\t', encoding=encoding, dtype=str, low_memory=False)
                
                df.to_parquet(output_path, engine='pyarrow', index=False)
                if verbose:
                    print(f"  ✓ Success (all columns as strings)")
                return
                
            except UnicodeDecodeError:
                continue
    
    raise Exception(f"Failed with all encoding attempts: {encodings}")


def convert_geojson_to_geoparquet(file_path: Path, verbose: bool = False) -> None:
    """Convert GeoJSON to GeoParquet."""
    output_path = normalize_filename(file_path)
    print(f"Converting {file_path.name}")
    
    gdf = gpd.read_file(file_path)
    gdf.to_parquet(output_path, index=False)
    
    if verbose:
        print(f"  ✓ Success")


def convert_shapefile_to_geoparquet(file_path: Path, verbose: bool = False) -> None:
    """Convert Shapefile to GeoParquet."""
    output_path = normalize_filename(file_path)
    print(f"Converting {file_path.name}")
    
    gdf = gpd.read_file(file_path)
    gdf.to_parquet(output_path, index=False)
    
    if verbose:
        print(f"  ✓ Success")


def process_directory(directory: Path, force: bool = False, verbose: bool = False) -> None:
    """Recursively process all supported files in directory."""
    converters = {
        '.csv': convert_csv_to_parquet,
        '.geojson': convert_geojson_to_geoparquet,
        '.shp': convert_shapefile_to_geoparquet,
        '.txt': convert_tsv_to_parquet,
    }
    
    files_converted = 0
    files_skipped = 0
    files_errored = 0
    error_details = []
    
    for ext, converter in converters.items():
        for file_path in directory.rglob(f"*{ext}"):
            output_path = normalize_filename(file_path)

            # Skip if parquet file already exists (unless --force)
            if output_path.exists() and not force:
                if verbose:
                    print(f"Skipping {file_path.name} (parquet exists)")
                files_skipped += 1
                continue
            
            try:
                converter(file_path, verbose=verbose)
                files_converted += 1
            except Exception as e:
                print(f"✗ ERROR: {file_path.name}: {e}")
                files_errored += 1
                error_details.append({
                    'file': str(file_path),
                    'error': str(e)
                })
    
    # Summary
    print("\n" + "=" * 60)
    print(f"✓ Converted: {files_converted}")
    print(f"⊘ Skipped:   {files_skipped}")
    print(f"✗ Errors:    {files_errored}")
    
    if error_details and verbose:
        print("\nError Details:")
        for err in error_details:
            print(f"  {err['file']}")
            print(f"    → {err['error']}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert CSV, TSV, GeoJSON, and Shapefile files to Parquet/GeoParquet"
    )
    parser.add_argument("directory", help="Directory to process recursively")
    parser.add_argument("--force", action="store_true", help="Overwrite existing parquet files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    
    args = parser.parse_args()
    directory = Path(args.directory)
    
    if not directory.exists():
        print(f"Error: Directory '{directory}' does not exist")
        sys.exit(1)
    
    if not directory.is_dir():
        print(f"Error: '{directory}' is not a directory")
        sys.exit(1)
    
    print(f"Processing: {directory.absolute()}")
    print(f"Force overwrite: {args.force}")
    print(f"Verbose: {args.verbose}")
    print("=" * 60)
    
    process_directory(directory, force=args.force, verbose=args.verbose)


if __name__ == "__main__":
    main()