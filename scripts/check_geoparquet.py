"""
Script to extract and display GeoParquet metadata.

Usage:
    python check_geoparquet_metadata.py <path_to_geoparquet_file>
"""

import sys
import json
import pyarrow.parquet as pq


def check_geoparquet_metadata(file_path: str):
    """Extract and display all metadata from a GeoParquet file."""
    
    print(f"Analyzing: {file_path}\n")
    print("=" * 80)
    
    # Read the parquet file
    parquet_file = pq.ParquetFile(file_path)
    
    # Basic file stats
    print("\n📊 FILE STATISTICS")
    print("-" * 80)
    print(f"Rows:        {parquet_file.metadata.num_rows:,}")
    print(f"Columns:     {parquet_file.metadata.num_columns}")
    print(f"Row groups:  {parquet_file.num_row_groups}")
    print(f"File size:   {parquet_file.metadata.serialized_size:,} bytes")
    
    # Schema metadata
    schema = parquet_file.schema_arrow
    metadata = schema.metadata
    
    if not metadata:
        print("\n⚠️  No metadata found in file")
        return
    
    # Check for GeoParquet metadata
    print("\n🗺️  GEOPARQUET METADATA")
    print("-" * 80)
    
    if b'geo' in metadata:
        geo_metadata = json.loads(metadata[b'geo'].decode())
        print(json.dumps(geo_metadata, indent=2))
        
        # Extract key info
        if 'columns' in geo_metadata:
            for col_name, col_info in geo_metadata['columns'].items():
                print(f"\n📍 Geometry Column: {col_name}")
                print(f"   Encoding: {col_info.get('encoding', 'N/A')}")
                print(f"   Geometry Types: {col_info.get('geometry_types', [])}")
                
                # CRS info
                if 'crs' in col_info and col_info['crs']:
                    crs = col_info['crs']
                    if 'id' in crs:
                        authority = crs['id'].get('authority', 'Unknown')
                        code = crs['id'].get('code', 'Unknown')
                        print(f"   CRS: {authority}:{code}")
                    if 'name' in crs:
                        print(f"   CRS Name: {crs['name']}")
                    if 'coordinate_system' in crs:
                        cs = crs['coordinate_system']
                        if 'axis' in cs and len(cs['axis']) > 0:
                            unit = cs['axis'][0].get('unit', {})
                            if isinstance(unit, dict):
                                unit_name = unit.get('name', 'unknown')
                            else:
                                unit_name = unit
                            print(f"   Units: {unit_name}")
                else:
                    print(f"   CRS: ❌ NOT SET")
                
                # Bbox
                if 'bbox' in col_info:
                    bbox = col_info['bbox']
                    print(f"   Bbox: [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]")
    else:
        print("❌ No 'geo' metadata found - this may not be a valid GeoParquet file")
    
    # Other metadata
    print("\n📋 OTHER METADATA")
    print("-" * 80)
    for key, value in metadata.items():
        if key != b'geo':
            key_str = key.decode()
            # Truncate long values
            value_str = value.decode()
            if len(value_str) > 200:
                value_str = value_str[:200] + "... (truncated)"
            print(f"{key_str}: {value_str}")
    
    # Geometry column Arrow extension
    print("\n🔧 ARROW EXTENSION INFO")
    print("-" * 80)
    try:
        geom_field = schema.field('geometry')
        if geom_field.metadata:
            for key, value in geom_field.metadata.items():
                key_str = key.decode()
                value_str = value.decode()
                if key_str != 'ARROW:extension:metadata':  # Skip detailed CRS
                    print(f"{key_str}: {value_str}")
    except KeyError:
        print("No 'geometry' column found")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_geoparquet_metadata.py <path_to_geoparquet_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        check_geoparquet_metadata(file_path)
    except FileNotFoundError:
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)