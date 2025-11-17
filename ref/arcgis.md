# ArcGIS dlt Pipeline Documentation

## Overview

This document describes a multi-dataset ArcGIS REST API ingestion pipeline built with dlt (data load tool), an open-source Python library for ELT data loading. The pipeline extracts spatial data from ArcGIS services and stores it as GeoParquet in Google Cloud Storage (GCS).

## Core Design Principles

### 1. Multi-Dataset Architecture
- **Single pipeline, multiple resources**: One dlt pipeline handles multiple spatial datasets (parcels, streets, impervious surfaces, etc.)
- **Configuration-driven**: New datasets added via YAML config—no code changes required
- **Shared API client**: All datasets use the same ArcGIS REST API client for consistency

### 2. Temporal Strategy: Yearly Snapshots
**Key principle**: Extract monthly, store yearly.

- Monthly pipeline runs extract current data from ArcGIS REST API
- Data is written to year-partitioned files in GCS
- Current year file is **replaced** with each monthly run (not appended)
- Historical years remain frozen once the calendar year ends
- Minimizes storage costs (1 file/year vs 12 files/year)

**Example timeline**:
```
Jan 2024 run  → writes year=2024/data.parquet
Feb 2024 run  → replaces year=2024/data.parquet
...
Dec 2024 run  → replaces year=2024/data.parquet
Jan 2025 run  → creates year=2025/data.parquet (2024 now frozen)
```

### 3. Pure ELT Philosophy
**dlt responsibilities (Extract + Load)**:
- API calls with pagination and retry logic
- Format conversion: GeoJSON → GeoParquet
- Schema inference and data typing
- State management and incremental tracking

**NOT dlt responsibilities** (Transform in dbt/DuckDB):
- Data validation or quality checks
- Geometry fixing or topology validation
- Attribute cleaning or standardization
- Deduplication logic
- Business rules or derived fields

### 4. Storage Structure
```
gs://your-bucket/arcgis/
├── parcels/
│   ├── year=2023/data.parquet  # finalized
│   ├── year=2024/data.parquet  # replaced monthly in 2024
│   └── year=2025/data.parquet  # active
├── streets/
│   ├── year=2023/data.parquet
│   └── year=2024/data.parquet
└── impervious_surfaces/
    └── year=2024/data.parquet
```

## Technical Implementation

### dlt Pipeline Configuration

**Basic pipeline setup**:
```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="arcgis_pipeline",
    destination="filesystem",  # GCS via filesystem destination
    dataset_name="arcgis"
)
```

### Dataset Configuration (YAML)
```yaml
# config/arcgis_datasets.yaml
arcgis_datasets:
  - name: parcels
    layer_url: "https://gis.example.com/arcgis/rest/services/Parcels/MapServer/0"
    geometry_type: polygon
    
  - name: streets
    layer_url: "https://gis.example.com/arcgis/rest/services/Streets/MapServer/0"
    geometry_type: linestring
    
  - name: impervious_surfaces
    layer_url: "https://gis.example.com/arcgis/rest/services/ImperviousSurfaces/MapServer/0"
    geometry_type: polygon
```

### Key Technical Specifications

**Format & Compression**:
- Output format: GeoParquet
- Compression: Snappy or Zstd (configurable)
- CRS: Standardize to EPSG:4326 (or project-specific CRS)

**Partitioning**:
- Partition key: `year` only (no month-level partitions)
- Partition value: Extracted from current date at runtime

**Write Disposition**:
- Use `replace` disposition at the year partition level
- dlt handles state tracking and incremental loading internally

**State Management**:
- dlt tracks extraction metadata (last run time, records processed)
- State persisted to enable incremental/delta detection
- No explicit state file management needed in code

### Resource Implementation Pattern

**Conceptual structure** (adapt to your ArcGIS client):
```python
import dlt
from typing import Iterator, Dict, Any

@dlt.resource(name="parcels", write_disposition="replace")
def extract_parcels(
    layer_url: str,
    geometry_type: str
) -> Iterator[Dict[str, Any]]:
    """
    Extract parcels from ArcGIS REST API.
    
    - Handles pagination automatically
    - Yields records as they're fetched (generator pattern)
    - Converts GeoJSON to dict structure
    - dlt handles conversion to GeoParquet downstream
    """
    # Your ArcGIS API client logic here
    # Yield records one at a time or in batches
    for record in fetch_from_arcgis(layer_url):
        yield record
```

### Yearly Partition Handling

**Add partition column dynamically**:
```python
from datetime import datetime

@dlt.resource
def extract_with_partition(layer_url: str):
    current_year = datetime.now().year
    
    for record in fetch_from_arcgis(layer_url):
        record["year"] = current_year  # Add partition column
        yield record
```

**Configure filesystem destination** for partitioned writes:
```python
# secrets.toml or environment
[destination.filesystem]
bucket_url = "gs://your-bucket/arcgis"
layout = "{table_name}/year={year}/{file_name}.parquet"
```

## Pipeline Execution

### Monthly Run Workflow
1. **Extract**: Fetch current data from all configured ArcGIS layers
2. **Transform**: Convert GeoJSON → dict → GeoParquet (format only)
3. **Partition**: Add `year` column based on current date
4. **Load**: Write/replace `year=YYYY/data.parquet` in GCS
5. **Track**: Update dlt state with run metadata

### Local Testing with DuckDB
dlt recommends testing pipelines locally with DuckDB before deploying to production destinations:

```python
# Test pipeline locally
test_pipeline = dlt.pipeline(
    pipeline_name="arcgis_test",
    destination="duckdb",
    dataset_name="arcgis_test"
)

# Run and inspect
load_info = test_pipeline.run(extract_parcels(...))
print(load_info)

# Query loaded data
test_pipeline.dataset().parcels.df()
```

## Benefits Summary

✅ **Cost-effective**: 1 file per year vs 12 files per year  
✅ **Query-friendly**: Simple year-based partitioning for DuckDB/SQL  
✅ **Scalable**: Easy to add new datasets via config  
✅ **Maintainable**: Schema evolution and data quality handled in dbt  
✅ **Flexible**: Same structure for historical and current data  
✅ **Aligned**: Matches annual assessment and planning cycles  

## Downstream Integration

### dbt/DuckDB Responsibilities
After dlt loads raw spatial data:
- Geometry validation and topology checks
- Attribute standardization and cleaning
- Duplicate detection and resolution
- Cross-dataset joins and relationships
- Business logic and derived metrics
- Data quality tests and monitoring

### Querying Pattern
```sql
-- DuckDB query example
SELECT *
FROM read_parquet('gs://bucket/arcgis/parcels/year=*/data.parquet')
WHERE year >= 2023;
```

## Adding New Datasets

To add a new dataset (e.g., `buildings`):

1. **Update config YAML**:
```yaml
- name: buildings
  layer_url: "https://gis.example.com/.../Buildings/MapServer/0"
  geometry_type: polygon
```

2. **No code changes needed**—pipeline automatically picks up new config

3. **Deploy and run**—new dataset follows same yearly pattern

## Important Notes

- **No transformations in dlt**: Keep extraction logic pure and simple
- **State persistence**: Ensure dlt state directory is backed up/persisted
- **Error handling**: dlt provides automatic retries; configure limits appropriately
- **API rate limits**: Implement respectful delays in ArcGIS client
- **CRS consistency**: Standardize output CRS across all datasets
- **Monitoring**: dlt provides schema inference and evolution alerts for governance

## References

- [dlt Documentation](https://dlthub.com/docs/intro)
- [dlt Filesystem Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [dlt GitHub Repository](https://github.com/dlt-hub/dlt)

---

**Pipeline Philosophy**: Extract simply, load efficiently, transform intelligently.