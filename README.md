# stmsn-dlt: Spatial Data Pipeline with dlt

>[!IMPORTANT]
>I'm retiring this repo's reliance on dlt — it doesn't handle geospatial ingestion well.
>See the [GeoParquet migration plan](ref/geoparquet-migration-plan.md) for the observations
>and the concrete conversion plan (GeoPandas `to_parquet` direct to GCS, dropping the
>two-pass finalizer, and moving bookkeeping into a DuckDB catalog).

A data pipeline project using [dlt](https://dlthub.com) to extract, load, and manage spatial
and tabular data from various sources into Google Cloud Storage as GeoParquet/Parquet.

## Project Structure

```
stmsn-dlt/
├── config/                          # Configuration files
│   ├── arcgis_datasets.yaml         # ArcGIS datasets, grouped by jurisdiction
│   └── webfiles_datasets.yaml       # Web Excel/CSV datasets, grouped by jurisdiction
├── clients/                         # Reusable HTTP / extraction clients
│   ├── arcgis.py                    # ArcGIS REST client: pagination, retry, geometry→WKB
│   ├── arcgis_metadata.py           # CRS / geometry-type / extent extraction
│   └── webfile.py                   # URL discovery + file download
├── pipelines/                       # dlt pipeline entry points
│   ├── arcgis.py                    # ArcGIS REST API → GeoParquet
│   └── webfiles.py                  # Web Excel/CSV → Parquet
├── utils/
│   ├── data_validator.py            # DuckDB baseline-metric validation gate
│   ├── file_parsers.py              # Excel/CSV → pandas (header detection)
│   ├── geoparquet_finalizer.py      # 2nd pass: parquet → GeoParquet (removed in migration)
│   └── load_history.py              # Track already-loaded files (skip re-loads)
├── scripts/                         # Utility scripts
│   ├── check_geoparquet.py          # Inspect GeoParquet metadata of a file
│   ├── convert_to_parquet.py        # Convert legacy CSV/TSV/GeoJSON/Shapefile → Parquet
│   └── upload_legacy_to_gcs.py      # Upload legacy Parquet to GCS (Hive partitioning)
├── ref/                             # Reference documentation
│   ├── arcgis.md                    # ArcGIS pipeline design document
│   └── geoparquet-migration-plan.md # Plan to retire dlt (GeoPandas direct-to-GCS)
└── .dlt/                            # dlt configuration
    ├── config.toml                  # Pipeline/destination config (compression, layout)
    └── secrets.toml                 # Credentials (gitignored)
```

## Setup

### 1. Install Dependencies

This project uses [uv](https://github.com/astral-sh/uv) for fast Python package management:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### 2. Configure GCS Credentials

Create `.dlt/secrets.toml` with your GCS configuration:

```toml
[destination.filesystem]
# Base path; each pipeline appends its source segment (e.g. /arcgis, /webfiles)
bucket_url = "gs://your-bucket-name/bronze"

[destination.filesystem.credentials]
project_id = "your-gcp-project-id"
# Option 1: Use service account key file
# private_key = "path/to/service-account-key.json"
# Option 2: Use application default credentials (gcloud auth)
# (no credentials needed if using ADC)
```

**Alternative:** Set environment variables:
```bash
export DESTINATION__FILESYSTEM__BUCKET_URL="gs://your-bucket-name/bronze"
export DESTINATION__FILESYSTEM__CREDENTIALS__PROJECT_ID="your-gcp-project-id"
```

### 3. Verify Setup

Test the pipeline locally with DuckDB (no GCS credentials needed):

```bash
uv run pipelines/arcgis.py --dry-run
```

## Pipelines

### ArcGIS Pipeline

Extracts spatial data from ArcGIS REST API endpoints and stores as yearly-partitioned GeoParquet in GCS.

**Features:**
- Configuration-driven: Add datasets via YAML, no code changes
- Jurisdiction-aware: datasets are grouped under a jurisdiction whose `crs` drives native-coordinate API requests
- Automatic pagination and retry logic (`clients/arcgis.py`)
- Geometry type extracted automatically from the service metadata (`clients/arcgis_metadata.py`)
- Validation gate: incoming aggregate metrics checked against the GCS baseline before loading
- Yearly snapshots: monthly runs write the current year's partition
- Pure ELT: format conversion only, transformations in dbt/DuckDB
- Smart sampling: dry-run / local-output mode automatically limits to 1,000 records for fast testing

**Usage:**

```bash
# Test locally with DuckDB (automatically samples 1,000 records)
uv run pipelines/arcgis.py --dry-run --datasets parcels

# Write real parquet to a local path (samples 1,000 records, no GCS needed)
uv run pipelines/arcgis.py --local-output ./test_output --datasets parcels

# Run all configured datasets (requires GCS credentials)
uv run pipelines/arcgis.py

# Extract specific datasets only
uv run pipelines/arcgis.py --datasets parcels streets

# Skip the validation gate or the GeoParquet conversion pass
uv run pipelines/arcgis.py --skip-validation
uv run pipelines/arcgis.py --skip-geoparquet

# Use custom config file
uv run pipelines/arcgis.py --config path/to/custom_config.yaml
```

**Add New Datasets:**

Edit `config/arcgis_datasets.yaml`. Datasets live under a jurisdiction; the jurisdiction's
`crs` is requested from the API, and each dataset's geometry type is discovered automatically
from the service metadata (no `geometry_type` key needed):

```yaml
jurisdictions:
  madison:
    crs: 8193  # drives native-coordinate API requests
    datasets:
      - name: parcels
        layer_url: "https://.../FeatureServer/0/query"
        description: "Madison parcel boundaries"
        validation:                 # optional: gate the load on a metric check
          enabled: true
          tolerance_percent: 5
          metrics:
            - parquet_column: total_taxes   # normalized column name in the parquet output
              aggregate: sum
              api_field: TotalTaxes         # field name in the ArcGIS API response
      - name: tax_roll
        layer_url: "https://.../MapServer/4/query"
        description: "Madison tax roll data"
        non_spatial: true           # skip GeoParquet conversion (no geometry)
```

**Validation:**

When a dataset declares a `validation` block, the pipeline accumulates the configured
aggregate metrics in-flight and compares them against the existing GCS baseline using DuckDB
(`utils/data_validator.py`) *before* loading. A result outside `tolerance_percent` aborts the
load so a bad pull can't overwrite good data. A first load with no baseline proceeds. Use
`--skip-validation` to bypass.

**GeoParquet conversion:**

The dlt load writes plain Parquet with geometry as a WKB-hex text column, then
`utils/geoparquet_finalizer.py` makes a second pass to rewrite each file as GeoParquet with the
CRS attached. This two-pass step is exactly what the
[migration plan](ref/geoparquet-migration-plan.md) removes (writing GeoParquet directly in one
pass). Use `--skip-geoparquet` to stop after the plain-Parquet load.

**Output Structure:**

```
gs://your-bucket/<base>/arcgis/
└── {jurisdiction}/                  # e.g. madison
    ├── parcels/
    │   └── year=2026/<file>.parquet
    ├── streets/
    │   └── year=2026/<file>.parquet
    └── {dataset_name}/
        └── year=YYYY/<file>.parquet
```

**Design Philosophy:**

See [ref/arcgis.md](ref/arcgis.md) for comprehensive design documentation covering:
- Multi-dataset architecture
- Temporal strategy (yearly snapshots)
- Storage structure and partitioning
- Downstream dbt/DuckDB integration

### Web Files Pipeline

Discovers and downloads Excel/CSV files from configured web URLs and stores them as
Hive-partitioned Parquet in GCS (`pipelines/webfiles.py`). It probes/lists candidate URLs via
`clients/webfile.py`, parses files into pandas DataFrames with automatic header detection
(`utils/file_parsers.py`), and records every processed file in `load_history`
(`utils/load_history.py`) so already-loaded files are skipped on later runs.

**Usage:**

```bash
# Run all configured datasets (requires GCS credentials)
uv run pipelines/webfiles.py

# Test locally with DuckDB
uv run pipelines/webfiles.py --dry-run

# Write real parquet to a local path
uv run pipelines/webfiles.py --local-output ./test_output --datasets tax_roll

# Force reload, ignoring load history
uv run pipelines/webfiles.py --force

# Download one explicit URL (requires --datasets)
uv run pipelines/webfiles.py --url "https://example.com/2024taxroll.xlsx" --datasets tax_roll
```

**Configuration** (`config/webfiles_datasets.yaml`): datasets are grouped by jurisdiction,
each with a `base_url`, `file_type`, candidate `url_patterns`, a `partition` spec (extracts
e.g. `year` from the filename), and optional `excel` settings (sheet, header detection).

**Output Structure:**

```
gs://your-bucket/<base>/webfiles/
└── {jurisdiction}/
    └── {partition}=.../           # e.g. year=2024
        └── <file>.parquet
```

## Utility Scripts

### Convert Legacy Data to Parquet

Convert CSV, TSV, GeoJSON, or Shapefiles to Parquet/GeoParquet:

```bash
# Convert all files in a directory
uv run scripts/convert_to_parquet.py /path/to/data

# With verbose output
uv run scripts/convert_to_parquet.py /path/to/data --verbose

# Overwrite existing Parquet files
uv run scripts/convert_to_parquet.py /path/to/data --force
```

### Upload Legacy Data to GCS

Upload Parquet files to GCS with Hive partitioning support:

```bash
# Set environment variables
export BUCKET_NAME="your-bucket-name"
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"

# Dry run (preview only)
uv run scripts/upload_legacy_to_gcs.py /path/to/parquet --dry-run

# Actually upload
uv run scripts/upload_legacy_to_gcs.py /path/to/parquet
```

## Development

### Project Requirements

- Python 3.12+
- uv package manager
- Google Cloud Platform account (for production)

### Key Dependencies

- `dlt[gcp,duckdb]` - Data load tool with GCP + DuckDB support
- `geopandas` / `shapely` - Spatial data handling
- `pandas` - Data manipulation
- `pyarrow` - Parquet support
- `openpyxl` - Excel parsing (webfiles)
- `pyyaml` - Config loading
- `requests` - HTTP client for API calls

### Testing Locally

Always test pipelines with DuckDB before running against GCS:

```bash
# Run pipeline in test mode
uv run pipelines/arcgis.py --dry-run

# Query the results in Python (dataset name is the jurisdiction, e.g. madison)
python
>>> import dlt
>>> pipeline = dlt.pipeline(pipeline_name='arcgis_madison_test', destination='duckdb')
>>> dataset = pipeline.dataset()
>>> print(dataset.parcels.df())
```

### Adding New Pipelines

1. Create a pipeline entry point in `pipelines/` (see `arcgis.py` / `webfiles.py` for the pattern)
2. Put reusable extraction/HTTP logic in `clients/` and shared helpers in `utils/`
3. Follow the dlt resource pattern
4. Add a configuration file in `config/` if needed
5. Document in README

## Scheduling

For production, schedule monthly runs using:

- **Cloud Scheduler + Cloud Run**: Serverless option
- **Apache Airflow**: Full orchestration platform
- **Cron**: Simple scheduled execution

Example cron (runs 1st of each month at 2am):
```cron
0 2 1 * * cd /path/to/stmsn-dlt && uv run pipelines/arcgis.py
```

## Downstream Integration

After dlt loads raw spatial data to GCS:

1. **dbt/DuckDB transformations**:
   - Geometry validation and topology checks
   - Attribute standardization and cleaning
   - Duplicate detection and resolution
   - Business logic and derived metrics

2. **Query pattern**:
```sql
-- DuckDB query example (note the {jurisdiction} segment in the path)
SELECT *
FROM read_parquet('gs://bucket/bronze/arcgis/madison/parcels/year=*/*.parquet')
WHERE year >= 2023;
```

## Resources

- [dlt Documentation](https://dlthub.com/docs/intro)
- [dlt Filesystem Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [GeoParquet Specification](https://geoparquet.org/)

## License

MIT (or your preferred license)
