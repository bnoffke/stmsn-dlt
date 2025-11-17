# stmsn-dlt: Spatial Data Pipeline with dlt

A data pipeline project using [dlt](https://dlthub.com) to extract, load, and manage spatial data from various sources into Google Cloud Storage as GeoParquet.

## Project Structure

```
stmsn-dlt/
├── config/                          # Configuration files
│   └── arcgis_datasets.yaml        # ArcGIS dataset definitions
├── pipelines/                       # dlt pipeline implementations
│   ├── arcgis.py                   # ArcGIS REST API → GeoParquet
│   └── real_estate.py              # Real estate data pipeline
├── scripts/                         # Utility scripts
│   ├── convert_to_parquet.py       # Convert legacy data to Parquet
│   └── upload_legacy_to_gcs.py     # Upload legacy data to GCS
├── ref/                             # Reference documentation
│   └── arcgis.md                   # ArcGIS pipeline design document
└── .dlt/                            # dlt configuration
    ├── config.toml                 # Pipeline configuration
    └── secrets.toml                # Credentials (gitignored)
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
bucket_url = "gs://your-bucket-name/arcgis"

[destination.filesystem.credentials]
project_id = "your-gcp-project-id"
# Option 1: Use service account key file
# private_key = "path/to/service-account-key.json"
# Option 2: Use application default credentials (gcloud auth)
# (no credentials needed if using ADC)
```

**Alternative:** Set environment variables:
```bash
export DESTINATION__FILESYSTEM__BUCKET_URL="gs://your-bucket-name/arcgis"
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
- Automatic pagination and retry logic
- Yearly snapshots: Monthly runs replace current year's file
- Pure ELT: Format conversion only, transformations in dbt/DuckDB
- Smart sampling: Dry-run mode automatically limits to 1,000 records for fast testing

**Usage:**

```bash
# Test locally with DuckDB (automatically samples 1,000 records)
uv run pipelines/arcgis.py --dry-run --datasets parcels

# Run all configured datasets (requires GCS credentials)
uv run pipelines/arcgis.py

# Extract specific datasets only
uv run pipelines/arcgis.py --datasets parcels streets

# Use custom config file
uv run pipelines/arcgis.py --config path/to/custom_config.yaml
```

**Add New Datasets:**

Edit `config/arcgis_datasets.yaml`:

```yaml
arcgis_datasets:
  - name: your_dataset_name
    layer_url: "https://your-arcgis-server.com/.../query"
    geometry_type: polygon  # or linestring, point
    description: "Optional description"
```

**Output Structure:**

```
gs://your-bucket/arcgis/
├── parcels/
│   └── year=2025/data.parquet
├── streets/
│   └── year=2025/data.parquet
└── [dataset_name]/
    └── year=YYYY/data.parquet
```

**Design Philosophy:**

See [ref/arcgis.md](ref/arcgis.md) for comprehensive design documentation covering:
- Multi-dataset architecture
- Temporal strategy (yearly snapshots)
- Storage structure and partitioning
- Downstream dbt/DuckDB integration

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

- `dlt[gcp]` - Data load tool with GCP support
- `geopandas` - Spatial data handling
- `pandas` - Data manipulation
- `pyarrow` - Parquet support
- `requests` - HTTP client for API calls

### Testing Locally

Always test pipelines with DuckDB before running against GCS:

```bash
# Run pipeline in test mode
uv run pipelines/arcgis.py --dry-run

# Query the results in Python
python
>>> import dlt
>>> pipeline = dlt.pipeline(pipeline_name='arcgis_test', destination='duckdb')
>>> with pipeline.sql_client() as client:
...     df = client.execute_sql("SELECT * FROM arcgis_data.parcels LIMIT 10").df()
...     print(df)
```

### Adding New Pipelines

1. Create pipeline file in `pipelines/` directory
2. Follow dlt resource pattern (see `arcgis.py` for example)
3. Add configuration file in `config/` if needed
4. Document in README

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
-- DuckDB query example
SELECT *
FROM read_parquet('gs://bucket/arcgis/parcels/year=*/data.parquet')
WHERE year >= 2023;
```

## Resources

- [dlt Documentation](https://dlthub.com/docs/intro)
- [dlt Filesystem Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- [GeoParquet Specification](https://geoparquet.org/)

## License

MIT (or your preferred license)
