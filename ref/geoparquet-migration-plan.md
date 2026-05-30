# Replacing dlt for ArcGIS → GeoParquet: Observations & Conversion Plan

**Status:** Draft for review · **Date:** 2026-05-29
**Scope (confirmed):** ArcGIS spatial pipeline **and** the webfiles pipeline. Move both off `dlt`'s
load/destination machinery while keeping the existing HTTP clients, metadata extraction, validation,
and load-history logic.

**Direction (confirmed):**
- Write engine: **GeoPandas `to_parquet` direct to GCS** (single pass, deletes the finalizer).
- Validation: **keep in the extract layer** as a pre-write gate.
- Column naming: **recreate dlt's `snake_case`** at write time so downstream names are unchanged.
- Write settings: **zstd** compression; **GeoParquet 1.1 with covering bbox**.
- **Drop the spatial metadata registry table** — fetch CRS/geometry-type at runtime and attach it
  inline to the GeoDataFrame; nothing persisted.
- **`validation_results` + `load_history` move into a DuckDB catalog** (SQL-native), not Parquet tables.
- **Atomic writes**: write to a temp key, then rename/copy to the final object so a failed load can't
  leave a partial or clobbered partition.
- **`--dry-run` = local path + 1000-row sample**; drop the DuckDB "dry-run destination".

---

## 1. Why this exists

The ArcGIS pipeline currently uses `dlt` to land data, then a second pass rewrites those files as
GeoParquet. `dlt` is doing a lot of work we don't need (state, schema contracts, load packages,
incremental machinery) and almost none of the work we *do* need (writing valid GeoParquet with a
CRS). The result is a two-pass "write plain parquet, read it back, rewrite as GeoParquet in place"
dance that is the source of most of the operational friction. This doc records how the pipeline
behaves today, surveys the common ways people write GeoParquet to object storage, and proposes a
concrete replacement.

---

## 2. How the pipeline behaves today

### 2.1 ArcGIS flow (the painful path)

1. **Extract** — `clients/arcgis.py::ArcGISClient.fetch_features()` paginates the ArcGIS REST API,
   converts each feature's geometry (ArcGIS JSON or GeoJSON) → Shapely → **WKB hex string**, and
   yields flat dict records (`attributes` + `geometry` as a hex `str`).
2. **Metadata** — `clients/arcgis_metadata.py` hits the service definition and captures CRS
   (`crs_epsg`/`crs_wkid`/`crs_wkt`), geometry type, extent, and Z/M flags into a
   `spatial_metadata_registry` table.
3. **Validation** — `pipelines/arcgis.py::create_arcgis_resource()` buffers **all** records in memory,
   accumulates metrics in-flight (`MetricsAccumulator`), and compares against a GCS baseline
   (`DataValidator`). A failure raises `ValidationError` and blocks the load.
4. **Load (dlt)** — `pipeline.run(..., loader_file_format="parquet")` writes plain Parquet to
   `{table_name}/year={YYYY}/{file_id}.parquet` (layout from `.dlt/config.toml`). Geometry is stored
   as a **text column** of WKB hex. dlt also writes its own bookkeeping
   (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`).
5. **Finalize (second pass)** — `utils/geoparquet_finalizer.py` reads every Parquet file back from
   GCS, converts the hex column back to Shapely, builds a `GeoDataFrame` with the CRS from the
   registry, writes a temp GeoParquet, and **uploads it back over the original object** via a
   PyArrow byte-stream round trip.

### 2.2 Webfiles flow

`pipelines/webfiles.py` discovers/downloads Excel/CSV via `clients/webfile.py`, parses to a pandas
`DataFrame` (`utils/file_parsers.py`), adds partition columns, converts to records, and hands them to
dlt for Parquet output under `{table_name}/{partition}=.../{file_id}.parquet`. It also maintains a
`load_history` table in GCS (`utils/load_history.py`, queried with DuckDB+gcsfs) to skip
already-loaded files. **No geometry** — it just needs plain Parquet.

### 2.3 Concrete pain points

| # | Friction | Root cause |
|---|----------|------------|
| 1 | **Two passes over every byte** (write parquet → read → rewrite geoparquet) | dlt can't emit GeoParquet metadata, so geometry goes in as a text column |
| 2 | **In-place GCS overwrite via temp files + PyArrow byte streams** (`process_dataset`) | No direct "write a GeoDataFrame to this object" step |
| 3 | **Dictionary-encoding normalization hack** (`read_parquet_normalized`) | dlt's Parquet output produces inconsistent dictionary encodings across files that break PyArrow schema merge |
| 4 | **WKB-hex-as-text** serialized and re-parsed | geometry never typed as geometry until the second pass |
| 5 | **Opaque file names** (`f39c7aa42c.parquet`) and dlt sidecar dirs (`_dlt_*`) | dlt's load-package layout; makes the "replace current year" intent harder to reason about |
| 6 | **CRS lives only in a side table** and is reattached late | dlt has no spatial type, so CRS can't ride with the data |
| 7 | dlt's value (state, contracts, incremental) is **unused** | wrong tool for an idempotent yearly snapshot |

---

## 3. What to keep vs. drop

**Keep (still useful):**
- `clients/arcgis.py` — pagination/retry/geometry conversion. (Tweak: also expose Shapely objects, not
  only WKB hex — see §6.)
- `clients/arcgis_metadata.py` — CRS/geometry-type/extent extraction.
- `clients/webfile.py`, `utils/file_parsers.py` — discovery, download, parsing.
- `MetricsAccumulator` / `DataValidator` — validation gate (§7).
- `utils/load_history.py` — already standalone (DuckDB + gcsfs); barely touches dlt.

**Drop:**
- `dlt` as a dependency for *loading* (the `dlt.pipeline`/`dlt.resource`/filesystem-destination layer
  in both pipelines).
- `utils/geoparquet_finalizer.py` — eliminated entirely by writing GeoParquet in one pass.
- The dictionary-normalization workaround — disappears with the finalizer.
- dlt's DuckDB "dry-run" destination — replaced by writing to a local path (see §9).

---

## 4. Framework landscape (research)

Common ways to get records → GeoParquet on object storage:

| Approach | How it writes | CRS / GeoParquet metadata | GCS write | Fit here |
|----------|---------------|---------------------------|-----------|----------|
| **GeoPandas `to_parquet`** | `gdf.to_parquet("gs://…")` (fsspec/gcsfs) | Native; CRS rides with the frame; WKB (default) or GeoArrow encoding; GeoParquet 1.1 incl. optional covering bbox | Yes, via gcsfs | **Chosen** — drop-in, reuses Shapely/geopandas already in deps |
| **DuckDB spatial (1.1+)** | `COPY (… ST_GeomFromHEXWKB(geometry) …) TO 'gs://…' (FORMAT parquet)` | Writes valid GeoParquet + bbox automatically when `spatial` loaded | Needs `httpfs` + GCS secret | Strong alternative; adds an engine + SQL plumbing for marginal gain over GeoPandas |
| **PyArrow + GeoArrow (low-level)** | Build Arrow table w/ GeoArrow extension types, `pq.write_table` | Manual GeoParquet metadata | PyArrow `GcsFileSystem` | Most control, most code; unnecessary here |
| **pyogrio / GDAL** | `pyogrio.write_dataframe(..., driver="Parquet")` | Native | Via GDAL VSI | Extra heavy dep; no advantage over GeoPandas |

**Decision: GeoPandas direct-to-GCS.** It reuses the Shapely conversion already in `ArcGISClient`,
keeps the whole flow in one Python process, removes the finalizer outright, and writes spec-compliant
GeoParquet with CRS attached. DuckDB stays in the project for what it's already good at (reading
partitioned GeoParquet for validation/load-history), not for writing.

### Write settings (research-backed defaults)
- **Compression: `zstd`** — better ratio than the current `snappy` at comparable decode speed; wins on
  network-bound cloud reads. (Current `.dlt/config.toml` uses snappy.)
- **GeoParquet 1.1 with covering bbox** — `gdf.to_parquet(..., schema_version="1.1.0",
  write_covering_bbox=True)` adds a bbox struct column enabling spatial predicate pushdown in
  DuckDB/readers.
- **Geometry encoding: WKB** (default) for max interoperability; revisit GeoArrow later if readers
  support it.
- **`row_group_size` ≈ 100k–150k** — keep current intent (`100000`); avoid tiny row groups.
- **One deterministic object per dataset/year** — write `…/year=YYYY/data.parquet` (not a hashed
  name) so the yearly "replace" is a plain overwrite (§9).

---

## 5. Target architecture

A small, single-purpose writer module replaces the dlt load + finalizer:

```
clients/            (unchanged: arcgis, arcgis_metadata, webfile)
utils/
  geoparquet_writer.py   ← NEW: records + metadata → GeoParquet on GCS (one pass)
  parquet_writer.py      ← NEW (or fold into above): plain DataFrame → Parquet on GCS (webfiles)
  naming.py              ← NEW: recreate dlt snake_case (§8)
  data_validator.py      (unchanged)
  load_history.py        (unchanged)
pipelines/
  arcgis.py              ← orchestration only: iterate datasets, validate, call writer
  webfiles.py            ← orchestration only: discover/download/parse, call writer
```

**ArcGIS write sketch:**

```python
import geopandas as gpd
from shapely import wkb

def write_geoparquet(records, metadata, gcs_uri):
    # records: list[dict] with normalized column names + "geometry" (shapely or WKB hex)
    gdf = gpd.GeoDataFrame(records)
    gdf["geometry"] = gdf["geometry"].map(
        lambda x: wkb.loads(bytes.fromhex(x)) if isinstance(x, str) else x
    )
    crs = metadata.get("crs_epsg") or metadata.get("crs_wkid")
    gdf = gdf.set_geometry("geometry").set_crs(f"EPSG:{crs}" if crs else metadata.get("crs_wkt"))
    gdf.to_parquet(
        gcs_uri,                      # e.g. gs://bucket/bronze/arcgis/madison/parcels/year=2026/data.parquet
        compression="zstd",
        schema_version="1.1.0",
        write_covering_bbox=True,
        index=False,
    )
```

Non-spatial ArcGIS tables (e.g. `tax_roll`, `non_spatial: true`) and all webfiles output skip the
GeoDataFrame and write plain Parquet (`df.to_parquet("gs://…", compression="zstd")`).

> **Dependency note:** `gs://` paths in `to_parquet` route through fsspec → **gcsfs**. `dlt[gcp]`
> already pulls gcsfs transitively; once dlt is removed, add `gcsfs` (and keep `duckdb` for
> validation/history) as explicit deps in `pyproject.toml`.

---

## 6. Suggested client tweak

`ArcGISClient` currently emits geometry as **WKB hex text**. The writer can consume that directly
(decode hex → Shapely as above), so **no client change is strictly required**. Optional cleanup:
add a `convert_to="shapely"` mode so the client can yield Shapely geometries directly and skip the
hex round-trip entirely. Low priority — the hex path works and keeps the client output JSON-friendly.

---

## 7. Validation (keep in extract layer)

Preserve the current pre-write gate so a bad pull never overwrites good GCS data:

1. Stream records from `ArcGISClient`, feeding `MetricsAccumulator` in-flight (unchanged).
2. After the pull, run `DataValidator.validate_dataset(...)` against the GCS baseline (unchanged).
3. **Only on pass / no-baseline** do we call the writer. On failure, raise and write nothing.

This is exactly today's control flow minus dlt — the buffering + validate + "then yield" pattern in
`create_arcgis_resource` becomes buffer + validate + **write**. `DataValidator` already reads
partitioned Parquet from GCS via DuckDB, so it keeps working against the new GeoParquet output
(GeoParquet *is* Parquet; the bbox/geometry columns don't disturb the metric SQL).

**Decided:** validation *results* are no longer written as a Parquet table — they go to the DuckDB
catalog (§7.1). And `spatial_metadata_registry` is **dropped entirely** (§7.2).

### 7.1 Bookkeeping → DuckDB catalog

`validation_results` and the webfiles `load_history` move out of Parquet and into a single
**SQL-native DuckDB catalog**, e.g. `…/stmsn-catalog.duckdb`. Rationale: these are small, append-only
bookkeeping tables that we query (not bulk-scan), and a DuckDB database is the most SQL-native home.

- **Schema:** `load_history(dataset_name, jurisdiction, source_url, filename, partition_values,
  loaded_at, row_count, file_hash)` and `validation_results(dataset_name, jurisdiction,
  validation_timestamp, status, results)` — the same fields the dataclasses already carry.
- **Lifecycle (single monthly writer, no concurrency):** DuckDB can't write its database file directly
  over `gs://`, so the pattern is: pull the catalog file from GCS → `ATTACH` locally → `INSERT` →
  push back. (Or keep it local and back up to GCS after each run.) For a once-a-month single-writer
  pipeline this is safe and simple.
- `utils/load_history.py` already uses DuckDB+gcsfs to *read* history; it shifts from "read Parquet
  glob" to "query the catalog table," which is a smaller, cleaner surface.
- **Decided (Q-A):** the catalog is a **GCS-hosted `.duckdb` file**, pulled at the start of a run and
  pushed back at the end — single source of truth, no local divergence.

### 7.2 Spatial metadata registry → dropped

The `spatial_metadata_registry` table existed mainly to feed CRS/geometry-type to the second-pass
finalizer. With single-pass writing, the writer already has the metadata in hand (from
`ArcGISMetadataExtractor`) at the moment it builds the GeoDataFrame, so CRS rides *with* the data into
GeoParquet and there's nothing to persist separately. `clients/arcgis_metadata.py` stays; the
**table** goes away. (If a downstream consumer ever wants the extent/Z-M flags, they're already in the
GeoParquet/file metadata or can be recomputed.)

---

## 8. Schema handling / sluggification (recreate `snake_case`)

You opted to reproduce dlt's normalization so downstream column names are unchanged (e.g.
`TotalTaxes → total_taxes`, `CurTotal → cur_total`, matching the current YAML metric configs). dlt's
default **`snake_case`** convention is simple and self-contained. Documented rules (from dlt's
`snake_case` naming convention):

1. Trim surrounding whitespace.
2. Split camelCase / PascalCase into words and insert `_` at case boundaries
   (`([^_])([A-Z][a-z]+)` and `([a-z0-9])([A-Z])`), then lowercase. So `TotalTaxes → total_taxes`,
   `XCoord → x_coord`, but an all-caps token has no internal boundary: **`OBJECTID → objectid`**.
3. Replace every char that isn't ASCII alphanumeric or `_` with `_`, with special-case maps:
   `+`,`*` → `x`; `-` → `_`; `@` → `a`; `|` → `l`.
4. Collapse runs of `_` into a single `_`.
5. Prepend `_` if the identifier starts with a digit.
6. Replace trailing `_` with `x`.
7. (Nested keys use `__` as a parent/child separator — **not relevant** here; ArcGIS attributes are
   flat.)

**Recommendation:** implement these in `utils/naming.py` as `normalize_identifier(name) -> str` and
apply to record keys before writing. To *guarantee* parity instead of reverse-engineering every edge
case, add a one-time parity test while dlt is still installed:

```python
from dlt.common.normalizers.naming.snake_case import NamingConvention
import utils.naming as n
nc = NamingConvention()
for name in observed_arcgis_and_webfile_fields:   # pull the real field sets from the services
    assert n.normalize_identifier(name) == nc.normalize_identifier(name)
```

If any field disagrees, either adjust `naming.py` or accept the divergence consciously. If parity
turns out to be fussy for a handful of oddball fields, the fallback is to **write raw ArcGIS names and
normalize in dbt staging** — but per your decision we default to recreating `snake_case`.

---

## 9. Yearly snapshot / "replace current year" semantics

> **Observed bug in the current design (motivating this change):** a recent run created `year=2026/`
> but **dropped the existing `year=2025/` partition**. That's because dlt's
> `write_disposition="replace"` is **table-scoped, not partition-scoped** — it replaces the whole
> table on each run, so prior years are *not* actually frozen the way `ref/arcgis.md` claims. The
> "yearly snapshot, historical years frozen" intent has therefore been silently broken under dlt.

Direct writes fix this by construction:

- Path: `…/arcgis/{jurisdiction}/{dataset}/year={YYYY}/data.parquet` (single deterministic object).
- A monthly run **only ever touches `year={current}/data.parquet`** — it writes that one object and
  never enumerates or deletes other years, so `year=2025/` stays put when 2026 runs.
- No `_dlt_*` sidecar directories to manage.
- Local testing: pass a local base path (e.g. `./test_output/...`) to the same writer — no DuckDB
  "dry-run destination" needed. **`--dry-run` = "local path + 1000-row sample"** (confirmed).

> If older partitions were already lost from GCS, they may need to be re-backfilled from source
> (where ArcGIS still serves current geometry) or from any prior local/legacy copies — worth checking
> before the first production cut-over.

---

## 10. Webfiles migration

Simpler than ArcGIS (no geometry):

- Replace the `dlt.resource` + filesystem-destination block with `df.to_parquet("gs://…",
  compression="zstd")`, building the partitioned path from the existing `partition_values` (reuse
  `build_layout`'s intent as a plain f-string).
- `load_history` is already dlt-independent except that it's *written* as a dlt resource — switch its
  writes to the DuckDB catalog (§7.1): `INSERT` new rows instead of appending Parquet files.
- Apply the same `snake_case` normalization to columns for consistency with the parsed DataFrames.

---

## 11. Suggested phasing

1. **`utils/naming.py` + parity test** against dlt (cheap, de-risks §8).
2. **`utils/geoparquet_writer.py`** + plain-Parquet writer; temp-key+rename; unit-test locally
   (write/read round trip, CRS present, bbox column present, `geopandas.read_parquet` happy).
3. **DuckDB catalog** (§7.1): create `load_history` + `validation_results` tables and the
   pull/`ATTACH`/`INSERT`/push helper. Migrate any existing Parquet `load_history` rows in.
4. **Cut over `pipelines/arcgis.py`**: replace dlt load + finalizer with validate→write; attach CRS
   inline and **drop the `spatial_metadata_registry` resource** (§7.2). Keep `--datasets`,
   `--dry-run` (now local), validation flags. Delete `utils/geoparquet_finalizer.py`.
5. **Cut over `pipelines/webfiles.py`** similarly; `load_history` writes go to the catalog.
6. **Prune deps**: drop `dlt`, add explicit `gcsfs`; keep `duckdb`, `geopandas`, `pyarrow`, `shapely`.
7. **Update `ref/arcgis.md` + README** to drop the dlt framing (and correct the "historical years
   frozen" claim per §9).
8. Delete `test_output/**/_dlt_*` and the `.dlt/` config/secrets once nothing reads them (move
   bucket/credentials config to env vars or a small project config).

---

## 12. Risks & things to watch

- **gcsfs auth**: `to_parquet("gs://…")` uses Application Default Credentials by default (same as the
  current validator). Confirm ADC works in the runtime that runs the pipeline.
- **Mixed geometry types / nulls**: GeoParquet best practice is one geometry type per file. ArcGIS
  polylines map to `MultiLineString`, polygons to `Polygon` — fine. Watch for null geometries and for
  layers that mix singlepart/multipart; may need to promote to multi-type.
- **Schema drift across years**: dlt previously absorbed some of this. With direct writes, a new
  column appearing in a later year just shows up in that year's file; resolve at read time in dbt
  (`union_by_name`) — acceptable per the ELT philosophy.
- **Atomicity of overwrite (decided: temp key + rename)**: the writer writes to a temp object
  (e.g. `…/year=2026/data.parquet.tmp-<runid>`) and only renames/copies it over the final
  `data.parquet` once the write fully succeeds, so a failed load can't leave a partial object or
  clobber the prior good file. Note GCS has no atomic server-side rename — it's copy-then-delete —
  but for a single monthly writer that's effectively atomic and still far lighter than the old
  read-modify-rewrite dance (it's one object, not a whole-directory round trip).

---

## 13. Resolved decisions

| # | Decision |
|---|----------|
| Q1 — bbox covering | **Enable** GeoParquet 1.1 `write_covering_bbox=True`. |
| Q2 — compression | **Switch `snappy → zstd`.** |
| Q3 — bookkeeping tables | **Drop** `spatial_metadata_registry` (CRS attached inline). **Move** `validation_results` + `load_history` into a **DuckDB catalog** (§7.1). |
| Q4 — atomic overwrite | **Adopt temp-key + rename** for crash safety on the yearly object. |
| Q5 — local/dry-run | **`--dry-run` = local path + 1000-row sample;** drop the DuckDB dry-run destination. |

| Q-A — catalog location | **GCS-hosted `.duckdb`,** pulled/pushed each run. |

### Residual item to confirm during implementation
- **Q-B — lost 2025 partitions:** confirm whether any historical year partitions were already
  destroyed by the table-scoped `replace` (§9) and, if so, whether they need re-backfilling before
  cut-over.

---

## References

- GeoPandas `to_parquet` — <https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_parquet.html>
- GeoParquet writing cookbook / cloud-native guide — <https://guide.cloudnativegeo.org/geoparquet/> ·
  <https://www.geomermaids.com/cookbook/geoparquet/>
- DuckDB native GeoParquet (1.1) write + Hilbert/bbox — <https://cloudnativegeo.org/blog/2025/01/using-duckdbs-hilbert-function-with-geoparquet/>
- DuckDB GeoParquet output discussion — <https://github.com/duckdb/duckdb/discussions/14274>
- dlt naming convention (snake_case rules) — <https://dlthub.com/docs/general-usage/naming-convention> ·
  <https://dlthub.com/docs/api_reference/dlt/common/normalizers/naming/snake_case>
</content>
</invoke>
