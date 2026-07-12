# stmsn-ingest Migration Guide: Retiring stmsn-dlt

**Status:** Draft for review · **Date:** 2026-07-11
**Builds on:** [`ref/geoparquet-migration-plan.md`](geoparquet-migration-plan.md) (all decisions there stand
unless amended below). That doc covers *why* and *what*; this one covers *where the code goes*, the
**testing framework** (new), and the cutover sequence for retiring this repo entirely.

**Confirmed direction:**
- New repo: **`stmsn-ingest`** — a generic ingestion repo, not dlt- or ArcGIS-branded.
- Write engine: **plain PyArrow / GeoPandas `to_parquet`**, **zstd** compression, GeoParquet 1.1 with
  covering bbox (per the prior plan).
- **Real-GCS integration testing** for the load/replace mechanism against an **ephemeral bucket** —
  this is the headline addition. A prod incident (dropped `year=2025` partition) passed local-filesystem
  testing and only failed on GCS; the test framework below is designed so that class of bug cannot ship
  again.

---

## 1. Why a new repo instead of refactoring in place

Retiring `stmsn-dlt` as a repo (not just dlt as a dependency) buys three things:

1. **A clean dependency floor.** No `dlt`, no `.dlt/` config dir, no `_dlt_*` sidecar fixtures in
   `test_output/`, no stale root-level `test_*.py` scripts. Everything in the new repo is intentional.
2. **A generic shape.** "ArcGIS pipeline" and "webfiles pipeline" become two *sources* feeding one
   shared write/catalog/validation core. Adding a third source (another jurisdiction's portal, a
   Socrata API, whatever) means writing a source module, not a new pipeline architecture.
3. **A testing reset.** stmsn-dlt has no test framework — pytest isn't even a dependency; the two root
   scripts are ad-hoc `__main__` files (one has a stale `MetricConfig(column=...)` API, one hits a
   hardcoded prod bucket). Starting fresh lets tests be first-class from commit one.

`stmsn-dlt` stays up (archived, read-only) as the reference for git history and the legacy scripts.

---

## 2. Target repo layout

```
stmsn-ingest/
├── pyproject.toml                  # uv-managed; src layout
├── src/stmsn_ingest/
│   ├── config.py                   # env-var settings (bucket, project) — replaces .dlt/secrets.toml
│   ├── layout.py                   # ★ single source of truth for every GCS path (§4)
│   ├── naming.py                   # dlt snake_case reimplementation (prior plan §8)
│   ├── io/
│   │   ├── writer.py               # write_geoparquet / write_parquet, zstd, temp-key+rename (§3)
│   │   └── gcs.py                  # one fsspec/gcsfs filesystem factory; nothing else touches GCS
│   ├── catalog/
│   │   └── catalog.py              # DuckLake integration: load_history + validation_results (§5)
│   ├── validation/
│   │   ├── metrics.py              # MetricConfig + MetricsAccumulator (ported from clients/arcgis.py)
│   │   └── validator.py            # DataValidator (ported; paths via layout.py)
│   ├── sources/
│   │   ├── arcgis/
│   │   │   ├── client.py           # ported clients/arcgis.py (pagination, WKB conversion)
│   │   │   └── metadata.py         # ported clients/arcgis_metadata.py
│   │   └── webfiles/
│   │       ├── client.py           # ported clients/webfile.py (discovery, case variations, download)
│   │       └── parsers.py          # ported utils/file_parsers.py
│   ├── pipelines/
│   │   ├── arcgis.py               # orchestration only: iterate → validate → write
│   │   └── webfiles.py             # orchestration only: discover → parse → write → catalog
│   └── cli.py                      # one entry point: `stmsn-ingest arcgis|webfiles [flags]`
├── config/
│   ├── arcgis_datasets.yaml        # copied as-is
│   └── webfiles_datasets.yaml      # copied as-is
├── tests/
│   ├── unit/                       # no network (§6.3)
│   ├── contract/                   # fsspec-backed writer/catalog tests on memory:// + local (§6.4)
│   └── gcs/                        # ★ real-GCS integration tests, ephemeral bucket (§6.5)
├── conftest.py                     # markers + ephemeral-bucket fixtures
└── .github/workflows/ci.yml        # unit+contract on PR; gcs suite on main/nightly (§6.7)
```

### 2.1 What moves, what's rewritten, what dies

| stmsn-dlt | stmsn-ingest | Change |
|---|---|---|
| `clients/arcgis.py` (`ArcGISClient`) | `sources/arcgis/client.py` | Port as-is; optionally add `convert_to="shapely"` later |
| `clients/arcgis.py` (`MetricConfig`, `MetricsAccumulator`) | `validation/metrics.py` | Port as-is (it never belonged in a client module) |
| `clients/arcgis_metadata.py` | `sources/arcgis/metadata.py` | Port; drop the "for dlt" docstring; metadata feeds the writer inline, no registry table |
| `clients/webfile.py` | `sources/webfiles/client.py` | Port as-is (discovery + case-variation logic is battle-tested) |
| `utils/file_parsers.py` | `sources/webfiles/parsers.py` | Port as-is (pure pandas) |
| `utils/data_validator.py` | `validation/validator.py` | Port; **all path strings come from `layout.py`**, not inline f-strings |
| `utils/load_history.py` | `catalog/catalog.py` | Rewritten: DuckLake table in the existing `stmsn.ducklake` catalog replaces the Parquet-glob history (§5) |
| `utils/geoparquet_finalizer.py` | — | **Deleted.** Single-pass writer eliminates it |
| `pipelines/arcgis.py` / `pipelines/webfiles.py` | `pipelines/*.py` + `cli.py` | Rewritten: dlt resources/destinations → validate-then-write orchestration |
| `.dlt/config.toml` + `.dlt/secrets.toml` | `config.py` (env vars) | `STMSN_BUCKET`, `STMSN_PROJECT_ID`; auth stays ADC |
| `spatial_metadata_registry` table | — | Dropped (prior plan §7.2) |
| `test_output/` fixtures, `test_validation.py`, `test_fixes.py` | `tests/` | Not ported; intent recreated as real pytest tests (§6) |
| `scripts/check_geoparquet.py` | `scripts/` (optional) | Handy for eyeballing output; already dlt-free |
| `ref/*.md` | `docs/` | Bring the migration docs along as design history |

**Dependencies:** `geopandas`, `pyarrow`, `shapely`, `pandas`, `duckdb` (+ the `ducklake` extension,
installed/loaded at runtime and version-pinned), `gcsfs`, `pyyaml`, `requests`, `openpyxl`, and
`stmsn-catalog` (the shared DuckLake sync helper, §5, as a `uv` git-tag dependency). Dev: `pytest`,
`pytest-cov`, `google-cloud-storage` (bucket admin for test fixtures — the CAS push lives in
`stmsn-catalog`; runtime reads stay on gcsfs). **No `dlt`.**

---

## 3. Write path (arrow/geopandas + zstd)

Unchanged in substance from the prior plan §4–5; restated here as the concrete module contract so tests
can be written against it.

```python
# src/stmsn_ingest/io/writer.py
import geopandas as gpd
import pandas as pd
from shapely import wkb

PARQUET_KWARGS = dict(compression="zstd", index=False)
GEOPARQUET_KWARGS = dict(**PARQUET_KWARGS, schema_version="1.1.0", write_covering_bbox=True)

def write_geoparquet(records, crs, target_uri, *, fs) -> str:
    """records → GeoParquet at target_uri via temp-key + rename. Returns final URI."""
    gdf = gpd.GeoDataFrame(records)
    gdf["geometry"] = gdf["geometry"].map(
        lambda g: wkb.loads(bytes.fromhex(g)) if isinstance(g, str) else g
    )
    gdf = gdf.set_geometry("geometry").set_crs(crs)
    return _atomic_write(gdf, target_uri, fs, GEOPARQUET_KWARGS)

def write_parquet(df: pd.DataFrame, target_uri, *, fs) -> str:
    """Plain DataFrame → Parquet (webfiles, non-spatial ArcGIS tables)."""
    return _atomic_write(df, target_uri, fs, PARQUET_KWARGS)

def _atomic_write(frame, target_uri, fs, kwargs) -> str:
    tmp_uri = f"{target_uri}.tmp-{uuid4().hex[:8]}"
    frame.to_parquet(tmp_uri, filesystem=fs, **kwargs)
    try:
        fs.mv(tmp_uri, target_uri)          # GCS: server-side copy + delete
    except Exception:
        fs.rm(tmp_uri, recursive=False)     # never leave a temp object behind
        raise
    return target_uri
```

Two deliberate choices worth calling out:

- **`fs` is an explicit parameter** (an fsspec filesystem), not derived from the URI scheme inside the
  writer. This is what makes the contract tests (§6.4) possible: the same writer code runs against
  `MemoryFileSystem`, `LocalFileSystem`, and `GCSFileSystem` with zero branching. Production passes the
  singleton from `io/gcs.py`.
- **Temp-key + `mv`** rather than write-in-place. On GCS `mv` is copy-then-delete (not atomic), but for
  a single monthly writer it guarantees the previous good `data.parquet` survives any mid-write crash.
  The failure-injection test in §6.5 pins this behavior.

**Standardize on gcsfs everywhere.** Today three GCS access styles coexist (pyarrow `GcsFileSystem` in
the finalizer, DuckDB-registered gcsfs in load_history/validator, raw fsspec in the validator's
existence checks). In stmsn-ingest, `io/gcs.py` exposes one `get_fs()` and everything — writer, catalog
sync, validator globs — goes through it. DuckDB continues to `register_filesystem(get_fs())` for reads.

---

## 4. `layout.py`: one module owns every GCS path

This is the biggest improvement over the prior plan, and it's motivated by the incident history. All
three production bugs in this repo were **path-layout mismatches between writer and reader**:

1. `write_disposition="replace"` was table-scoped, not partition-scoped → a 2026 run deleted `year=2025/`.
2. The load-history reader globbed `/load_history/*.parquet` while dlt wrote nested load packages →
   `**` was needed, history reads silently returned empty, and everything re-loaded.
3. The reader globbed `{pipeline}/load_history/` while dlt wrote `{pipeline}/{jurisdiction}/load_history/`
   → same silent-empty failure (fixed in commit 586d614).

And path shapes are *still* inconsistent in stmsn-dlt: webfiles writes under `{pipeline}/{jurisdiction}/…`,
arcgis under `arcgis/{jurisdiction}/…`, and the validator queries `bronze/arcgis/{jurisdiction}/…`. Each
call site builds its own f-string, so writer and reader can drift independently — that's the root cause,
and dlt's opaque layout engine made it worse, but removing dlt doesn't remove the risk by itself.

The fix is structural: **no module except `layout.py` ever concatenates a GCS path.**

```python
# src/stmsn_ingest/layout.py
BRONZE = "bronze"

def dataset_partition_uri(bucket, source, jurisdiction, dataset, year) -> str:
    """The one true location of a yearly snapshot. Writer and validator both call this."""
    return f"gs://{bucket}/{BRONZE}/{source}/{jurisdiction}/{dataset}/year={year}/data.parquet"

def dataset_glob(bucket, source, jurisdiction, dataset, year=None) -> str:
    part = f"year={year}" if year else "year=*"
    return f"gs://{bucket}/{BRONZE}/{source}/{jurisdiction}/{dataset}/{part}/*.parquet"

def catalog_uri(meta_bucket: str = "stmsn-meta") -> str:
    """The existing DuckLake catalog — shared with the dbt project (§5)."""
    return f"gs://{meta_bucket}/catalog/stmsn.ducklake"
```

Because the writer and every reader (validator baseline queries, catalog sync, any future dbt source
definition) call the same functions, writer/reader drift becomes a compile-time impossibility rather
than a prod incident. Unit tests pin the exact strings (§6.3) so any deliberate layout change shows up
as a loud test diff, and the GCS integration suite (§6.5) verifies the round trip — *what layout.py
says gets written is what globbing actually finds*.

Note the deterministic object name (`data.parquet`, not dlt's hashed `f39c7aa42c.parquet`): "replace
year 2026" is literally "overwrite this one object," and no operation in the codebase enumerates or
deletes sibling partitions. Bug #1 becomes unwritable.

---

## 5. Bookkeeping → the existing DuckLake catalog

**Amends prior plan §7.1.** Since that plan was written, the dbt project moved to a **DuckLake
catalog** — single writer to `gs://stmsn-meta/catalog/stmsn.ducklake` with a pull/write/push
mechanism. That is mechanically the same sync pattern §7.1 proposed for a standalone `.duckdb`, so we
**do not create a second catalog**: `load_history` and `validation_results` become DuckLake tables in
a dedicated `ingest` schema of the existing catalog.

```python
con.execute("INSTALL ducklake; LOAD ducklake;")
# pull stmsn.ducklake from gs://stmsn-meta/catalog/ to a local temp path, then:
con.execute(f"ATTACH 'ducklake:{local_path}' AS lake")   # DATA_PATH is stored in the catalog
con.execute("CREATE SCHEMA IF NOT EXISTS lake.ingest")
con.execute("INSERT INTO lake.ingest.load_history VALUES (...)")
# push the file back
```

Notes on the integration:

- **Shared sync helper → its own tiny repo (`stmsn-catalog`).** The pull/write/push mechanism
  currently lives in the dbt repo; with two writer repos needing bit-identical semantics in the one
  place where drift means silent data clobbering, it gets extracted into a minimal helper repo that
  both consume as a `uv` git-tag dependency (`stmsn-catalog @ git+…@v0.1.0` — no PyPI needed).
  Scope: **transport and locking only** — pull with generation capture, attach, CAS push
  (`if_generation_match`), retry policy, exposed as a `catalog_session(...)` context manager that
  raises `ConcurrentWriteError` on 412. Table DDL stays with the repo that owns the tables (`ingest`
  schema here, dbt's tables there). Sequencing: extract from the dbt repo and point dbt at it
  *first*, so the helper is proven against existing usage before stmsn-ingest builds on it.
  `catalog.py` then wraps it with a `pending`/`commit` pattern replacing
  `LoadHistoryManager.record_load/clear_pending`.
- **What it buys:** one catalog and one sync path; dbt models can query `lake.ingest.load_history` /
  `lake.ingest.validation_results` natively; DuckLake snapshots give a free time-travel audit trail of
  every ingest run.
- **Small-file accretion:** each monthly `INSERT` normally writes a tiny Parquet file to the lake's
  data path. Prefer **data inlining** (`DATA_INLINING_ROW_LIMIT` on attach) so bookkeeping rows live
  inside the catalog file itself (caveat: newer feature — verify on the pinned DuckLake version);
  fallback is accepting small files plus occasional `ducklake_merge_adjacent_files()` /
  `ducklake_expire_snapshots()`.
- **Two writers, one file (the real constraint):** ingest and dbt now both pull/mutate/push
  `stmsn.ducklake`. Last-push-wins would silently drop one side's changes — which would resurrect the
  "history reads empty, everything re-loads" failure mode. Defense in depth: (a) **serialize by
  orchestration** — ingest is monthly; schedule it strictly outside dbt runs; (b) **optimistic locking
  on push** — record the object `generation` at pull time and push with `if_generation_match`; a
  concurrent write becomes a loud HTTP 412 (re-pull, re-apply, retry) instead of a silent clobber.
  Adding (b) to the shared helper protects the dbt side too.
- **Scope boundary:** the bronze GeoParquet files stay **outside** DuckLake *for this migration*.
  DuckLake does support geometry now (since 0.3, with `GEOMETRY` built into DuckDB 1.5), so this is a
  scoping call, not a capability gap: (a) DuckLake writes the new *native Parquet GEOMETRY logical
  types* ("GeoParquet 2.0" direction) while geopandas/QGIS/GDAL are mid-transition and read
  GeoParquet 1.1 most reliably today — direct `to_parquet(schema_version="1.1.0")` keeps maximum
  interop; (b) lake-managed files have opaque catalog-assigned names, forfeiting `layout.py`'s
  deterministic paths for external consumers; (c) every bronze write would add pull/push contention
  on the single-writer catalog; (d) the T1–T5 test suite pins object-level replace semantics.
  **Phase 2 candidate:** lake-managed bronze would give transactional per-year replace and time
  travel (the 2025 partition loss would have been recoverable) — revisit once native-geometry
  Parquet reading is the ecosystem default and DuckLake geometry has a few more releases behind it.
- **Migrate existing history before cutover.** One-time script: read the current
  `gcs://{bucket}/webfiles/{jurisdiction}/load_history/**/*.parquet` glob (the post-586d614 shape)
  into `lake.ingest.load_history`. Run it while stmsn-dlt still exists so the old reader can
  cross-check row counts.

Validation flow is unchanged (buffer → `MetricsAccumulator` → `DataValidator` vs. GCS baseline → write
only on pass), just with paths from `layout.py` and results inserted into the lake.

---

## 6. Testing framework (new)

### 6.1 The lesson from prod

The `year=2025` loss and both load-history bugs shared a signature: **the code was "tested" against a
local filesystem (or not at all), and the failure only existed in the GCS-shaped world** — dlt's actual
object layout, recursive-glob behavior over object-store prefixes, and table-scoped delete semantics.
Local paths are not a faithful model of object storage: GCS has no directories, no atomic rename,
different listing semantics, and (in dlt's case) a different physical layout than the local destination
produced. Any test strategy that stops at `tmp_path` will re-create this blind spot.

So the framework has three tiers, and the rule is: **the load/replace mechanism and every path/glob
round trip must pass against real GCS before release.** Emulators (fake-gcs-server) are deliberately
*not* in the plan — an emulator is a second approximation of GCS semantics, and approximation error is
exactly what bit us. With one small ingest project, the real thing is cheap enough to test against
directly.

### 6.2 Stack

- **pytest** with markers declared in `pyproject.toml`:

  ```toml
  [tool.pytest.ini_options]
  markers = [
      "gcs: hits real GCS; needs ADC + STMSN_TEST_PROJECT; creates an ephemeral bucket",
  ]
  addopts = "-m 'not gcs'"          # default run is offline; opt in with: pytest -m gcs
  ```

- **gcsfs** for all runtime I/O (as in prod); **google-cloud-storage** as a dev-only dependency for
  bucket create/delete in fixtures (gcsfs can't set lifecycle rules cleanly).
- Directory = tier: `tests/unit/`, `tests/contract/`, `tests/gcs/`.

### 6.3 Tier 1 — unit (offline, milliseconds, run constantly)

- `naming.py`: table-driven tests for the snake_case rules, seeded from the **dlt parity dump** —
  before retiring stmsn-dlt, run dlt's `NamingConvention` over every observed ArcGIS/webfile field name
  (prior plan §8) and commit the `(raw, normalized)` pairs as a fixture file. The new repo then asserts
  parity forever without depending on dlt.
- `layout.py`: pin exact URI strings for every function. Boring, and the point — a layout change must
  be a visible, reviewed diff.
- `parsers.py`, `MetricsAccumulator`, `DataValidator.compare_metrics` (tolerance math), webfile
  discovery pattern/case-variation logic with canned HTML.

### 6.4 Tier 2 — contract tests (offline, fsspec-backed, run on every PR)

Because the writer and catalog take an explicit `fs`, the full write/replace/read flow runs against
fsspec's `MemoryFileSystem` (and `LocalFileSystem` via `tmp_path`). These tests verify the *logic* —
GeoParquet metadata, CRS, bbox column, zstd, temp-key cleanup on failure, catalog insert/query — cheaply
and deterministically:

```python
def test_writer_roundtrip_memory_fs(memory_fs, sample_gdf):
    uri = write_geoparquet(sample_records, "EPSG:8193", "memory://b/year=2026/data.parquet", fs=memory_fs)
    out = gpd.read_parquet(uri, filesystem=memory_fs)
    assert out.crs.to_epsg() == 8193
    assert "bbox" in pq_schema_names(uri, memory_fs)      # covering bbox present
```

Contract tests are necessary but explicitly **not sufficient** — they share the writer's fsspec
abstraction, so they can't catch GCS-specific semantics. That's tier 3's job.

### 6.5 Tier 3 — real-GCS integration against an ephemeral bucket ★

**Fixture design:** a session-scoped pytest fixture creates a uniquely named bucket, and teardown
force-deletes it. A lifecycle rule (delete objects after 1 day) is set at creation as a safety net so a
crashed run can't leak storage; a `try/finally` plus a name prefix (`stmsn-ingest-test-`) makes leaked
buckets easy to spot and sweep.

```python
# conftest.py
import os, uuid, pytest
from google.cloud import storage
from google.cloud.storage.bucket import LifecycleRuleDelete

@pytest.fixture(scope="session")
def ephemeral_bucket():
    project = os.environ["STMSN_TEST_PROJECT"]          # fail fast if unset
    client = storage.Client(project=project)
    name = f"stmsn-ingest-test-{uuid.uuid4().hex[:12]}"
    bucket = client.bucket(name)
    bucket.lifecycle_rules = [LifecycleRuleDelete(age=1)]   # leak insurance
    client.create_bucket(bucket, location="us-central1")
    try:
        yield name
    finally:
        bucket.delete(force=True)                        # force: deletes remaining objects

@pytest.fixture()
def gcs_fs():
    import gcsfs
    return gcsfs.GCSFileSystem()                         # ADC, same as prod
```

(If per-session bucket creation ever becomes a permissions problem, the fallback is one long-lived
`stmsn-ingest-test` bucket with the same lifecycle rule and a per-session `run-{uuid}/` prefix — same
tests, one less IAM grant. Ephemeral-bucket is the primary design because it also exercises a truly
cold bucket: no stale objects, no leftover generations.)

**The test scenarios.** Each one is a bug class this project has actually hit, or the mechanism that
prevents it:

| # | Test | What it pins down |
|---|------|-------------------|
| **T1** | `test_replace_year_preserves_siblings` — seed `year=2025/data.parquet` and `year=2026/data.parquet` via the real writer; run the pipeline's replace path for 2026 with new data; assert 2026 content changed **and 2025 is byte-identical** (compare CRC32C/generation via the storage client). | **The prod incident.** Replace must be partition-scoped by construction; this test fails loudly if any future change re-introduces table-scoped deletion. |
| **T2** | `test_atomic_write_failure_leaves_prior_object` — seed a good `data.parquet`; monkeypatch the frame's `to_parquet` (or `fs.mv`) to raise mid-operation; assert the final object is unchanged and **no `*.tmp-*` object remains** in the prefix. | Temp-key + rename crash safety, on real GCS copy+delete semantics rather than a local `os.rename`. |
| **T3** | `test_overwrite_is_single_object` — write the same partition URI twice; list the prefix; assert exactly one object with the second run's content and a bumped generation. | Deterministic naming: "replace" is an overwrite, never an accumulation of hashed files. |
| **T4** | `test_layout_glob_roundtrip` — write partitions for two datasets × two years through `layout.dataset_partition_uri`; assert `fs.glob(layout.dataset_glob(...))` finds exactly the expected keys, per-year and cross-year. | The `*` vs `**` / missing-jurisdiction bug class: what we write is what our own globs find, on real GCS listing. |
| **T5** | `test_geoparquet_readable_from_gcs` — write real geometries; `gpd.read_parquet("gs://…")` and a DuckDB `read_parquet` over the partition glob; assert CRS, geometry type, bbox column, row count. | Downstream (validator, dbt) can consume the files exactly as written; GeoParquet metadata survives the GCS round trip. |
| **T6** | `test_catalog_pull_insert_push_cycle` — fresh bucket has no catalog: first attach bootstraps an ephemeral DuckLake catalog (metadata + data path both in the test bucket); insert a `load_history` row; push; a **new** catalog instance pulls and `is_already_loaded` returns True; run discovery-skip logic against it. Also assert the `if_generation_match` push fails loudly (412) when the object changed under it. | The load-history mechanism end-to-end on real GCS — the surface behind two of the three prod bugs — plus the bootstrap path and the concurrent-writer guard from §5, neither of which a long-lived dev catalog ever exercises. |
| **T7** | `test_validator_baseline_from_gcs` — write a year-2025 baseline through the writer; run `DataValidator.query_baseline_metrics` + `validate_dataset` for a synthetic 2026 pull (one passing, one tolerance-violating). | Validation gate reads the writer's real output via `layout.py` globs — replaces the old `test_fixes.py` "GCS auth" script with a meaningful assertion. |
| **T8** | `test_full_pipeline_dry_run_vs_gcs` *(optional, slower)* — run the webfiles pipeline end-to-end against a local fixture HTTP file with the bucket pointed at the ephemeral bucket; run it twice; assert the second run skips everything via the catalog. | Idempotency of a whole run — the property `--force` exists to override. |

Runtime for T1–T7 is dominated by bucket create/delete (~a few seconds each) and small-object I/O;
the whole suite should stay under ~2 minutes and costs effectively nothing (objects are kilobytes and
live for minutes).

### 6.6 Making the pipeline testable enough for T1/T8

One refactor requirement falls out of the test plan: `run_pipeline(...)` must accept **bucket and
filesystem injection** (today the bucket comes from `dlt.secrets` deep inside). In stmsn-ingest,
`config.py` reads env vars once at the CLI boundary and everything below takes explicit
`bucket`/`fs` arguments — so tests point the *unmodified production code path* at the ephemeral
bucket by passing parameters, not by monkeypatching globals. This also cleanly replaces the old
`--local-output` plumbing: `--dry-run` becomes "local tmp path + 1000-row sample" (prior plan §9)
via the same parameters.

### 6.7 CI wiring

- **Every PR:** `pytest` (default `-m 'not gcs'`) — unit + contract, no credentials, seconds.
- **Main / nightly / pre-release:** `pytest -m gcs` in a GitHub Actions job authenticated via
  **Workload Identity Federation** (`google-github-actions/auth`) to a dedicated test project/service
  account whose IAM is scoped to `storage.buckets.create/delete` + object admin on
  `stmsn-ingest-test-*`. No long-lived keys in secrets.
- **Local:** `gcloud auth application-default login`, `export STMSN_TEST_PROJECT=…`, `pytest -m gcs`.
  Document this in the new repo's README — the gcs suite must be trivially runnable from a laptop,
  because "I can reproduce the prod behavior locally against real GCS" is the whole point.
- Since the pipeline is monthly, a **nightly `-m gcs` schedule** gives ~30 real-GCS verifications
  between production runs — regressions surface weeks before they can touch prod data.

---

## 7. Cutover sequence

Ordering refined from prior plan §11, now framed as building `stmsn-ingest` rather than editing in place:

1. **In stmsn-dlt (while dlt is still installed):** run the naming-parity dump (§6.3) over the real
   ArcGIS/webfile field sets and export the fixture file. Also resolve **Q-B**: audit GCS for missing
   historical `year=` partitions and re-backfill from source/legacy copies if needed.
2. **Scaffold stmsn-ingest:** pyproject (uv, src layout, pytest config), `config.py`, `layout.py` +
   unit tests, `naming.py` + parity fixture, CI skeleton with both jobs.
3. **`io/writer.py` + `io/gcs.py`** with contract tests (tier 2), then the ephemeral-bucket fixture and
   **T1–T5**. Getting T1 green early is deliberate: the load/replace guarantee exists before any
   pipeline code depends on it.
4. **Extract `stmsn-catalog`** from the dbt repo (pull/push + `if_generation_match` CAS, §5); point
   the dbt project at it and verify a normal dbt cycle — the helper is proven before ingest uses it.
5. **`catalog/catalog.py`** (DuckLake `ingest` schema on top of `stmsn-catalog`) + T6; run the
   one-time load-history migration script; cross-check counts against the old reader.
6. **Port sources and validation** (`arcgis`, `webfiles`, `parsers`, `metrics`, `validator`) with their
   unit tests; T7.
7. **Pipelines + CLI** (`stmsn-ingest arcgis --datasets … --dry-run`, `stmsn-ingest webfiles --force …`),
   preserving today's flags; optional T8.
8. **Parallel-run one monthly cycle:** run stmsn-ingest against a staging prefix (or the test bucket),
   diff outputs against the stmsn-dlt production run (row counts, validator metrics, schema, CRS).
9. **Cut over:** point the production schedule at stmsn-ingest; first prod run writes the current-year
   partitions; confirm prior years untouched (same check as T1, run against prod read-only).
10. **Retire stmsn-dlt:** final README pointer to stmsn-ingest, archive the repo on GitHub. Clean up
   dlt's leftovers in the prod bucket (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state` dirs, the
   old `spatial_metadata_registry` and Parquet `load_history` tables) **after** a bucket-level backup
   or once the parallel-run diff is signed off.

---

## 8. Suggestions beyond the original plan (summary)

1. **`layout.py` as the single path authority** (§4) — the structural fix for the bug class behind all
   three prod incidents; the prior plan fixed the instances, this fixes the category.
2. **Explicit `fs` injection through writer/catalog/pipeline** (§3, §6.6) — what makes tier-2 tests
   possible and lets tier-3 tests exercise unmodified production code against the ephemeral bucket.
3. **Skip emulators; test against real GCS** (§6.1) — fake-gcs-server would re-introduce an
   approximation layer; at this project's scale the real thing is cheap, fast enough, and authoritative.
4. **Nightly real-GCS CI** (§6.7) — for a monthly pipeline, scheduled integration runs are the early-
   warning system; don't wait for the production run to discover a regression.
5. **Commit the dlt parity fixture, not a dlt dependency** (§6.3) — parity with dlt's snake_case is
   verified forever without dlt ever appearing in stmsn-ingest's lockfile.
6. **Unify on gcsfs** (§3) — retire the three-way pyarrow-GcsFileSystem / DuckDB-gcsfs / raw-fsspec
   split; one filesystem factory, DuckDB registers it.
7. **Byte-identity assertion for untouched partitions** (T1) — "preserves other years" is checked by
   CRC32C/generation, not by "the file still exists"; a rewrite-with-same-name regression can't hide.
8. **Bootstrap-path testing** (T6) — the empty ephemeral bucket naturally covers first-run behavior
   (no catalog yet, no baseline yet), which a long-lived dev bucket never exercises.
