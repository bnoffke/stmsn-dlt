"""
Load history manager for tracking processed files.

Tracks which files have been loaded to prevent duplicate processing
and enable incremental loading.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Set

import duckdb


@dataclass
class LoadHistoryRecord:
    """Record of a loaded file."""

    dataset_name: str
    jurisdiction: str
    source_url: str
    filename: str
    partition_values: str  # JSON string of partition dict
    loaded_at: str  # ISO format timestamp
    row_count: int
    file_hash: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for dlt resource."""
        return asdict(self)


class LoadHistoryManager:
    """
    Manages load history for webfiles pipeline.

    Tracks which files have been processed to enable incremental loading
    and prevent duplicate processing.
    """

    def __init__(self, gcs_bucket: str, pipeline_name: str = "webfiles"):
        """
        Initialize the load history manager.

        Args:
            gcs_bucket: GCS bucket name (e.g., "stmsn-bronze")
            pipeline_name: Name of the pipeline (default: "webfiles")
        """
        self.gcs_bucket = gcs_bucket
        self.pipeline_name = pipeline_name
        self._loaded_urls: Set[str] = set()
        self._pending_records: List[LoadHistoryRecord] = []
        self._jurisdiction: Optional[str] = None
        self._dataset_name: Optional[str] = None

    def _init_duckdb_gcs(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Initialize DuckDB with GCS support using fsspec/gcsfs.

        Args:
            conn: DuckDB connection
        """
        from fsspec import filesystem

        fs = filesystem("gcs")
        conn.register_filesystem(fs)

    def _get_history_path(self) -> str:
        """Get GCS path to load history parquet files."""
        return f"gcs://{self.gcs_bucket}/{self.pipeline_name}/load_history/**/*.parquet"

    def load_existing_history(
        self,
        jurisdiction: Optional[str] = None,
        dataset_name: Optional[str] = None,
    ) -> None:
        """
        Load existing load history from GCS.

        Args:
            jurisdiction: Optional filter by jurisdiction
            dataset_name: Optional filter by dataset name
        """
        # Store jurisdiction and dataset for use by other methods
        self._jurisdiction = jurisdiction
        self._dataset_name = dataset_name

        # Build path with jurisdiction (DLT writes to {bucket}/{pipeline}/{dataset}/table_name/)
        if jurisdiction:
            history_path = f"gcs://{self.gcs_bucket}/{self.pipeline_name}/{jurisdiction}/load_history/**/*.parquet"
        else:
            history_path = self._get_history_path()

        try:
            conn = duckdb.connect(":memory:")
            self._init_duckdb_gcs(conn)

            # Build query with optional filters
            where_clauses = []
            if jurisdiction:
                where_clauses.append(f"jurisdiction = '{jurisdiction}'")
            if dataset_name:
                where_clauses.append(f"dataset_name = '{dataset_name}'")

            where_sql = ""
            if where_clauses:
                where_sql = "WHERE " + " AND ".join(where_clauses)

            query = f"""
                SELECT DISTINCT source_url
                FROM read_parquet('{history_path}')
                {where_sql}
            """

            result = conn.execute(query).fetchall()
            self._loaded_urls = {row[0] for row in result}

            print(f"  Loaded {len(self._loaded_urls)} URLs from history")

        except duckdb.IOException as e:
            # No history exists yet - this is fine
            if "No files found" in str(e) or "Unable to connect" in str(e):
                print("  No existing load history found (first run)")
                self._loaded_urls = set()
            else:
                raise
        except Exception as e:
            # Handle other errors gracefully
            print(f"  Warning: Could not load history: {e}")
            self._loaded_urls = set()

    def is_already_loaded(self, url: str) -> bool:
        """
        Check if a URL has already been loaded.

        Args:
            url: URL to check

        Returns:
            True if URL is in load history
        """
        return url in self._loaded_urls

    def get_loaded_partition_values(self, partition_field: str = "year") -> Set[str]:
        """
        Get set of partition values that have already been loaded.

        Useful for skipping years during discovery to avoid unnecessary probing.

        Args:
            partition_field: Partition field to extract (default: "year")

        Returns:
            Set of partition values (e.g., {"2024", "2023", "2022"})
        """
        import json

        # If no history loaded yet, return empty set
        if not self._loaded_urls:
            return set()

        # Query history to get partition values
        try:
            conn = duckdb.connect(":memory:")
            self._init_duckdb_gcs(conn)

            # Use the same path logic as load_existing_history
            if self._jurisdiction:
                history_path = f"gcs://{self.gcs_bucket}/{self.pipeline_name}/{self._jurisdiction}/load_history/**/*.parquet"
            else:
                history_path = self._get_history_path()

            query = f"""
                SELECT DISTINCT partition_values
                FROM read_parquet('{history_path}')
            """

            result = conn.execute(query).fetchall()

            # Parse JSON partition_values and extract the requested field
            partition_set = set()
            for row in result:
                partition_json = row[0]
                try:
                    partition_dict = json.loads(partition_json)
                    if partition_field in partition_dict:
                        partition_set.add(partition_dict[partition_field])
                except (json.JSONDecodeError, TypeError):
                    continue

            return partition_set

        except Exception as e:
            # If we can't load partition values, return empty set
            print(f"  Warning: Could not load partition values from history: {e}")
            return set()

    def filter_unloaded(self, urls: List[str]) -> List[str]:
        """
        Filter a list of URLs to only those not yet loaded.

        Args:
            urls: List of URLs to filter

        Returns:
            List of URLs not in load history
        """
        return [url for url in urls if url not in self._loaded_urls]

    def record_load(
        self,
        dataset_name: str,
        jurisdiction: str,
        source_url: str,
        filename: str,
        partition_values: Dict[str, str],
        row_count: int,
        file_hash: Optional[str] = None,
    ) -> LoadHistoryRecord:
        """
        Record a successful file load.

        Args:
            dataset_name: Name of the dataset
            jurisdiction: Jurisdiction name
            source_url: URL that was loaded
            filename: Original filename
            partition_values: Dict of partition field to value
            row_count: Number of rows loaded
            file_hash: Optional MD5 hash of source file

        Returns:
            LoadHistoryRecord that was created
        """
        import json

        record = LoadHistoryRecord(
            dataset_name=dataset_name,
            jurisdiction=jurisdiction,
            source_url=source_url,
            filename=filename,
            partition_values=json.dumps(partition_values),
            loaded_at=datetime.now().isoformat(),
            row_count=row_count,
            file_hash=file_hash,
        )

        self._pending_records.append(record)
        self._loaded_urls.add(source_url)

        return record

    def get_pending_records(self) -> List[LoadHistoryRecord]:
        """
        Get all pending load history records.

        Returns:
            List of LoadHistoryRecord objects to be written
        """
        return self._pending_records

    def clear_pending(self) -> None:
        """Clear pending records after successful write."""
        self._pending_records = []

    def get_history_for_dataset(
        self,
        jurisdiction: str,
        dataset_name: str,
    ) -> List[Dict]:
        """
        Get full load history for a specific dataset.

        Args:
            jurisdiction: Jurisdiction name
            dataset_name: Dataset name

        Returns:
            List of history records as dicts
        """
        history_path = f"gcs://{self.gcs_bucket}/{self.pipeline_name}/{jurisdiction}/load_history/**/*.parquet"

        try:
            conn = duckdb.connect(":memory:")
            self._init_duckdb_gcs(conn)

            query = f"""
                SELECT *
                FROM read_parquet('{history_path}')
                WHERE jurisdiction = '{jurisdiction}'
                  AND dataset_name = '{dataset_name}'
                ORDER BY loaded_at DESC
            """

            result = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]

            return [dict(zip(columns, row)) for row in result]

        except Exception:
            return []
