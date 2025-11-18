"""
Data validation utility for comparing incoming data against existing baseline.

Queries existing parquet data from GCS using DuckDB and compares metrics
with incoming data to validate data quality before loading.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
import duckdb


@dataclass
class ValidationResult:
    """Result of a validation check."""
    passed: bool
    metric_name: str
    baseline_value: Optional[float]
    incoming_value: Optional[float]
    percent_difference: Optional[float]
    tolerance_percent: float
    error_message: Optional[str] = None

    def __str__(self):
        if self.passed:
            return (
                f"✓ {self.metric_name}: {self.incoming_value:.2f} "
                f"(baseline: {self.baseline_value:.2f}, "
                f"diff: {self.percent_difference:.1f}%)"
            )
        else:
            return (
                f"✗ {self.metric_name}: {self.error_message or 'FAILED'}\n"
                f"  Baseline: {self.baseline_value}\n"
                f"  Incoming: {self.incoming_value}\n"
                f"  Difference: {self.percent_difference:.1f}% "
                f"(tolerance: ±{self.tolerance_percent}%)"
            )


class DataValidator:
    """
    Validates incoming data against existing baseline data in GCS.

    Uses DuckDB to query parquet files from GCS and compare metrics
    with incoming data collected during fetch.
    """

    def __init__(
        self,
        gcs_bucket: str,
        default_tolerance_percent: float = 5.0,
    ):
        """
        Initialize the data validator.

        Uses gcloud application-default credentials or GOOGLE_APPLICATION_CREDENTIALS
        environment variable for GCS authentication.

        Args:
            gcs_bucket: GCS bucket name (e.g., "your-bucket")
            default_tolerance_percent: Default tolerance for metric comparisons (default: 5%)
        """
        self.gcs_bucket = gcs_bucket
        self.default_tolerance_percent = default_tolerance_percent

    def _init_duckdb_gcs(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Initialize DuckDB with GCS support using fsspec/gcsfs.

        This method uses the fsspec filesystem abstraction with gcsfs,
        which automatically uses gcloud application-default credentials
        or GOOGLE_APPLICATION_CREDENTIALS environment variable.

        Args:
            conn: DuckDB connection
        """
        from fsspec import filesystem

        # Register gcsfs filesystem for GCS access
        # This automatically uses gcloud auth credentials
        fs = filesystem('gcs')
        conn.register_filesystem(fs)

    def _check_baseline_exists(
        self,
        jurisdiction: str,
        dataset_name: str,
        year: Optional[int] = None,
    ) -> bool:
        """
        Check if baseline data exists in GCS before attempting validation.

        Args:
            jurisdiction: Jurisdiction name
            dataset_name: Dataset name
            year: Optional year to check

        Returns:
            True if baseline data exists, False otherwise
        """
        from fsspec import filesystem

        fs = filesystem('gcs')

        # Build path to check
        if year:
            path = f"{self.gcs_bucket}/bronze/arcgis/{jurisdiction}/{dataset_name}/year={year}/"
        else:
            path = f"{self.gcs_bucket}/bronze/arcgis/{jurisdiction}/{dataset_name}/"

        try:
            # Check if the path exists and contains any files
            if not fs.exists(path):
                return False

            # Check if there are any parquet files
            files = fs.glob(f"{path}**/*.parquet")
            return len(files) > 0

        except Exception:
            # If there's any error checking, assume it doesn't exist
            return False

    def query_baseline_metrics(
        self,
        jurisdiction: str,
        dataset_name: str,
        metrics: List[Dict[str, str]],
        year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Query baseline metrics from existing GCS parquet data.

        Args:
            jurisdiction: Jurisdiction name (e.g., "madison")
            dataset_name: Dataset name (e.g., "parcels")
            metrics: List of metric configurations (e.g., [{"name": "total_taxes", "aggregate": "sum", "parquet_column": "total_taxes"}])
            year: Optional year to query (defaults to most recent)

        Returns:
            Dictionary with metric names and their baseline values, including 'row_count'

        Raises:
            FileNotFoundError: If no baseline data exists
            Exception: For other errors querying baseline data
        """
        conn = duckdb.connect(":memory:")
        try:
            self._init_duckdb_gcs(conn)

            # Build GCS path pattern
            if year:
                path_pattern = f"gs://{self.gcs_bucket}/bronze/arcgis/{jurisdiction}/{dataset_name}/year={year}/*.parquet"
            else:
                # Query all years and get most recent
                path_pattern = f"gs://{self.gcs_bucket}/bronze/arcgis/{jurisdiction}/{dataset_name}/**/*.parquet"

            # Build SELECT clause for metrics
            metric_clauses = ["COUNT(*) as row_count"]
            for metric in metrics:
                agg = metric["aggregate"].upper()
                col = metric["parquet_column"]  # Use normalized column name from parquet
                name = metric["name"]
                metric_clauses.append(f"{agg}({col}) as {name}")

            select_clause = ", ".join(metric_clauses)

            # Query baseline metrics
            query = f"""
                SELECT {select_clause}
                FROM read_parquet('{path_pattern}')
            """

            print(f"Querying baseline metrics from: {path_pattern}")
            result = conn.execute(query).fetchone()

            if result is None:
                raise FileNotFoundError(
                    f"No baseline data found at {path_pattern}"
                )

            # Build results dictionary
            results = {"row_count": result[0]}
            for i, metric in enumerate(metrics):
                results[metric["name"]] = result[i + 1]

            print(f"Baseline metrics: {results}")
            return results

        except Exception as e:
            print(f"Error querying baseline metrics: {e}")
            raise
        finally:
            conn.close()

    def compare_metrics(
        self,
        baseline_metrics: Dict[str, Any],
        incoming_metrics: Dict[str, Any],
        metric_configs: List[Dict[str, Any]],
    ) -> List[ValidationResult]:
        """
        Compare baseline and incoming metrics with tolerance checking.

        Args:
            baseline_metrics: Metrics from existing GCS data
            incoming_metrics: Metrics from incoming data
            metric_configs: Metric configurations with optional tolerance overrides

        Returns:
            List of ValidationResult objects for each metric
        """
        results = []

        # Always check row count first
        baseline_count = baseline_metrics.get("row_count", 0)
        incoming_count = incoming_metrics.get("row_count", 0)

        if baseline_count == 0:
            results.append(
                ValidationResult(
                    passed=False,
                    metric_name="row_count",
                    baseline_value=baseline_count,
                    incoming_value=incoming_count,
                    percent_difference=None,
                    tolerance_percent=self.default_tolerance_percent,
                    error_message="Baseline has 0 rows",
                )
            )
        else:
            pct_diff = abs(incoming_count - baseline_count) / baseline_count * 100
            passed = pct_diff <= self.default_tolerance_percent

            results.append(
                ValidationResult(
                    passed=passed,
                    metric_name="row_count",
                    baseline_value=baseline_count,
                    incoming_value=incoming_count,
                    percent_difference=pct_diff,
                    tolerance_percent=self.default_tolerance_percent,
                )
            )

        # Check custom metrics
        for metric_config in metric_configs:
            metric_name = metric_config["name"]
            tolerance = metric_config.get(
                "tolerance_percent", self.default_tolerance_percent
            )

            baseline_val = baseline_metrics.get(metric_name)
            incoming_val = incoming_metrics.get(metric_name)

            if baseline_val is None or incoming_val is None:
                results.append(
                    ValidationResult(
                        passed=False,
                        metric_name=metric_name,
                        baseline_value=baseline_val,
                        incoming_value=incoming_val,
                        percent_difference=None,
                        tolerance_percent=tolerance,
                        error_message=f"Missing metric value (baseline: {baseline_val}, incoming: {incoming_val})",
                    )
                )
                continue

            if baseline_val == 0:
                results.append(
                    ValidationResult(
                        passed=False,
                        metric_name=metric_name,
                        baseline_value=baseline_val,
                        incoming_value=incoming_val,
                        percent_difference=None,
                        tolerance_percent=tolerance,
                        error_message="Baseline value is 0",
                    )
                )
                continue

            # Calculate percent difference
            pct_diff = abs(incoming_val - baseline_val) / abs(baseline_val) * 100
            passed = pct_diff <= tolerance

            results.append(
                ValidationResult(
                    passed=passed,
                    metric_name=metric_name,
                    baseline_value=baseline_val,
                    incoming_value=incoming_val,
                    percent_difference=pct_diff,
                    tolerance_percent=tolerance,
                )
            )

        return results

    def validate_dataset(
        self,
        jurisdiction: str,
        dataset_name: str,
        incoming_metrics: Dict[str, Any],
        metric_configs: List[Dict[str, Any]],
        year: Optional[int] = None,
    ) -> tuple[bool, List[ValidationResult]]:
        """
        Validate incoming dataset against baseline.

        Args:
            jurisdiction: Jurisdiction name
            dataset_name: Dataset name
            incoming_metrics: Metrics collected from incoming data
            metric_configs: Metric configurations
            year: Optional year to compare against (defaults to most recent)

        Returns:
            Tuple of (all_passed, list of ValidationResult objects)

        Raises:
            Exception: If baseline data cannot be queried
        """
        print(f"\nValidating {jurisdiction}/{dataset_name}...")

        # Check if baseline data exists before attempting to query
        if not self._check_baseline_exists(jurisdiction, dataset_name, year):
            year_info = f" for year={year}" if year else ""
            print(f"⚠ No baseline data found for {jurisdiction}/{dataset_name}{year_info}")
            print(f"  This appears to be the first load. Skipping validation.")
            # Return success with empty results - allow the load to proceed
            return True, []

        # Query baseline metrics
        baseline_metrics = self.query_baseline_metrics(
            jurisdiction=jurisdiction,
            dataset_name=dataset_name,
            metrics=metric_configs,
            year=year,
        )

        # Compare metrics
        validation_results = self.compare_metrics(
            baseline_metrics=baseline_metrics,
            incoming_metrics=incoming_metrics,
            metric_configs=metric_configs,
        )

        # Check if all validations passed
        all_passed = all(result.passed for result in validation_results)

        # Print results
        print("\nValidation Results:")
        for result in validation_results:
            print(f"  {result}")

        return all_passed, validation_results


class ValidationError(Exception):
    """Exception raised when data validation fails."""

    def __init__(self, dataset_name: str, results: List[ValidationResult]):
        self.dataset_name = dataset_name
        self.results = results
        failed = [r for r in results if not r.passed]
        message = f"Validation failed for {dataset_name}:\n"
        for result in failed:
            message += f"  {result}\n"
        super().__init__(message)
