#!/usr/bin/env python3
"""
Test script for validation system.

This demonstrates the validation functionality by:
1. Reading baseline metrics from local parquet files
2. Simulating incoming data metrics
3. Running validation comparison
"""

from pathlib import Path
from utils.data_validator import DataValidator
from clients.arcgis import MetricsAccumulator, MetricConfig
import duckdb


def test_local_validation():
    """Test validation against local parquet files."""

    print("=" * 60)
    print("Validation System Test")
    print("=" * 60)

    # Setup paths
    local_data_path = Path("test_output/arcgis/madison")
    parcels_path = local_data_path / "parcels/year=2025/*.parquet"

    print(f"\nBaseline data path: {parcels_path}")

    # Query baseline metrics from local parquet using DuckDB
    print("\n1. Querying baseline metrics from local parquet...")
    conn = duckdb.connect(":memory:")

    query = f"""
        SELECT
            COUNT(*) as row_count,
            SUM(total_taxes) as total_taxes_sum
        FROM read_parquet('{parcels_path}')
    """

    result = conn.execute(query).fetchone()
    baseline_row_count = result[0]
    baseline_total_taxes = result[1]

    print(f"   Baseline row count: {baseline_row_count}")
    print(f"   Baseline total taxes: ${baseline_total_taxes:,.2f}")

    # Simulate incoming metrics (slightly different to test tolerance)
    print("\n2. Simulating incoming data metrics...")

    # Scenario 1: Within tolerance (3% difference)
    incoming_row_count_pass = int(baseline_row_count * 1.03)  # 3% more
    incoming_total_taxes_pass = baseline_total_taxes * 1.02   # 2% more

    print(f"   Scenario 1 - Should PASS (within 5% tolerance):")
    print(f"     Incoming row count: {incoming_row_count_pass} ({((incoming_row_count_pass - baseline_row_count) / baseline_row_count * 100):.1f}% diff)")
    print(f"     Incoming total taxes: ${incoming_total_taxes_pass:,.2f} ({((incoming_total_taxes_pass - baseline_total_taxes) / baseline_total_taxes * 100):.1f}% diff)")

    # Test with DataValidator
    print("\n3. Testing validation with DataValidator...")

    # Note: For this test, we'll manually create validation results
    # since DataValidator expects GCS paths

    incoming_metrics_pass = {
        'row_count': incoming_row_count_pass,
        'total_taxes': incoming_total_taxes_pass
    }

    baseline_metrics = {
        'row_count': baseline_row_count,
        'total_taxes': baseline_total_taxes
    }

    metric_configs = [
        {
            'name': 'total_taxes',
            'aggregate': 'sum',
            'column': 'total_taxes',
        }
    ]

    # Create a mock validator for local testing
    class LocalValidator(DataValidator):
        def query_baseline_metrics(self, jurisdiction, dataset_name, metrics, year=None):
            """Override to read from local parquet instead of GCS."""
            return baseline_metrics

    validator = LocalValidator(
        gcs_bucket="test",  # Not used for local test
        default_tolerance_percent=5.0
    )

    # Test Scenario 1: Should PASS
    print("\n   Testing Scenario 1 (should PASS)...")
    validation_results_pass = validator.compare_metrics(
        baseline_metrics=baseline_metrics,
        incoming_metrics=incoming_metrics_pass,
        metric_configs=metric_configs
    )

    all_passed = all(r.passed for r in validation_results_pass)
    print(f"   Result: {'✓ PASSED' if all_passed else '✗ FAILED'}")
    for result in validation_results_pass:
        print(f"     {result}")

    # Scenario 2: Outside tolerance (10% difference) - should FAIL
    print("\n   Testing Scenario 2 (should FAIL - 10% difference)...")
    incoming_row_count_fail = int(baseline_row_count * 1.10)  # 10% more
    incoming_total_taxes_fail = baseline_total_taxes * 0.88   # 12% less

    print(f"     Incoming row count: {incoming_row_count_fail} ({((incoming_row_count_fail - baseline_row_count) / baseline_row_count * 100):.1f}% diff)")
    print(f"     Incoming total taxes: ${incoming_total_taxes_fail:,.2f} ({((incoming_total_taxes_fail - baseline_total_taxes) / baseline_total_taxes * 100):.1f}% diff)")

    incoming_metrics_fail = {
        'row_count': incoming_row_count_fail,
        'total_taxes': incoming_total_taxes_fail
    }

    validation_results_fail = validator.compare_metrics(
        baseline_metrics=baseline_metrics,
        incoming_metrics=incoming_metrics_fail,
        metric_configs=metric_configs
    )

    all_passed_fail = all(r.passed for r in validation_results_fail)
    print(f"   Result: {'✓ PASSED' if all_passed_fail else '✗ FAILED (as expected)'}")
    for result in validation_results_fail:
        print(f"     {result}")

    print("\n" + "=" * 60)
    print("Validation System Test Complete!")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    test_local_validation()
