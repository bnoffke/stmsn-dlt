#!/usr/bin/env python3
"""
Test script to verify the validation fixes.

Tests:
1. Column name fix - TotalTaxes accumulates correctly
2. GCS authentication - fsspec/gcsfs works with gcloud auth
"""

import duckdb
from clients.arcgis import MetricsAccumulator, MetricConfig


def test_column_name_fix():
    """Test that TotalTaxes (API field name) accumulates correctly."""
    print("=" * 60)
    print("Test 1: Column Name Fix (TotalTaxes)")
    print("=" * 60)

    # Simulate API response with TotalTaxes (PascalCase)
    api_records = [
        {"parcel": "001", "TotalTaxes": 1000.50, "geometry": "..."},
        {"parcel": "002", "TotalTaxes": 2500.75, "geometry": "..."},
        {"parcel": "003", "TotalTaxes": 1500.25, "geometry": "..."},
        {"parcel": "004", "TotalTaxes": None, "geometry": "..."},  # Null value
    ]

    # Create accumulator with TotalTaxes (API field name)
    metrics = [
        MetricConfig(
            name="total_taxes",
            aggregate="sum",
            column="TotalTaxes",  # API field name
        )
    ]

    accumulator = MetricsAccumulator(metric_configs=metrics)

    # Process records
    for record in api_records:
        accumulator.add_record(record)

    # Get results
    results = accumulator.get_results()

    print(f"\nRecords processed: {results['row_count']}")
    print(f"Total taxes sum: ${results['total_taxes']:,.2f}")
    print(f"Expected sum: $5,001.50")

    expected_sum = 1000.50 + 2500.75 + 1500.25
    assert results['row_count'] == 4, f"Row count should be 4, got {results['row_count']}"
    assert abs(results['total_taxes'] - expected_sum) < 0.01, f"Sum should be {expected_sum}, got {results['total_taxes']}"

    print("✓ Test PASSED: TotalTaxes accumulates correctly")


def test_gcs_authentication():
    """Test that GCS authentication works with fsspec/gcsfs."""
    print("\n" + "=" * 60)
    print("Test 2: GCS Authentication with fsspec/gcsfs")
    print("=" * 60)

    from fsspec import filesystem

    conn = duckdb.connect(':memory:')

    print("\nRegistering gcsfs filesystem with DuckDB...")
    fs = filesystem('gcs')
    conn.register_filesystem(fs)

    # Test querying GCS parquet files
    gcs_path = "gcs://strong-towns-madison/bronze/arcgis/madison/parcels/**/*.parquet"
    print(f"Querying: {gcs_path}")

    try:
        query = f"""
            SELECT
                COUNT(*) as row_count,
                SUM(total_taxes) as total_taxes_sum
            FROM read_parquet('{gcs_path}')
        """

        result = conn.execute(query).fetchone()
        row_count = result[0]
        total_taxes_sum = result[1]

        print(f"\nRow count: {row_count:,}")
        print(f"Total taxes sum: ${total_taxes_sum:,.2f}")

        assert row_count > 0, "Row count should be greater than 0"
        assert total_taxes_sum > 0, "Total taxes sum should be greater than 0"

        print("✓ Test PASSED: GCS authentication works with fsspec/gcsfs")

    except Exception as e:
        print(f"✗ Test FAILED: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    test_column_name_fix()
    test_gcs_authentication()

    print("\n" + "=" * 60)
    print("All Tests PASSED!")
    print("=" * 60)
