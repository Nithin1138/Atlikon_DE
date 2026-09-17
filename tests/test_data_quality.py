"""
Unit Tests for Data Quality Checks Engine (src/data_quality.py).
"""

import pytest
from src.data_quality import run_quality_checks


def test_quality_checks_all_pass(spark):
    data = [
        (10, "2025-07-01", "CUST_1"),
        (25, "2025-07-02", "CUST_2"),
    ]
    df = spark.createDataFrame(data, ["order_qty", "order_placement_date", "customer_id"])

    checks = {
        "order_qty_positive": ("order_qty > 0", True),
        "date_not_null": ("order_placement_date IS NOT NULL", True),
    }

    results = run_quality_checks(df, checks, "test_table")
    assert len(results) == 2
    assert all(failed == 0 for _, failed, _ in results)


def test_quality_checks_blocking_raises_exception(spark):
    data = [
        (10, "2025-07-01"),
        (-5, "2025-07-02"),  # Violates order_qty > 0
    ]
    df = spark.createDataFrame(data, ["order_qty", "order_placement_date"])

    checks = {
        "order_qty_positive": ("order_qty > 0", True),  # Blocking
    }

    with pytest.raises(ValueError, match="BLOCKING DATA QUALITY FAILURE"):
        run_quality_checks(df, checks, "test_table")


def test_quality_checks_non_blocking_does_not_raise(spark):
    data = [
        ("12345",),
        ("999999",),  # Sentinel row
        ("999999",),
    ]
    df = spark.createDataFrame(data, ["customer_id"])

    checks = {
        "customer_id_valid_rate": ("customer_id != '999999'", False),  # Non-blocking warning
    }

    results = run_quality_checks(df, checks, "test_table")
    assert len(results) == 1
    name, failed_count, is_blocking = results[0]
    assert name == "customer_id_valid_rate"
    assert failed_count == 2
    assert is_blocking is False
