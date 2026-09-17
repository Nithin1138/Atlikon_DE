"""
Data Quality Validation Engine.
Provides declarative, lightweight assertions for blocking and non-blocking quality gates before Gold writes.
"""

from typing import Dict, Tuple, List
import logging
from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger("fmcg_pipeline.data_quality")


def run_quality_checks(
    df: DataFrame,
    checks: Dict[str, Tuple[str, bool]],
    table_name: str
) -> List[Tuple[str, int, bool]]:
    """
    Executes a dictionary of data quality assertions against a DataFrame.
    
    Args:
        df: Target DataFrame to evaluate.
        checks: Mapping of check_name -> (SQL condition expression that MUST be true, is_blocking flag).
        table_name: Identifier for the table/dataset being verified.
        
    Returns:
        List of tuples: (check_name, failed_row_count, is_blocking)
        
    Raises:
        ValueError: If a blocking check fails (failed_row_count > 0).
    """
    results: List[Tuple[str, int, bool]] = []

    for name, (condition, is_blocking) in checks.items():
        # Count rows where the assertion expression evaluates to false (or null)
        failed_count = df.filter(~F.expr(condition)).count()
        results.append((name, failed_count, is_blocking))

        if failed_count > 0:
            msg = f"Data Quality Check '{name}' failed on {failed_count} rows in table '{table_name}' (expression: {condition})."
            if is_blocking:
                logger.error(f"[BLOCKING] {msg}")
                raise ValueError(f"BLOCKING DATA QUALITY FAILURE: {msg}")
            else:
                logger.warning(f"[WARNING] {msg}")

    return results


# Standard Production Check Definitions
ORDERS_QUALITY_CHECKS = {
    "order_qty_not_null": ("order_qty IS NOT NULL", True),
    "order_qty_positive": ("order_qty > 0", True),
    "date_parsed": ("order_placement_date IS NOT NULL", True),
    "customer_id_valid_rate": ("customer_id != '999999'", False),  # Non-blocking warning for spikes
    "product_id_valid_rate": ("product_id != '999999'", False),
}

PRICING_QUALITY_CHECKS = {
    "price_non_negative": ("price_inr >= 0", True),
    "year_not_null": ("year IS NOT NULL", True),
    "product_code_not_null": ("product_code IS NOT NULL", True),
}

PRODUCTS_QUALITY_CHECKS = {
    "product_code_not_null": ("product_code IS NOT NULL", True),
    "division_not_null": ("division IS NOT NULL", True),
}

CUSTOMERS_QUALITY_CHECKS = {
    "customer_code_not_null": ("customer_code IS NOT NULL", True),
    "market_not_null": ("market IS NOT NULL", True),
}
