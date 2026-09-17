"""
Pure, Reusable PySpark Transformation & Cleansing Functions.
Centralizes business logic to eliminate DRY violations and enable unit testing.
"""

from typing import List, Dict, Optional, cast
from pyspark.sql import DataFrame, functions as F

DEFAULT_DATE_FORMATS: List[str] = [
    "yyyy/MM/dd",
    "dd-MM-yyyy",
    "dd/MM/yyyy",
    "MMMM dd, yyyy",
    "yyyy-MM-dd",
]

DEFAULT_CITY_MAPPING: Dict[str, str] = {
    "Bengaluruu": "Bengaluru",
    "Bengalore": "Bengaluru",
    "Hyderabadd": "Hyderabad",
    "Hyderbad": "Hyderabad",
    "NewDelhi": "New Delhi",
    "NewDheli": "New Delhi",
    "NewDelhee": "New Delhi",
}

DEFAULT_ALLOWED_CITIES: List[str] = ["Bengaluru", "Hyderabad", "New Delhi"]


def clean_sentinel_id(df: DataFrame, column: str, sentinel: str = "999999") -> DataFrame:
    """
    Replaces non-numeric or malformed identifiers with a designated sentinel value (e.g. '999999').
    Preserves fact rows while avoiding silent join failures.
    """
    return df.withColumn(
        column,
        F.when(
            F.col(column).cast("string").rlike("^[0-9]+$"),
            F.col(column).cast("string")
        ).otherwise(F.lit(sentinel))
    )


def strip_weekday_prefix(df: DataFrame, column: str) -> DataFrame:
    """
    Strips leading weekday names (e.g., 'Tuesday, July 01, 2025' -> 'July 01, 2025').
    """
    return df.withColumn(
        column,
        F.regexp_replace(F.col(column), r"^[A-Za-z]+,\s*", "")
    )


def parse_multi_format_date(
    df: DataFrame,
    column: str,
    formats: Optional[List[str]] = None
) -> DataFrame:
    """
    Attempts parsing dates sequentially across multiple common formats via F.coalesce + F.try_to_date.
    """
    if formats is None:
        formats = DEFAULT_DATE_FORMATS
    return df.withColumn(
        column,
        F.coalesce(*[F.try_to_date(F.col(column), fmt) for fmt in formats])
    )


def clean_price_column(df: DataFrame, column: str = "gross_price") -> DataFrame:
    """
    Validates numeric formats, flips erroneous negative prices to positive,
    and defaults unparseable values to 0.0.
    """
    is_valid_numeric = F.col(column).rlike(r"^-?\d+(\.\d+)?$")
    numeric_val = F.col(column).cast("double")
    
    return df.withColumn(
        column,
        F.when(
            is_valid_numeric,
            F.when(numeric_val < 0, -1.0 * numeric_val).otherwise(numeric_val)
        ).otherwise(F.lit(0.0))
    )


def extract_product_variant(
    df: DataFrame,
    source_col: str = "product_name",
    target_col: str = "variant"
) -> DataFrame:
    """
    Extracts packaging/variant information enclosed in parentheses (e.g. 'Hydration Mix (Lemon)' -> 'Lemon').
    """
    return df.withColumn(
        target_col,
        F.regexp_extract(F.col(source_col), r"\((.*?)\)", 1)
    )


def generate_product_hash(
    df: DataFrame,
    source_col: str = "product_name",
    target_col: str = "product_code"
) -> DataFrame:
    """
    Generates a deterministic SHA-256 hash from a sanitized product name as surrogate key.
    """
    return df.withColumn(
        target_col,
        F.sha2(F.col(source_col).cast("string"), 256)
    )


def clean_cities(
    df: DataFrame,
    column: str = "city",
    city_mapping: Optional[Dict[str, str]] = None,
    allowed_cities: Optional[List[str]] = None
) -> DataFrame:
    """
    Standardizes geographic city names using a typo mapping dictionary
    and applies a whitelist filter setting unrecognized entries to null.
    """
    if city_mapping is None:
        city_mapping = DEFAULT_CITY_MAPPING
    if allowed_cities is None:
        allowed_cities = DEFAULT_ALLOWED_CITIES

    # Apply typo replacements
    df_replaced = df.replace(cast(dict, city_mapping), subset=[column])

    # Whitelist filtering
    return df_replaced.withColumn(
        column,
        F.when(F.col(column).isNull(), None)
        .when(F.col(column).isin(allowed_cities), F.col(column))
        .otherwise(None)
    )


def clean_orders_silver(
    df: DataFrame,
    date_formats: Optional[List[str]] = None
) -> DataFrame:
    """
    Comprehensive Silver cleaning pipeline for raw orders data.
    Shared across both full-load backfill and incremental loads.
    """
    # 1. Filter out rows with missing measure column (order_qty)
    cleaned = df.filter(F.col("order_qty").isNotNull())

    # 2. Clean customer_id with sentinel '999999'
    cleaned = clean_sentinel_id(cleaned, "customer_id", sentinel="999999")

    # 3. Strip weekday prefix from order placement date
    cleaned = strip_weekday_prefix(cleaned, "order_placement_date")

    # 4. Multi-format date parsing
    cleaned = parse_multi_format_date(cleaned, "order_placement_date", date_formats)

    # 5. Ensure product_id is cast to string
    cleaned = cleaned.withColumn("product_id", F.col("product_id").cast("string"))

    # 6. Deduplicate by compound natural key
    cleaned = cleaned.dropDuplicates([
        "order_id",
        "order_placement_date",
        "customer_id",
        "product_id",
        "order_qty",
    ])

    return cleaned


def aggregate_orders_to_monthly(
    df: DataFrame,
    date_col: str = "date",
    qty_col: str = "sold_quantity",
    product_col: str = "product_code",
    customer_col: str = "customer_code"
) -> DataFrame:
    """
    Aggregates daily transaction orders to monthly grain matching corporate OLAP expectations.
    """
    return (
        df
        .withColumn("month_start", F.trunc(date_col, "MM"))
        .groupBy("month_start", product_col, customer_col)
        .agg(F.sum(qty_col).alias(qty_col))
        .withColumnRenamed("month_start", date_col)
    )
