"""
Unit Tests for Transformation Functions (src/transformations.py).
"""

import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.transformations import (
    clean_sentinel_id,
    strip_weekday_prefix,
    parse_multi_format_date,
    clean_price_column,
    extract_product_variant,
    generate_product_hash,
    clean_cities,
    clean_orders_silver,
    aggregate_orders_to_monthly,
)


def test_clean_sentinel_id_preserves_numeric_replaces_invalid(spark):
    data = [
        ("12345",),
        ("987654",),
        ("abc-99",),
        ("corrupt#1",),
        (None,),
    ]
    df = spark.createDataFrame(data, ["customer_id"])
    result = clean_sentinel_id(df, "customer_id", sentinel="999999").collect()

    assert result[0]["customer_id"] == "12345"
    assert result[1]["customer_id"] == "987654"
    assert result[2]["customer_id"] == "999999"
    assert result[3]["customer_id"] == "999999"
    assert result[4]["customer_id"] == "999999"


def test_strip_weekday_prefix(spark):
    data = [
        ("Tuesday, July 01, 2025",),
        ("Wednesday, August 15, 2024",),
        ("2025-07-01",),
        ("01/07/2025",),
    ]
    df = spark.createDataFrame(data, ["order_placement_date"])
    result = strip_weekday_prefix(df, "order_placement_date").collect()

    assert result[0]["order_placement_date"] == "July 01, 2025"
    assert result[1]["order_placement_date"] == "August 15, 2024"
    assert result[2]["order_placement_date"] == "2025-07-01"
    assert result[3]["order_placement_date"] == "01/07/2025"


def test_parse_multi_format_date_handles_all_supported_formats(spark):
    data = [
        ("2025/07/01",),
        ("01-07-2025",),
        ("01/07/2025",),
        ("July 01, 2025",),
        ("2025-07-01",),
        ("invalid-date-string",),
    ]
    df = spark.createDataFrame(data, ["order_placement_date"])
    result = parse_multi_format_date(df, "order_placement_date").collect()

    target_date = datetime.date(2025, 7, 1)
    for i in range(5):
        assert result[i]["order_placement_date"] == target_date, f"Failed for input {data[i][0]}"

    assert result[5]["order_placement_date"] is None


def test_clean_price_column_rectifies_negative_and_scrubs_garbage(spark):
    data = [
        ("250.00",),
        ("-180.50",),
        ("0",),
        ("FREE_SAMPLE",),
        ("None",),
        (None,),
    ]
    df = spark.createDataFrame(data, ["gross_price"])
    result = clean_price_column(df, "gross_price").collect()

    assert result[0]["gross_price"] == 250.0
    assert result[1]["gross_price"] == 180.50  # Negative flipped positive
    assert result[2]["gross_price"] == 0.0
    assert result[3]["gross_price"] == 0.0     # Garbage string defaulted to 0.0
    assert result[4]["gross_price"] == 0.0
    assert result[5]["gross_price"] == 0.0


def test_extract_product_variant(spark):
    data = [
        ("Hydration Mix (Lemon)",),
        ("Protein Bar (Chocolate Chip 60g)",),
        ("Standard Shaker Bottle",),
    ]
    df = spark.createDataFrame(data, ["product_name"])
    result = extract_product_variant(df, "product_name", "variant").collect()

    assert result[0]["variant"] == "Lemon"
    assert result[1]["variant"] == "Chocolate Chip 60g"
    assert result[2]["variant"] == ""


def test_generate_product_hash_deterministic(spark):
    data = [
        ("Hydration Mix (Lemon)",),
        ("Hydration Mix (Lemon)",),
        ("Protein Bar (Chocolate)",),
    ]
    df = spark.createDataFrame(data, ["product_name"])
    result = generate_product_hash(df, "product_name", "product_code").collect()

    # Deterministic SHA-256
    assert result[0]["product_code"] == result[1]["product_code"]
    assert len(result[0]["product_code"]) == 64
    assert result[0]["product_code"] != result[2]["product_code"]


def test_clean_cities_typos_and_whitelist(spark):
    data = [
        ("Bengaluruu",),
        ("Hyderabadd",),
        ("NewDelhee",),
        ("Bengaluru",),
        ("UnknownCityXYZ",),
        (None,),
    ]
    df = spark.createDataFrame(data, ["city"])
    result = clean_cities(df, "city").collect()

    assert result[0]["city"] == "Bengaluru"
    assert result[1]["city"] == "Hyderabad"
    assert result[2]["city"] == "New Delhi"
    assert result[3]["city"] == "Bengaluru"
    assert result[4]["city"] is None  # Whitelist rejected
    assert result[5]["city"] is None


def test_clean_orders_silver_end_to_end(spark):
    raw_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_placement_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_qty", IntegerType(), True),
    ])

    data = [
        ("O1", "Tuesday, July 01, 2025", "789403", "501", 3),
        ("O2", "02-07-2025", "abc-bad", "501", 2),
        ("O3", "July 03, 2025", "789403", "501", None),      # Should be dropped (null qty)
        ("O1", "Tuesday, July 01, 2025", "789403", "501", 3), # Duplicate of row 1
    ]
    raw_df = spark.createDataFrame(data, raw_schema)
    cleaned_df = clean_orders_silver(raw_df)

    rows = cleaned_df.collect()
    assert len(rows) == 2  # Row 3 dropped (null qty), Row 4 dropped (duplicate)

    # Verify sentinel replacement
    o2_row = [r for r in rows if r["order_id"] == "O2"][0]
    assert o2_row["customer_id"] == "999999"
    assert o2_row["order_placement_date"] == datetime.date(2025, 7, 2)


def test_aggregate_orders_to_monthly(spark):
    data = [
        (datetime.date(2025, 7, 2), 10, "PROD_A", "CUST_1"),
        (datetime.date(2025, 7, 15), 5, "PROD_A", "CUST_1"),   # Same month, prod, cust -> sum = 15
        (datetime.date(2025, 8, 1), 20, "PROD_A", "CUST_1"),   # Different month -> 20
        (datetime.date(2025, 7, 10), 7, "PROD_B", "CUST_1"),   # Different prod -> 7
    ]
    df = spark.createDataFrame(data, ["date", "sold_quantity", "product_code", "customer_code"])
    monthly_df = aggregate_orders_to_monthly(df)

    results = monthly_df.collect()
    assert len(results) == 3

    july_a = [r for r in results if r["date"] == datetime.date(2025, 7, 1) and r["product_code"] == "PROD_A"][0]
    assert july_a["sold_quantity"] == 15

    aug_a = [r for r in results if r["date"] == datetime.date(2025, 8, 1) and r["product_code"] == "PROD_A"][0]
    assert aug_a["sold_quantity"] == 20
