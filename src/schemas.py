"""
Explicit PySpark Schemas for Ingestion.
Eliminates inferSchema passes and prevents silent schema drift.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# Orders Schema (Bronze Landing)
orders_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_placement_date", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("order_qty", IntegerType(), nullable=True),
])

# Products Schema (Bronze Landing)
products_schema = StructType([
    StructField("product_id", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
])

# Pricing Schema (Bronze Landing)
pricing_schema = StructType([
    StructField("product_id", StringType(), nullable=True),
    StructField("month", StringType(), nullable=True),
    StructField("gross_price", StringType(), nullable=True),
])

# Customers Schema (Bronze Landing)
customers_schema = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("customer_name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
])
