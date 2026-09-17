# Databricks notebook source
# COMMAND ----------
# Centralized Environment Configuration, Schemas & Shared Logic

import sys
import os

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = os.getcwd()

repo_root = os.path.abspath(os.path.join(current_dir, "..")) if os.path.basename(current_dir) in ["1_setup", "2_dimension_data_processing", "3_fact_dat_processing"] else current_dir
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Initialize Databricks local compatibility shim
try:
    from src.compat import init_notebook_context
    spark, dbutils, display = init_notebook_context(globals())
except ImportError:
    pass

# COMMAND ----------
# 2. Environment-Aware Configuration (dev vs prod)
try:
    env = dbutils.widgets.get("env").lower()
except Exception:
    env = os.getenv("ENVIRONMENT", "prod").lower()

CONFIG = {
    "dev": {"catalog": "fmcg_dev", "bucket": "spartsbar-2355-dev"},
    "prod": {"catalog": "fmcg", "bucket": "spartsbar-2355"},
}
active_config = CONFIG.get(env, CONFIG["prod"])
catalog = active_config["catalog"]
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
s3_bucket = active_config["bucket"]

def base_path_for(data_source: str) -> str:
    return f"s3://{s3_bucket}/{data_source}/*.csv"

# COMMAND ----------
# 3. Expose Schemas, Transformations, Data Quality & Auditing
from src.schemas import (
    orders_schema, products_schema, pricing_schema, customers_schema
)
from src.transformations import (
    clean_sentinel_id, strip_weekday_prefix, parse_multi_format_date,
    clean_price_column, extract_product_variant, generate_product_hash,
    clean_cities, clean_orders_silver, aggregate_orders_to_monthly
)
from src.data_quality import (
    run_quality_checks, ORDERS_QUALITY_CHECKS, PRICING_QUALITY_CHECKS,
    PRODUCTS_QUALITY_CHECKS, CUSTOMERS_QUALITY_CHECKS
)
from src.audit import add_audit_columns, log_pipeline_event, generate_run_id
