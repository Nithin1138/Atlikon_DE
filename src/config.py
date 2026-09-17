"""
Pipeline Environment Configuration Module.
Supports dynamic switching between 'dev' and 'prod' catalogs and cloud storage buckets.
"""

import os
from typing import Dict, Any, Optional

DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "dev": {
        "catalog": "fmcg_dev",
        "bucket": "spartsbar-2355-dev",
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "gold_schema": "gold",
    },
    "prod": {
        "catalog": "fmcg",
        "bucket": "spartsbar-2355",
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "gold_schema": "gold",
    },
}


def get_environment(dbutils=None) -> str:
    """
    Resolves the current execution environment (dev/prod).
    Checks Databricks widgets first, then environment variable, defaulting to 'prod'.
    """
    if dbutils is not None:
        try:
            return dbutils.widgets.get("env").lower()
        except Exception:
            pass
    return os.getenv("ENVIRONMENT", "prod").lower()


def get_config(env: Optional[str] = None, dbutils=None) -> Dict[str, Any]:
    """
    Retrieves the pipeline configuration dictionary for the specified or resolved environment.
    """
    if env is None:
        env = get_environment(dbutils)
    if env not in DEFAULT_CONFIG:
        env = "prod"
    return DEFAULT_CONFIG[env]


def base_path_for(data_source: str, env: Optional[str] = None, dbutils=None) -> str:
    """
    Generates the S3 base path for a given data source based on current environment.
    """
    cfg = get_config(env, dbutils)
    bucket = cfg["bucket"]
    return f"s3://{bucket}/{data_source}/*.csv"
