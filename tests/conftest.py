"""
PyTest Configuration and Shared SparkSession Fixture.
"""

import os
import sys
import pytest
from pyspark.sql import SparkSession

# Ensure PySpark workers match the exact driver Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(scope="session")
def spark():
    """Provides a lightweight local SparkSession for unit testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("atlikon_de_test_suite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
