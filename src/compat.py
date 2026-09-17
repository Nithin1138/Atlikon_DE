"""
Databricks Notebook Local Compatibility Shim.
Provides seamless execution of Databricks notebooks in local VS Code / Jupyter environments
by providing fallback implementations for `spark`, `dbutils`, `display`, and Unity Catalog emulation.
When executed inside an actual Databricks cluster, this module is completely non-intrusive.
"""

import os
import re
import sys
from typing import Any, Dict, List, Optional

# Ensure driver and workers run the exact same python executable and bind to localhost on macOS
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class MockFileInfo:
    """Mock file representation mimicking com.databricks.backend.daemon.dbutils.FileInfo."""
    def __init__(self, path: str, name: str, size: int = 1024):
        self.path = path
        self.name = name
        self.size = size
        self.modificationTime = 0

    def __repr__(self):
        return f"FileInfo(path='{self.path}', name='{self.name}', size={self.size})"


class MockWidgets:
    """Mock Databricks dbutils.widgets module."""
    def __init__(self):
        self._widgets: Dict[str, str] = {}

    def text(self, name: str, defaultValue: str = "", label: str = "") -> None:
        if name not in self._widgets:
            self._widgets[name] = str(defaultValue)

    def dropdown(self, name: str, defaultValue: str = "", choices: Optional[List[str]] = None, label: str = "") -> None:
        if name not in self._widgets:
            self._widgets[name] = str(defaultValue)

    def combobox(self, name: str, defaultValue: str = "", choices: Optional[List[str]] = None, label: str = "") -> None:
        if name not in self._widgets:
            self._widgets[name] = str(defaultValue)

    def multiselect(self, name: str, defaultValue: str = "", choices: Optional[List[str]] = None, label: str = "") -> None:
        if name not in self._widgets:
            self._widgets[name] = str(defaultValue)

    def get(self, name: str) -> str:
        env_val = os.getenv(f"WIDGET_{name.upper()}", os.getenv(name.upper()))
        if env_val is not None:
            return env_val
        return self._widgets.get(name, "")

    def getArgument(self, name: str, defaultValue: Optional[str] = None) -> str:
        val = self.get(name)
        return val if val else (defaultValue or "")

    def remove(self, name: str) -> None:
        self._widgets.pop(name, None)

    def removeAll(self) -> None:
        self._widgets.clear()


class MockFS:
    """Mock Databricks dbutils.fs module."""
    def ls(self, path: str) -> List[MockFileInfo]:
        print(f"[Local dbutils.fs] ls('{path}')")
        return []

    def mv(self, source: str, dest: str, recurse: bool = False) -> bool:
        print(f"[Local dbutils.fs] mv('{source}' -> '{dest}', recurse={recurse})")
        return True

    def cp(self, source: str, dest: str, recurse: bool = False) -> bool:
        print(f"[Local dbutils.fs] cp('{source}' -> '{dest}', recurse={recurse})")
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        print(f"[Local dbutils.fs] rm('{path}', recurse={recurse})")
        return True

    def mkdirs(self, path: str) -> bool:
        print(f"[Local dbutils.fs] mkdirs('{path}')")
        return True

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        print(f"[Local dbutils.fs] put('{path}', overwrite={overwrite})")
        return True

    def head(self, path: str, maxBytes: int = 65536) -> str:
        return ""


class MockSecrets:
    """Mock Databricks dbutils.secrets module."""
    def get(self, scope: str, key: str) -> str:
        env_key = f"{scope.upper()}_{key.upper()}"
        return os.getenv(env_key, os.getenv(key.upper(), "mock_secret"))


class MockNotebook:
    """Mock Databricks dbutils.notebook module."""
    def run(self, path: str, timeout_seconds: int = 0, arguments: Optional[Dict[str, str]] = None) -> str:
        print(f"[Local dbutils.notebook] run('{path}', arguments={arguments})")
        return ""

    def exit(self, value: str) -> None:
        print(f"[Local dbutils.notebook] exit({value})")


class MockDBUtils:
    """Mock Databricks Utilities."""
    def __init__(self):
        self.widgets = MockWidgets()
        self.fs = MockFS()
        self.secrets = MockSecrets()
        self.notebook = MockNotebook()


def mock_display(obj: Any, n: int = 20) -> None:
    """Local fallback for Databricks display() function."""
    if hasattr(obj, "show"):
        obj.show(n, truncate=False)
    else:
        print(obj)

# Monkey-patch DataFrame.display for local execution compatibility
try:
    from pyspark.sql import DataFrame
    if not hasattr(DataFrame, "display"):
        DataFrame.display = lambda self, n=20: mock_display(self, n)  # type: ignore
except Exception:
    pass


def register_run_resolver(ip=None):
    """Hooks IPython %run magic to transparently resolve relative Databricks notebook calls like `%run ./utilities`."""
    try:
        if ip is None:
            import IPython
            ip = IPython.get_ipython()
        if ip is not None:
            orig_run = ip.find_line_magic("run")
            if orig_run and not getattr(orig_run, "_is_atlikon_wrapped", False):
                def custom_run(parameter_s="", runner=None):
                    arg = parameter_s.strip().strip('"').strip("'")
                    if not os.path.exists(arg) and not os.path.exists(arg + ".py"):
                        base_name = os.path.basename(arg)
                        current_dir = os.getcwd()
                        repo_root = current_dir
                        if os.path.basename(current_dir) in ["1_setup", "2_dimension_data_processing", "3_fact_dat_processing"]:
                            repo_root = os.path.dirname(current_dir)

                        candidates = [
                            os.path.join(repo_root, "1_setup", base_name + ".py"),
                            os.path.join(repo_root, "1_setup", base_name + ".ipynb"),
                            os.path.join(current_dir, base_name + ".py"),
                            os.path.join(current_dir, base_name + ".ipynb"),
                            arg + ".py",
                            arg + ".ipynb",
                        ]
                        for cand in candidates:
                            if os.path.exists(cand):
                                return orig_run(cand, runner=runner)
                    return orig_run(parameter_s, runner=runner)

                custom_run._is_atlikon_wrapped = True
                ip.register_magic_function(custom_run, "line", "run")
    except Exception:
        pass


def register_sql_magic(ip=None):
    """
    Registers %sql and %%sql support for local IPython / Jupyter execution,
    mirroring Databricks %sql language switcher cells.
    """
    try:
        if ip is None:
            import IPython
            ip = IPython.get_ipython()
        if ip is None:
            return

        def sql_cell_transformer(lines):
            if not lines:
                return lines
            first_idx = None
            for i, line in enumerate(lines):
                if line.strip():
                    first_idx = i
                    break
            if first_idx is not None:
                first_line = lines[first_idx].strip()
                if (
                    first_line == "%sql"
                    or first_line == "%%sql"
                    or first_line.startswith("%sql ")
                    or first_line.startswith("%%sql ")
                ):
                    prefix = "%sql" if first_line.startswith("%sql") else "%%sql"
                    sql_first = first_line[len(prefix):].strip()
                    rest = lines[first_idx + 1:]
                    query_lines = ([sql_first] if sql_first else []) + [l.rstrip("\r\n") for l in rest]
                    full_sql = "\n".join(query_lines).strip()
                    if full_sql:
                        return [f"display(spark.sql({full_sql!r}))\n"]
                    else:
                        return ["pass\n"]
            return lines

        if hasattr(ip, "input_transformers_cleanup"):
            if not getattr(ip, "_has_atlikon_sql_transformer", False):
                ip.input_transformers_cleanup.append(sql_cell_transformer)
                ip._has_atlikon_sql_transformer = True

        def sql_magic(line="", cell=None):
            spark = ip.user_ns.get("spark")
            if spark is None:
                spark = get_or_create_local_spark()
                ip.user_ns["spark"] = spark
            query = cell if cell is not None and cell.strip() else line
            query = query.strip()
            if query:
                df = spark.sql(query)
                disp = ip.user_ns.get("display")
                if disp:
                    disp(df)
                return df

        sql_magic._is_atlikon_wrapped = True
        ip.register_magic_function(sql_magic, magic_kind="line_cell", magic_name="sql")
    except Exception:
        pass


def get_or_create_local_spark():
    """Initializes a local SparkSession with Delta Lake support and Unity Catalog emulation."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        venv_python = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".venv", "bin", "python"))
        raise RuntimeError(
            f"\n[Environment Error] PySpark is not installed in the currently active Python interpreter:\n"
            f"  Current Python: {sys.executable} (v{sys.version.split()[0]})\n"
            f"To fix this, please switch the Jupyter Notebook Kernel in the top-right corner to:\n"
            f"  'Python (.venv Atlikon_DE)' or '{venv_python}' (Python 3.12)\n"
        ) from e

    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    warehouse_dir = os.path.join(repo_root, "spark-warehouse")
    os.makedirs(warehouse_dir, exist_ok=True)

    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("Atlikon_DE_Local")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.warehouse.dir", f"file://{warehouse_dir}")
        .config("spark.ui.enabled", "false")
    )
    try:
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    except Exception:
        pass

    try:
        spark = builder.getOrCreate()
    except Exception:
        builder = (
            SparkSession.builder
            .master("local[1]")
            .appName("Atlikon_DE_Local")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.ui.enabled", "false")
        )
        spark = builder.getOrCreate()

    # Enable Delta schema autoMerge
    try:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    except Exception:
        pass

    # Wrap spark.sql to handle Databricks Unity Catalog commands on local Spark
    if not getattr(spark, "_is_local_emulated", False):
        orig_sql = spark.sql

        def safe_sql(query: str, *args, **kwargs):
            stripped = query.strip()
            upper = stripped.upper()

            # Handle semicolon-separated multi-statement blocks (e.g. in %sql cells)
            statements = [s.strip() for s in query.split(";") if s.strip()]
            if len(statements) > 1:
                last_res = None
                for stmt in statements:
                    last_res = safe_sql(stmt, *args, **kwargs)
                return last_res or spark.createDataFrame([], "status string")

            if stripped.endswith(";"):
                stripped = stripped[:-1].strip()
                upper = stripped.upper()

            # Handle Databricks Unity Catalog commands: CREATE CATALOG, USE CATALOG
            if upper.startswith("CREATE CATALOG") or upper.startswith("USE CATALOG"):
                print(f"[Local Spark Emulation] Handled Unity Catalog command: {stripped}")
                return spark.createDataFrame([], "status string")

            try:
                return orig_sql(stripped, *args, **kwargs)
            except Exception as e:
                err_str = str(e)
                # If local spark complains about multi-part namespace like fmcg.gold
                if "REQUIRES_SINGLE_PART_NAMESPACE" in err_str or "CATALOG_NOT_FOUND" in err_str:
                    simplified = re.sub(r"\b(fmcg|fmcg_dev)\.", "", stripped, flags=re.IGNORECASE)
                    print(f"[Local Spark Emulation] Multi-part namespace adapted: {stripped} -> {simplified}")
                    try:
                        return orig_sql(simplified, *args, **kwargs)
                    except Exception as inner_e:
                        inner_err = str(inner_e)
                        if upper.startswith("OPTIMIZE"):
                            print(f"[Local Spark Emulation] OPTIMIZE completed/skipped: {stripped}")
                            return spark.createDataFrame([], "status string")
                        if upper.startswith("DROP TABLE") and "TABLE_OR_VIEW_NOT_FOUND" in inner_err:
                            print(f"[Local Spark Emulation] Table already dropped or not found: {stripped}")
                            return spark.createDataFrame([], "status string")
                        raise inner_e
                if upper.startswith("OPTIMIZE") and ("TABLE_OR_VIEW_NOT_FOUND" in err_str or "DELTA_MISSING_DELTA_TABLE" in err_str):
                    print(f"[Local Spark Emulation] OPTIMIZE skipped for missing table: {stripped}")
                    return spark.createDataFrame([], "status string")
                if upper.startswith("DROP TABLE") and "TABLE_OR_VIEW_NOT_FOUND" in err_str:
                    print(f"[Local Spark Emulation] Table already dropped or not found: {stripped}")
                    return spark.createDataFrame([], "status string")
                raise

        spark.sql = safe_sql  # type: ignore

        # Wrap spark.catalog.tableExists to safely handle multi-part namespace in local metastore
        orig_table_exists = spark.catalog.tableExists

        def safe_table_exists(tableName: str, dbName: Optional[str] = None) -> bool:
            try:
                return orig_table_exists(tableName, dbName)
            except Exception as e:
                if "REQUIRES_SINGLE_PART_NAMESPACE" in str(e):
                    simplified = re.sub(r"\b(fmcg|fmcg_dev)\.", "", tableName, flags=re.IGNORECASE)
                    try:
                        return orig_table_exists(simplified, dbName)
                    except Exception:
                        return False
                return False

        spark.catalog.tableExists = safe_table_exists  # type: ignore

        # Wrap spark.table to safely handle multi-part namespace in local metastore
        orig_table = spark.table

        def safe_table(tableName: str):
            simplified = re.sub(r"\b(fmcg|fmcg_dev)\.", "", tableName, flags=re.IGNORECASE)
            try:
                return orig_table(tableName)
            except Exception as e:
                err_str = str(e)
                if "REQUIRES_SINGLE_PART_NAMESPACE" in err_str or "CATALOG_NOT_FOUND" in err_str or "TABLE_OR_VIEW_NOT_FOUND" in err_str:
                    print(f"[Local Spark Emulation] spark.table adapted: {tableName} -> {simplified}")
                    try:
                        return orig_table(simplified)
                    except Exception as inner_e:
                        if "TABLE_OR_VIEW_NOT_FOUND" in str(inner_e):
                            if "." in simplified:
                                schema_name = simplified.split(".")[0]
                                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                            if "products" in simplified:
                                data = [
                                    ("P101", "P101_CODE", "Beverages", "Beverages", "Atlikon Cola 500ml", "Regular"),
                                    ("P102", "P102_CODE", "Beverages", "Beverages", "Atlikon Orange Juice 1L", "Regular"),
                                    ("P103", "P103_CODE", "Snacks", "Snacks", "Atlikon Potato Chips 150g", "Regular"),
                                    ("P104", "P104_CODE", "Snacks", "Snacks", "Atlikon Dark Chocolate 100g", "Regular"),
                                ]
                                df_mock = spark.createDataFrame(data, ["product_id", "product_code", "division", "category", "product", "variant"])
                                df_mock.write.format("delta").mode("overwrite").saveAsTable(simplified)
                                return orig_table(simplified)
                            elif "gross_price" in simplified or "pricing" in simplified:
                                data = [
                                    ("P101_CODE", "P101", "2024-01-01", 1.50, 2024),
                                    ("P102_CODE", "P102", "2024-01-01", 2.80, 2024),
                                    ("P103_CODE", "P103", "2024-01-01", 1.20, 2024),
                                    ("P104_CODE", "P104", "2024-01-01", 3.00, 2024),
                                ]
                                df_mock = spark.createDataFrame(data, ["product_code", "product_id", "month", "price_inr", "year"])
                                df_mock.write.format("delta").mode("overwrite").saveAsTable(simplified)
                                return orig_table(simplified)
                        raise inner_e
                raise

        spark.table = safe_table  # type: ignore
        spark._is_local_emulated = True  # type: ignore

    # Wrap DataFrameWriter.saveAsTable and DataFrameReader.load for local execution
    try:
        from pyspark.sql.readwriter import DataFrameWriter, DataFrameReader

        if not getattr(DataFrameWriter.saveAsTable, "_is_local_emulated", False):
            orig_saveAsTable = DataFrameWriter.saveAsTable

            def safe_saveAsTable(self, name: str, *args, **kwargs):
                try:
                    return orig_saveAsTable(self, name, *args, **kwargs)
                except Exception as e:
                    err_str = str(e)
                    if (
                        "Couldn't find a catalog" in err_str
                        or "REQUIRES_SINGLE_PART_NAMESPACE" in err_str
                        or "DELTA_METADATA_MISMATCH" in err_str
                        or "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in err_str
                    ):
                        simplified = re.sub(r"\b(fmcg|fmcg_dev)\.", "", name, flags=re.IGNORECASE)
                        print(f"[Local Spark Emulation] Adapted target table: {name} -> {simplified}")
                        if "." in simplified:
                            schema_name = simplified.split(".")[0]
                            try:
                                self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                            except Exception:
                                pass
                        writer = self.option("overwriteSchema", "true").option("mergeSchema", "true")
                        try:
                            return orig_saveAsTable(writer, simplified, *args, **kwargs)
                        except Exception as write_err:
                            if "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in str(write_err):
                                import shutil
                                repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
                                parts = simplified.split(".")
                                target_dir = os.path.join(repo_root, "spark-warehouse", f"{parts[0]}.db", parts[1]) if len(parts) > 1 else os.path.join(repo_root, "spark-warehouse", simplified)
                                if os.path.exists(os.path.join(target_dir, "_delta_log")):
                                    try:
                                        self._spark.sql(f"CREATE TABLE IF NOT EXISTS {simplified} USING DELTA LOCATION '{target_dir}'")
                                        return orig_saveAsTable(writer, simplified, *args, **kwargs)
                                    except Exception:
                                        pass
                                shutil.rmtree(target_dir, ignore_errors=True)
                                return orig_saveAsTable(writer, simplified, *args, **kwargs)
                            raise
                    raise

            safe_saveAsTable._is_local_emulated = True  # type: ignore
            DataFrameWriter.saveAsTable = safe_saveAsTable  # type: ignore

        if not getattr(DataFrameReader.load, "_is_local_emulated", False):
            orig_load = DataFrameReader.load
            orig_schema = DataFrameReader.schema

            def safe_schema(self, schema):
                self._custom_schema = schema
                return orig_schema(self, schema)

            def safe_load(self, path=None, format=None, schema=None, **options):
                try:
                    return orig_load(self, path=path, format=format, schema=schema, **options)
                except Exception as e:
                    path_str = str(path or "")
                    err_str = str(e)
                    if "s3://" in path_str or "s3a://" in path_str or "No FileSystem" in err_str:
                        print(f"[Local Spark Emulation] S3 path detected without AWS credentials. Providing mock data for: {path_str}")
                        from src.schemas import (
                            orders_schema,
                            products_schema,
                            pricing_schema,
                            customers_schema,
                        )
                        import pyspark.sql.functions as F
                        meta_struct = F.struct(
                            F.lit("sample_data.csv").alias("file_name"),
                            F.lit(1024).cast("long").alias("file_size"),
                            F.lit(path_str).alias("file_path"),
                        )
                        if "product" in path_str.lower():
                            data = [
                                ("P101", "Atlikon Cola 500ml", "Beverages"),
                                ("P102", "Atlikon Orange Juice 1L", "Beverages"),
                                ("P103", "Atlikon Potato Chips 150g", "Snacks"),
                                ("P104", "Atlikon Dark Chocolate 100g", "Snacks"),
                            ]
                            df = self._spark.createDataFrame(data, products_schema)
                            return df.withColumn("_metadata", meta_struct)
                        elif "pricing" in path_str.lower():
                            data = [
                                ("P101", "2024-01-01", "$1.50"),
                                ("P102", "2024-01-01", "$2.80"),
                                ("P103", "2024-01-01", "$1.20"),
                                ("P104", "2024-01-01", "$3.00"),
                            ]
                            df = self._spark.createDataFrame(data, pricing_schema)
                            return df.withColumn("_metadata", meta_struct)
                        elif "customer" in path_str.lower():
                            data = [
                                ("1001", "Acme Supermarket", "New York"),
                                ("1002", "Metro Foods", "San Francisco"),
                                ("1003", "Apex Retail", "Chicago"),
                                ("999999", "Unknown Sentinel", "Unknown"),
                            ]
                            df = self._spark.createDataFrame(data, customers_schema)
                            return df.withColumn("_metadata", meta_struct)
                        elif "fact" in path_str.lower() or "order" in path_str.lower():
                            data = [
                                ("ORD001", "Tuesday, July 01, 2025", "1001", "P101", 50),
                                ("ORD002", "2025-07-15", "1002", "P102", 20),
                                ("ORD003", "01/08/2025", "1003", "P103", 100),
                                ("ORD004", "August 20, 2025", "1001", "P104", 15),
                            ]
                            df = self._spark.createDataFrame(data, orders_schema)
                            return df.withColumn("_metadata", meta_struct)
                        elif getattr(self, "_custom_schema", None) is not None:
                            df = self._spark.createDataFrame([], self._custom_schema)
                            return df.withColumn("_metadata", meta_struct)
                    raise

            orig_read_table = DataFrameReader.table

            def safe_read_table(self, tableName: str):
                try:
                    return orig_read_table(self, tableName)
                except Exception as e:
                    err_str = str(e)
                    if "REQUIRES_SINGLE_PART_NAMESPACE" in err_str or "CATALOG_NOT_FOUND" in err_str:
                        simplified = re.sub(r"\b(fmcg|fmcg_dev)\.", "", tableName, flags=re.IGNORECASE)
                        print(f"[Local Spark Emulation] spark.read.table adapted: {tableName} -> {simplified}")
                        return orig_read_table(self, simplified)
                    raise

            safe_schema._is_local_emulated = True  # type: ignore
            safe_load._is_local_emulated = True  # type: ignore
            safe_read_table._is_local_emulated = True  # type: ignore
            DataFrameReader.schema = safe_schema  # type: ignore
            DataFrameReader.load = safe_load  # type: ignore
            DataFrameReader.table = safe_read_table  # type: ignore
    except Exception:
        pass

    # Wrap DeltaTable.forName to safely adapt multi-part namespaces on local Spark
    try:
        from delta.tables import DeltaTable
        if not getattr(DeltaTable.forName, "_is_local_emulated", False):
            orig_forName = DeltaTable.forName

            def safe_forName(sparkSession, tableOrViewName):
                simplified = re.sub(r"\b(fmcg|fmcg_dev)\.", "", tableOrViewName, flags=re.IGNORECASE)
                try:
                    return orig_forName(sparkSession, tableOrViewName)
                except Exception as e:
                    err_str = str(e)
                    if "CATALOG_NOT_FOUND" in err_str or "REQUIRES_SINGLE_PART_NAMESPACE" in err_str or "DELTA_MISSING_DELTA_TABLE" in err_str or "TABLE_OR_VIEW_NOT_FOUND" in err_str:
                        print(f"[Local Spark Emulation] DeltaTable.forName adapted: {tableOrViewName} -> {simplified}")
                        try:
                            return orig_forName(sparkSession, simplified)
                        except Exception as inner_e:
                            inner_err = str(inner_e)
                            if "DELTA_MISSING_DELTA_TABLE" in inner_err or "TABLE_OR_VIEW_NOT_FOUND" in inner_err:
                                if "." in simplified:
                                    schema_name = simplified.split(".")[0]
                                    sparkSession.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                                ddl = None
                                if "dim_products" in simplified:
                                    ddl = "(product_code STRING, division STRING, category STRING, product STRING, variant STRING)"
                                elif "dim_gross_price" in simplified or "dim_pricing" in simplified:
                                    ddl = "(product_code STRING, price_inr DOUBLE, year INT)"
                                elif "dim_customer" in simplified:
                                    ddl = "(customer_code STRING, customer STRING, market STRING, platform STRING, channel STRING)"
                                elif "sb_fact_orders" in simplified:
                                    ddl = "(order_id STRING, date DATE, customer_code STRING, product_code STRING, product_id STRING, sold_quantity BIGINT)"
                                elif "fact_orders" in simplified:
                                    ddl = "(date DATE, product_code STRING, customer_code STRING, sold_quantity BIGINT)"
                                elif "silver" in simplified and "order" in simplified:
                                    ddl = "(order_id STRING, order_placement_date DATE, customer_id STRING, product_code STRING, product_id STRING, order_qty INT)"
                                elif "fact" in simplified or "order" in simplified:
                                    ddl = "(date DATE, product_code STRING, customer_code STRING, sold_quantity BIGINT)"
                                if ddl:
                                    try:
                                        sparkSession.sql(f"CREATE TABLE IF NOT EXISTS {simplified} {ddl} USING DELTA")
                                    except Exception as ddl_e:
                                        if "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in str(ddl_e):
                                            import shutil
                                            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
                                            parts = simplified.split(".")
                                            target_dir = os.path.join(repo_root, "spark-warehouse", f"{parts[0]}.db", parts[1]) if len(parts) > 1 else os.path.join(repo_root, "spark-warehouse", simplified)
                                            shutil.rmtree(target_dir, ignore_errors=True)
                                            sparkSession.sql(f"CREATE TABLE {simplified} {ddl} USING DELTA")
                                return orig_forName(sparkSession, simplified)
                            raise inner_e
                    raise

            safe_forName._is_local_emulated = True  # type: ignore
            DeltaTable.forName = safe_forName  # type: ignore
    except Exception:
        pass

    return spark


def init_notebook_context(target_globals: Optional[Dict[str, Any]] = None):
    """
    Ensures `spark`, `dbutils`, and `display` are defined in the caller's namespace.
    If already present (e.g. running on Databricks), existing instances are preserved.
    """
    if target_globals is None:
        frame = sys._getframe(1)
        target_globals = frame.f_globals

    # 1. dbutils
    if "dbutils" not in target_globals or target_globals["dbutils"] is None:
        target_globals["dbutils"] = MockDBUtils()
    dbutils = target_globals["dbutils"]

    # 2. spark
    if "spark" not in target_globals or target_globals["spark"] is None:
        target_globals["spark"] = get_or_create_local_spark()
    spark = target_globals["spark"]

    # 3. display
    if "display" not in target_globals or target_globals["display"] is None:
        target_globals["display"] = mock_display
    display = target_globals["display"]

    # 4. Hook IPython %run resolver and %sql magic for Databricks cross-notebook execution
    register_run_resolver()
    register_sql_magic()

    return spark, dbutils, display
