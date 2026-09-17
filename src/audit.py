"""
Pipeline Auditing & Structured Logging Module.
Enables end-to-end data lineage tracking and persistent execution observability via _pipeline_logs.
"""

import uuid
from typing import Optional
from pyspark.sql import DataFrame, SparkSession, functions as F


def generate_run_id() -> str:
    """Generates a unique UUID4 string for the pipeline execution run."""
    return str(uuid.uuid4())


def add_audit_columns(
    df: DataFrame,
    run_id: Optional[str] = None,
    pipeline_version: str = "1.0"
) -> DataFrame:
    """
    Standardizes audit columns across Medallion layers.
    Enables tracking of which pipeline run created or updated a given row.
    """
    if run_id is None:
        run_id = generate_run_id()

    return (
        df
        .withColumn("created_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("pipeline_version", F.lit(pipeline_version))
    )


def log_pipeline_event(
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
    run_id: str,
    notebook: str,
    stage: str,
    table: str,
    row_count: int,
    status: str,
    message: str = ""
) -> None:
    """
    Appends a structured audit event to the persistent Delta logging table `{catalog}.{bronze_schema}._pipeline_logs`.
    """
    log_data = [{
        "run_id": run_id,
        "notebook": notebook,
        "stage": stage,
        "table": table,
        "row_count": int(row_count),
        "status": status,
        "message": message,
    }]

    log_df = (
        spark.createDataFrame(log_data)
        .withColumn("logged_at", F.current_timestamp())
    )

    try:
        (
            log_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{catalog}.{bronze_schema}._pipeline_logs")
        )
    except Exception as exc:
        # Fallback to standard python logging if delta write fails (e.g. table not yet initialized)
        import logging
        logging.getLogger("fmcg_pipeline.audit").warning(
            f"Failed writing to _pipeline_logs: {exc}. Log payload: {log_data}"
        )
