# 🚨 Operational Runbook: On-Call Pipeline Triage

This runbook guides data engineers on diagnosing, mitigating, and recovering from failures in the **AtliQ FMCG (`Atlikon_DE`)** Medallion pipeline.

---

## 1. Immediate Failure Triage (First 5 Minutes)

When an alert fires from the Databricks Workflow (`fmcg_medallion_pipeline`):

1. **Identify the Failing Task:**
   Open the Databricks Workflow run console and inspect which DAG task failed:
   * `setup_catalog` / `dim_date`
   * `products` / `customers` / `pricing`
   * `incremental_orders`

2. **Query the Audit Logs:**
   Run the following query in Databricks SQL or a scratch notebook:
   ```sql
   SELECT run_id, notebook, stage, table, status, message, logged_at
   FROM fmcg.bronze._pipeline_logs
   WHERE logged_at >= current_timestamp() - INTERVAL 24 HOURS
     AND status = 'failure'
   ORDER BY logged_at DESC;
   ```

---

## 2. Layer-by-Layer Remediation Guide

### A. Failure in Bronze Ingestion (Source / S3)
* **Root Causes:** Network timeout with AWS S3, malformed CSV with unescaped delimiters, or schema mismatch.
* **Remediation Steps:**
  1. Identify offending file from `_metadata.file_name` in the log.
  2. Inspect S3 landing folder: `s3://spartsbar-2355/<data_source>/landing/`.
  3. If a corrupt file exists:
     * Move the corrupted file to a quarantine directory: `s3://spartsbar-2355/<data_source>/quarantine/`.
     * Notify the data provider / upstream vendor.
  4. In Databricks Workflows, click **"Repair Run"** and rerun **only the failed task**.

### B. Failure in Silver Data Quality Gates
* **Root Causes:** Blocking validation check failed in `run_quality_checks` (e.g. `order_qty IS NULL` or unparseable dates).
* **Remediation Steps:**
  1. Look up the specific check name in the log message (e.g., `[BLOCKING] Data Quality Check 'order_qty_positive' failed on 14 rows`).
  2. Inspect the raw batch in Bronze:
     ```sql
     SELECT * FROM fmcg.bronze.orders WHERE order_qty <= 0 OR order_qty IS NULL;
     ```
  3. Determine if source business logic changed or if the check expression needs adjustment.
  4. Fix the source records or adjust rule definitions in `src/data_quality.py`.

### C. Failure in Gold Upsert / Merge
* **Root Causes:** Schema evolution conflict, duplicate natural key collision, or concurrent write conflict.
* **Remediation Steps:**
  1. If concurrent writes conflicted: Delta Lake automatically retries. If manual retry is needed, rerun the task.
  2. Check merge keys: Verify that `(product_code, year)` in pricing or `(date, product_code, customer_code)` in orders has no duplicate keys in the source view.

---

## 3. Disaster Recovery & Rollback (Delta Time Travel)

If a bad batch accidentally committed corrupted data into `fmcg.gold.fact_orders`:

1. **Check Table History:**
   ```sql
   DESCRIBE HISTORY fmcg.gold.fact_orders;
   ```
   Identify the `version` immediately preceding the failing `run_id`.

2. **Restore Table Instantly:**
   ```sql
   RESTORE TABLE fmcg.gold.fact_orders TO VERSION AS OF <last_good_version>;
   ```

3. **Re-run Incremental Load:**
   After rectifying the bad source batch, trigger the incremental task with `availableNow=True`.
