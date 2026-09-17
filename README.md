# 🛒 AtliQ FMCG Data Lakehouse & Integration Pipeline (`Atlikon_DE`)

[![CI](https://github.com/Nithin1138/Atlikon_DE/actions/workflows/ci.yml/badge.svg)](https://github.com/Nithin1138/Atlikon_DE/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/PyTest-12%20Passed-brightgreen?logo=pytest&logoColor=white)](tests/)
[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark%203.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20Storage-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![AWS S3](https://img.shields.io/badge/AWS-S3%20Storage-569A31?logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-blue)](https://www.databricks.com/glossary/medallion-architecture)
[![Checklist](https://img.shields.io/badge/Production%20Readiness-100%25%20Audit-success)](docs/ProductionReadinessChecklist.md)

An enterprise-grade **Databricks**, **PySpark**, and **Delta Lake** ETL lakehouse pipeline implementing the **Medallion Architecture** to solve a post-acquisition data integration challenge for **AtliQ FMCG**.

The pipeline ingests, cleanses, standardizes, validates, and merges operational sales, product, pricing, and customer datasets from an acquired retail/FMCG subsidiary (**Sports Bar**) into AtliQ's centralized corporate lakehouse.

---

## 📌 Business Scenario & Problem Statement

Following the corporate acquisition of **Sports Bar**, AtliQ FMCG required unified analytical reporting across all retail chains and regional markets. However, the acquired company’s source data suffered from multiple data hygiene and architecture discrepancies:

1. **Grain Mismatch**: The subsidiary records orders at a **daily transactional grain**, whereas the parent enterprise fact table operates at a **monthly aggregated grain** (`month_start`, `product_code`, `customer_code`).
2. **Key Inconsistencies**: The subsidiary used internal, non-standard product and customer identifiers (with missing or corrupted non-numeric values), requiring deterministic surrogate keys (SHA-256 hashing) and lookup joins.
3. **Data Quality Issues**:
   - String dates stored across conflicting formats (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `Tuesday, July 01, 2025`).
   - Typos in geographic regions (e.g., `"Bengaluruu"` instead of `"Bengaluru"`).
   - Erroneous negative pricing values and non-numeric price artifacts.
   - Embedded packaging variant specifications in product title strings (e.g., `"Product Name (250ml)"`).
4. **Idempotency & Incremental Sync**: Ingestion must support both **full historical backfills** and **daily incremental loads** without duplicating records or double-counting monthly quantities in the parent enterprise fact table.

---

## 🏗️ Architecture: Medallion Lakehouse Pattern

The pipeline is organized using Databricks Unity Catalog under the `fmcg` (or `fmcg_dev`) catalog across three distinct medallion layers:

```
                  ┌────────────────────────────────────────┐
                  │    AWS S3 Landing Bucket (CSV)         │
                  │    s3://spartsbar-2355/<data_source>   │
                  └───────────────────┬────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 🥉 BRONZE LAYER (fmcg.bronze)                                               │
│ • Raw data ingestion with explicit StructType schemas (no inferSchema scan) │
│ • Audit metadata injection (_metadata.file_name, file_size, read_timestamp) │
│ • Delta Change Data Feed (CDF) enabled                                      │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 🥈 SILVER LAYER (fmcg.silver)                                               │
│ • Robust date parsing (F.coalesce + F.try_to_date across 4+ formats)        │
│ • Regex cleansing (extracting package variants: (250ml) -> 'variant')       │
│ • Deterministic surrogate key generation via SHA-256 (product_code)         │
│ • Fallback handling for corrupted foreign keys ('999999')                   │
│ • Price validation (negative value rectification, non-numeric scrubbing)    │
│ • Optimized broadcast joins against small dimension tables                  │
│ • Deduplication and dimension enrichment                                    │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 🥇 GOLD LAYER (fmcg.gold)                                                   │
│ • Star schema dimensional modeling:                                         │
│   - sb_dim_customers, sb_dim_products, sb_dim_gross_price, sb_fact_orders   │
│ • Pre-merge Data Quality assertions (blocking + non-blocking gates)         │
│ • Window ranking for annual latest pricing (partitionBy product_code, year) │
│ • Daily-to-monthly grain aggregation (F.trunc("date", "MM"))                │
│ • ACID Upsert / MERGE INTO parent enterprise tables:                        │
│   - fmcg.gold.dim_customers                                                 │
│   - fmcg.gold.dim_products                                                  │
│   - fmcg.gold.dim_gross_price  [Fixed: multi-year history preservation]     │
│   - fmcg.gold.fact_orders                                                   │
│ • Small-file compaction and Z-ORDER indexing                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```text
Atlikon_DE/
├── src/                                  # Modular Python package (clean, reusable, testable)
│   ├── __init__.py
│   ├── config.py                         # Dynamic dev/prod configuration resolver
│   ├── schemas.py                        # Explicit StructType schemas for all sources
│   ├── transformations.py                # Pure PySpark transformation & cleansing functions
│   ├── data_quality.py                   # Blocking and non-blocking quality assertion engine
│   └── audit.py                          # Audit columns and structured _pipeline_logs logger
│
├── tests/                                # Automated PyTest suite (runs locally and in CI)
│   ├── __init__.py
│   ├── conftest.py                       # Local SparkSession fixture
│   ├── test_transformations.py           # Unit tests for transformations & grain recompute
│   └── test_data_quality.py              # Unit tests for quality check assertions
│
├── 1_setup/                              # Databricks bootstrap & shared environment setup
│   ├── 1_setup_catalog.ipynb             # Parameterized Unity Catalog & schema initialization
│   ├── dim_date_table_creation.ipynb     # Shared monthly calendar dimension generator
│   └── utilities.ipynb                   # Environment config & modular function loader
│
├── 2_dimension_data_processing/          # Dimension ETL notebooks
│   ├── customer_data_processing.ipynb    # Customer Bronze/Silver/Gold ETL & parent upsert
│   ├── 2_products_data_processing.ipynb  # Product cleansing, variant extraction & SHA-256
│   └── 3_pricing_data_processing.ipynb   # [Bug-Fixed] Multi-year pricing resolution & upsert
│
├── 3_fact_dat_processing/               # Fact ETL notebooks
│   ├── 1_full_load_fact.ipynb            # Historical batch order load & monthly aggregation
│   └── 2_incremental_load_fact.ipynb     # Staging ingestion, dynamic month recalc & merge
│
├── workflows/                            # Orchestration specifications
│   └── fmcg_pipeline.yml                 # Databricks Workflow Job definition (DAG & alerts)
│
├── .github/workflows/                    # Continuous Integration
│   └── ci.yml                            # GitHub Actions workflow running PyTest on PRs
│
├── docs/                                 # Enterprise Documentation Suite
│   ├── BusinessRules.md                  # Every judgment call & business rule documented
│   ├── Architecture.md                   # End-to-end & incremental sequence diagrams
│   ├── Runbook.md                        # On-call operational triage playbook
│   ├── DataDictionary.md                 # Complete 18-table enterprise schema catalog
│   └── ProductionReadinessChecklist.md   # 20-item production audit
│
└── README.md                             # Project overview & execution guide
```

---

## ⚡ Key Production Optimizations & Bug Fixes

### 1. Multi-Year Pricing Bug Fix (`dim_gross_price`)
* **The Problem:** The original merge condition matched only on `target.product_code = source.product_code`, omitting `year`. Every incremental yearly load silently overwrote previous years' prices, collapsing the table into a single year and destroying price history.
* **The Fix:** Corrected the merge condition in `3_pricing_data_processing.ipynb`:
  ```python
  condition="target.product_code = source.product_code AND target.year = source.year"
  ```

### 2. Elimination of Hardcoded Personal Paths (100% Portability)
* **The Problem:** Notebooks contained hardcoded paths referencing a personal workspace (`%run /Workspace/Users/alekhyarayavarapu@gmail.com/...`), preventing execution in any other workspace or CI pipeline.
* **The Fix:** Refactored all notebooks to use dynamic repo root detection and relative paths (`%run ../1_setup/utilities`) compatible with Databricks Repos / Git Folders.

### 3. Explicit Schemas vs `inferSchema=True`
* Replaced expensive `inferSchema=True` scans across raw CSVs with explicit `StructType` definitions in `src/schemas.py`. This eliminates an extra full dataset pass over S3 and prevents silent type-drift errors.

### 4. Broadcast Joins (`broadcast(df_products)`)
* Applied Spark `broadcast()` hints when joining high-volume orders and pricing datasets against the compact products dimension table (`silver.products`). This eliminates cluster-wide shuffle phases, reducing network I/O and job latency.

### 5. Compaction and Z-Ordering (`OPTIMIZE`)
* Scheduled periodic Delta `OPTIMIZE ... ZORDER BY (date, product_code, customer_code)` on high-churn tables (`fact_orders`, `dim_products`) to solve the "small-file problem" caused by daily micro-merges.

### 6. Automated Unit Testing & CI/CD
* Extracted inline notebook transformations into pure functions (`src/transformations.py`).
* Implemented 12 automated unit tests (`tests/test_transformations.py`, `tests/test_data_quality.py`) running in 5 seconds via `pytest` and automatically verified on pull requests via GitHub Actions.

---

## 🔬 Enterprise Documentation Suite

Detailed operational documentation is available in the [`docs/`](docs/) directory:

| Document | Description |
|:---|:---|
| **[`docs/BusinessRules.md`](docs/BusinessRules.md)** | Full specification of all data cleaning judgment calls (negative price flipping, sentinels, annual price resolution, city whitelists). |
| **[`docs/Architecture.md`](docs/Architecture.md)** | End-to-end Medallion data flow and sequence diagrams of the dynamic month-recomputation mechanism. |
| **[`docs/Runbook.md`](docs/Runbook.md)** | Step-by-step on-call triage procedures for task failures and Delta Lake Time Travel rollbacks. |
| **[`docs/DataDictionary.md`](docs/DataDictionary.md)** | Detailed catalog of all 18 tables, schema definitions, grains, and key constraints. |
| **[`docs/ProductionReadinessChecklist.md`](docs/ProductionReadinessChecklist.md)** | The complete 20-point production readiness checklist. |

---

## 🚀 How to Run Locally & In Databricks

### Running Tests Locally
Ensure Python 3.10+ and Java 11+ are installed:
```bash
# Run the complete test suite with PyTest
pytest tests/ -v
```

### Deploying & Executing in Databricks
1. **Clone into Databricks Repos:**
   In your Databricks workspace, create a new Repo pointing to your Git repository URL.
2. **Environment Parameter:**
   Configure the `env` widget to `"dev"` (targets `fmcg_dev` catalog and `spartsbar-2355-dev` bucket) or `"prod"` (targets `fmcg` catalog and `spartsbar-2355`).
3. **Execution Sequence:**
   * Run `1_setup/1_setup_catalog.ipynb`
   * Run `1_setup/dim_date_table_creation.ipynb`
   * Run Dimension notebooks: `2_products_data_processing.ipynb`, `customer_data_processing.ipynb`, `3_pricing_data_processing.ipynb`
   * Run Historical Fact Backfill: `3_fact_dat_processing/1_full_load_fact.ipynb`
   * Schedule Daily Incremental Workflow: `workflows/fmcg_pipeline.yml`
