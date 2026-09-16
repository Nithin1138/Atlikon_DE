# 🛒 AtliQ FMCG Data Lakehouse & Integration Pipeline (`Atlikon_DE`)

[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark%203.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20Storage-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![AWS S3](https://img.shields.io/badge/AWS-S3%20Storage-569A31?logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-blue)](https://www.databricks.com/glossary/medallion-architecture)

An enterprise-grade **Databricks**, **PySpark**, and **Delta Lake** ETL pipeline implementing the **Medallion Architecture** to solve a post-acquisition data integration challenge for **AtliQ FMCG**.

The pipeline ingests, cleanses, standardizes, and merges operational sales, product, pricing, and customer datasets from an acquired retail/FMCG subsidiary (**Sports Bar**) into AtliQ's centralized corporate lakehouse.

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

The pipeline is organized using Databricks Unity Catalog under the `fmcg` catalog across three distinct medallion layers:

```
                  ┌────────────────────────────────────────┐
                  │    AWS S3 Landing Bucket (CSV)         │
                  │    s3://spartsbar-2355/<data_source>   │
                  └───────────────────┬────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 🥉 BRONZE LAYER (fmcg.bronze)                                               │
│ • Raw data ingestion with schema inference                                   │
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
│ • Deduplication and dimension enrichment (inner joins)                      │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 🥇 GOLD LAYER (fmcg.gold)                                                   │
│ • Star schema dimensional modeling:                                         │
│   - sb_dim_customers, sb_dim_products, sb_dim_gross_price, sb_fact_orders   │
│ • Window ranking for annual latest pricing (partitionBy product_code, year) │
│ • Daily-to-monthly grain aggregation (F.trunc("date", "MM"))                │
│ • ACID Upsert / MERGE INTO parent enterprise tables:                        │
│   - fmcg.gold.dim_customers                                                 │
│   - fmcg.gold.dim_products                                                  │
│   - fmcg.gold.dim_gross_price                                               │
│   - fmcg.gold.fact_orders                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```text
Atlikon_DE/
├── 1_setup/
│   ├── 1_setup_catalog.ipynb          # Unity Catalog & Medallion schema initialization
│   ├── dim_date_table_creation.ipynb  # Monthly calendar dimension generator with date keys
│   └── utilities.ipynb               # Shared pipeline constants and schema references
│
├── 2_dimension_data_processing/
│   ├── customer_data_processing.ipynb # Customer Bronze/Silver/Gold ETL & parent upsert
│   ├── 2_products_data_processing.ipynb # Product cleansing, variant extraction & SHA-256 hashing
│   └── 3_pricing_data_processing.ipynb  # Multi-format date parsing, price sanitation & window rank
│
└── 3_fact_dat_processing/
    ├── 1_full_load_fact.ipynb         # Historical batch order load, monthly aggregation & merge
    └── 2_incremental_load_fact.ipynb  # Incremental load with staging, dynamic month recalc & archival
```

---

## 🔬 Pipeline Deep-Dive & Notebook Breakdown

### 1. Environment & Setup (`1_setup/`)
* **`1_setup_catalog.ipynb`**:
  Initializes the Unity Catalog `fmcg` and creates the three medallion schemas:
  ```sql
  CREATE CATALOG IF NOT EXISTS fmcg;
  CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
  CREATE SCHEMA IF NOT EXISTS fmcg.silver;
  CREATE SCHEMA IF NOT EXISTS fmcg.gold;
  ```
* **`dim_date_table_creation.ipynb`**:
  Generates a shared monthly calendar dimension (`fmcg.gold.dim_date`) using `sequence(to_date('2024-01-01'), to_date('2025-12-01'), interval 1 month)`. Adds analytical attributes:
  - `date_key` (e.g., `202401`)
  - `year`, `month_name`, `month_short_name`
  - `quarter` (`Q1`-`Q4`), `year_quarter` (`2024-Q1`)
* **`utilities.ipynb`**:
  Centralizes configuration parameters (`bronze_schema = "bronze"`, etc.) referenced across notebooks via `%run`.

---

### 2. Dimension Data Processing (`2_dimension_data_processing/`)

#### A. Customer Dimension (`customer_data_processing.ipynb`)
* **Ingestion**: Ingests customer master CSVs from S3 landing zone with file metadata.
* **Transformations**:
  - Drops duplicate customer entries.
  - Cleans city and state text (e.g., corrects `"Bengaluruu"` to `"Bengaluru"`).
  - Cleans non-numeric customer identifiers with fallback assignments (`999999`).
  - Adds enterprise metadata: `market = 'India'`, `platform = 'Sports Bar'`, `channel = 'Acquisition'`.
* **Merge Strategy**:
  Upserts into child dimension `fmcg.gold.sb_dim_customers`, followed by a Delta Lake `MERGE` into the parent table `fmcg.gold.dim_customers`:
  ```python
  delta_table.alias("target").merge(
      source=df_child_customers.alias("source"),
      condition="target.customer_code = source.customer_code"
  ).whenMatchedUpdate(
      set={"customer": "source.customer", "city": "source.city", ...}
  ).whenNotMatchedInsert(
      values={...}
  ).execute()
  ```

#### B. Product Dimension (`2_products_data_processing.ipynb`)
* **Regex Extraction**:
  Extracts packaging variants from product names using regex:
  ```python
  df_silver = df_silver.withColumn(
      "variant",
      F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
  )
  ```
* **Deterministic Hashing**:
  Computes SHA-256 hashes on sanitized product names to generate collision-resistant `product_code` keys compatible with parent enterprise joins:
  ```python
  df_silver = df_silver.withColumn(
      "product_code",
      F.sha2(F.col("product_name").cast("string"), 256)
  )
  ```
* **ID Normalization**: Invalid or corrupted non-numeric `product_id` values are replaced with fallback `999999` to ensure referential integrity in fact joins.
* **Parent Merge**: Upserts into `fmcg.gold.dim_products` matching on `product_code`.

#### C. Pricing Dimension (`3_pricing_data_processing.ipynb`)
* **Multi-Format Date Normalization**:
  Consolidates inhomogeneous date strings into standard dates via `F.coalesce`:
  ```python
  df_silver = df_bronze.withColumn(
      "month",
      F.coalesce(
          F.try_to_date(F.col("month"), "yyyy/MM/dd"),
          F.try_to_date(F.col("month"), "dd/MM/yyyy"),
          F.try_to_date(F.col("month"), "yyyy-MM-dd"),
          F.try_to_date(F.col("month"), "dd-MM-yyyy")
      )
  )
  ```
* **Price Cleansing**:
  Validates numeric formats via regex, corrects inverted negative prices (`-1 * price`), and converts non-numeric records to `0`.
* **Product Code Enrichment**:
  Performs an inner join with `fmcg.silver.products` on `product_id` to attach `product_code`.
* **Annual Pricing Window Function**:
  Retrieves the latest valid price per product per year using PySpark window ranking:
  ```python
  w = Window.partitionBy("product_code", "year").orderBy(F.col("is_zero"), F.col("month").desc())
  df_gold_latest_price = df_gold_price.withColumn("rnk", F.row_number().over(w)).filter(F.col("rnk") == 1)
  ```
* **Parent Merge**: Merges into `fmcg.gold.dim_gross_price` on `product_code`.

---

### 3. Fact Data Processing (`3_fact_dat_processing/`)

#### A. Full Historical Load (`1_full_load_fact.ipynb`)
1. **Raw Ingestion**: Ingests all historical CSV files from `landing_path/*.csv`.
2. **Cleansing & Parsing**:
   - Filters null order quantities.
   - Replaces non-numeric `customer_id` with fallback `'999999'`.
   - Strips leading weekday strings (e.g., `"Tuesday, July 01, 2025"` ➔ `"July 01, 2025"`).
   - Normalizes order dates with `F.coalesce(F.try_to_date(...))`.
   - Drops duplicate transactions on natural compound key `["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]`.
3. **Dimension Enrichment**: Joins with `fmcg.silver.products` to fetch canonical `product_code`.
4. **Child Silver & Gold Upsert**: Upserts transactions into `fmcg.silver.orders` and `fmcg.gold.sb_fact_orders`.
5. **Grain Realignment (Daily ➔ Monthly)**:
   Aggregates daily sales to monthly corporate fact grain:
   ```python
   df_monthly = (
       df_child
       .withColumn("month_start", F.trunc("date", "MM"))
       .groupBy("month_start", "product_code", "customer_code")
       .agg(F.sum("sold_quantity").alias("sold_quantity"))
       .withColumnRenamed("month_start", "date")
   )
   ```
6. **Parent Fact Merge**: Merges aggregated monthly sales into `fmcg.gold.fact_orders` matching on `(date, product_code, customer_code)`.

#### B. Daily Incremental Load (`2_incremental_load_fact.ipynb`)
1. **Isolated Ingestion**: Ingests only new incoming files into an isolated staging table (`fmcg.bronze.staging_orders`).
2. **Automated Archival**: Moves ingested files from the landing directory to a processed directory (`dbutils.fs.mv`).
3. **Staging Silver Transformation**: Applies cleansing transformations to the incoming batch and saves to `fmcg.silver.staging_orders`.
4. **Child Lakehouse Merge**: Performs an incremental Delta merge into `fmcg.silver.orders` and `fmcg.gold.sb_fact_orders`.
5. **Dynamic Affected-Month Recalculation**:
   To avoid corrupting monthly aggregates with partial updates, the pipeline:
   - Identifies distinct months impacted by the incoming incremental batch.
   - Extracts all transactions for those affected months from the complete child table.
   - Recalculates full monthly sums for only the affected periods.
   - Executes an idempotent merge into `fmcg.gold.fact_orders`:
   ```python
   gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
   gold_parent_delta.alias("parent_gold").merge(
       df_monthly_recalc.alias("child_gold"),
       "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
   ```
6. **Staging Teardown**: Drops staging tables (`staging_orders`) to free cluster resources and maintain isolation.

---

## 📊 Star Schema Data Model

```text
               ┌──────────────────────────────┐
               │    fmcg.gold.dim_date        │
               ├──────────────────────────────┤
               │ PK  month_start_date (date)  │
               │     date_key (int)           │
               │     year (int)               │
               │     month_name (string)      │
               │     quarter (string)         │
               └──────────────┬───────────────┘
                              │ 1
                              │
                              │ N
┌──────────────────────────┐  │  ┌──────────────────────────────┐  │  ┌──────────────────────────┐
│ fmcg.gold.dim_customers  │  │  │   fmcg.gold.fact_orders      │  │  │   fmcg.gold.dim_products │
├──────────────────────────┤  │  ├──────────────────────────────┤  │  ├──────────────────────────┤
│ PK customer_code (string)├──┼──┤ FK date (date)               │  └──┤ PK product_code (string) │
│    customer (string)     │  │  │ FK customer_code (string)    ├─────┤    division (string)     │
│    city (string)         │  └──┤ FK product_code (string)     │     │    category (string)     │
│    state (string)        │     │    sold_quantity (bigint)    │     │    product (string)      │
│    market (string)       │     └──────────────────────────────┘     │    variant (string)      │
│    platform (string)     │                                          └───────────┬──────────────┘
│    channel (string)      │                                                      │ 1
└──────────────────────────┘                                                      │
                                                                                  │ N
                                                                      ┌───────────┴──────────────┐
                                                                      │ fmcg.gold.dim_gross_price│
                                                                      ├──────────────────────────┤
                                                                      │ FK product_code (string) │
                                                                      │    year (string)         │
                                                                      │    price_inr (double)    │
                                                                      └──────────────────────────┘
```

---

## 🛠️ Key Technical Implementations & Design Decisions

| Technical Challenge | Implemented Solution | Benefit |
|:---|:---|:---|
| **Multiple Date Formats** | `F.coalesce(F.try_to_date(...))` across ISO, slash, hyphen, and written month styles. | Zero record drops due to malformed date formatting. |
| **Corrupted Foreign Keys** | `F.when(col.rlike('^[0-9]+$'), col).otherwise('999999')` | Preserves referential integrity and prevents fact record drops. |
| **Product Key Collisions** | `F.sha2(F.col("product_name"), 256)` surrogate keys | Deterministic, idempotent hash keys across heterogeneous source catalogs. |
| **Negative / Invalid Pricing** | Conditional regex matching, negative number rectification (`abs`), and fallback to 0. | Eliminates calculation distortions in gross sales metrics. |
| **Annual Price Deduplication** | `Window.partitionBy("product_code", "year").orderBy(is_zero, month.desc())` + `row_number()` | Ensures each product maps to its latest valid price per fiscal period. |
| **Daily to Monthly Grain** | `F.trunc("date", "MM")` + `groupBy` monthly aggregation before parent merge. | Seamless alignment between subsidiary daily operations and corporate monthly OLAP. |
| **Incremental Accuracy** | Dynamic extraction of modified months with partition recalculation. | Prevents double-counting or partial-month data skew during daily loads. |
| **Schema Evolution** | Delta Lake `.option("mergeSchema", "true")` and `.option("delta.enableChangeDataFeed", "true")`. | Seamless adaptation to source column modifications and downstream CDC tracking. |

---

## 🚀 Execution Guide

### Prerequisites
* Databricks Runtime (DBR) 13.0+ (Apache Spark 3.4+, Delta Lake 2.4+).
* Unity Catalog enabled on the target Databricks workspace.
* S3 access configured with IAM roles or cluster storage credentials for `s3://spartsbar-2355/`.

### Run Sequence
1. **Catalog & Setup**:
   - Execute `1_setup/1_setup_catalog.ipynb` to provision the `fmcg` catalog and schemas.
   - Execute `1_setup/dim_date_table_creation.ipynb` to seed the shared date dimension.
2. **Dimensions**:
   - Execute `2_dimension_data_processing/customer_data_processing.ipynb` (Parameters: `catalog="fmcg"`, `data_source="customers"`).
   - Execute `2_dimension_data_processing/2_products_data_processing.ipynb` (Parameters: `catalog="fmcg"`, `data_source="products"`).
   - Execute `2_dimension_data_processing/3_pricing_data_processing.ipynb` (Parameters: `catalog="fmcg"`, `data_source="gross_price"`).
3. **Facts**:
   - **Initial Backfill**: Execute `3_fact_dat_processing/1_full_load_fact.ipynb`.
   - **Daily Ingestion**: Schedule `3_fact_dat_processing/2_incremental_load_fact.ipynb` as a recurring Databricks Workflow job.
