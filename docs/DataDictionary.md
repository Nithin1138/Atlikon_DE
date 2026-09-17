# 📚 Enterprise Data Dictionary & Schema Reference

This catalog details all 18 tables maintained across the **AtliQ FMCG (`Atlikon_DE`)** Medallion Lakehouse.

---

## 1. Gold Enterprise Star Schema (Parent Unified Layer)

### Table: `fmcg.gold.fact_orders`
* **Description:** Unified enterprise sales orders fact table aggregated to monthly corporate reporting grain.
* **Grain:** 1 row per `(date, product_code, customer_code)`.
* **Columns:**
  | Column Name | Data Type | Key | Description |
  |:---|:---|:---|:---|
  | `date` | `DATE` | PK / FK | First day of the calendar month (`yyyy-MM-01`). References `dim_date.month_start_date`. |
  | `product_code` | `STRING` | PK / FK | Deterministic SHA-256 surrogate hash of product name. References `dim_products.product_code`. |
  | `customer_code` | `STRING` | PK / FK | Normalized enterprise customer identifier. References `dim_customers.customer_code`. |
  | `sold_quantity` | `BIGINT` | Measure | Total units sold across all daily orders within the month. |
  | `created_at` | `TIMESTAMP` | Audit | Timestamp when record was loaded/updated. |
  | `run_id` | `STRING` | Audit | UUID representing the pipeline job execution. |

---

### Table: `fmcg.gold.dim_products`
* **Description:** Conformed master product dimension catalog.
* **Grain:** 1 row per `product_code`.
* **Columns:**
  | Column Name | Data Type | Key | Description |
  |:---|:---|:---|:---|
  | `product_code` | `STRING` | PK | Deterministic SHA-256 surrogate hash of sanitized product name. |
  | `division` | `STRING` | Attribute | Conformed corporate division (e.g. `"Nutrition Bars"`, `"Breakfast Foods"`). |
  | `category` | `STRING` | Attribute | Product category (e.g. `"Protein Bars"`). |
  | `product` | `STRING` | Attribute | Cleaned product display name. |
  | `variant` | `STRING` | Attribute | Packaging size/flavor extracted from parenthesis. |

---

### Table: `fmcg.gold.dim_gross_price`
* **Description:** Annual product pricing index.
* **Grain:** 1 row per `(product_code, year)`.
* **Columns:**
  | Column Name | Data Type | Key | Description |
  |:---|:---|:---|:---|
  | `product_code` | `STRING` | PK / FK | SHA-256 hash referencing `dim_products.product_code`. |
  | `year` | `INT` | PK | Fiscal / calendar year. |
  | `price_inr` | `DOUBLE` | Measure | Latest valid non-zero unit price in Indian Rupees (INR) for the year. |

---

### Table: `fmcg.gold.dim_customers`
* **Description:** Conformed master customer dimension.
* **Grain:** 1 row per `customer_code`.
* **Columns:**
  | Column Name | Data Type | Key | Description |
  |:---|:---|:---|:---|
  | `customer_code` | `STRING` | PK | Canonical customer identifier. |
  | `customer` | `STRING` | Attribute | Composite customer label (`customer_name` + `city`). |
  | `city` | `STRING` | Attribute | Cleaned city location (validated against whitelist). |
  | `market` | `STRING` | Attribute | Market geography (`"India"`). |
  | `platform` | `STRING` | Attribute | Originating platform (`"Sports Bar"`). |
  | `channel` | `STRING` | Attribute | Distribution channel (`"Acquisition"`). |

---

### Table: `fmcg.gold.dim_date`
* **Description:** Pre-computed monthly enterprise calendar dimension.
* **Grain:** 1 row per month start.
* **Columns:**
  | Column Name | Data Type | Key | Description |
  |:---|:---|:---|:---|
  | `month_start_date`| `DATE` | PK | First day of the calendar month (`yyyy-MM-01`). |
  | `date_key` | `INT` | Key | Integer surrogate key (`yyyyMM`, e.g. `202401`). |
  | `year` | `INT` | Attribute | Calendar year (`2024`, `2025`). |
  | `month_name` | `STRING` | Attribute | Full month name (`"January"`). |
  | `month_short_name`| `STRING` | Attribute | Abbreviated month (`"Jan"`). |
  | `quarter` | `STRING` | Attribute | Quarter indicator (`"Q1"`-`"Q4"`). |
  | `year_quarter` | `STRING` | Attribute | Combined year and quarter (`"2024-Q1"`). |

---

## 2. Child Subsidiary Star Schema (Gold Child Layer)

* **`fmcg.gold.sb_fact_orders`**: Subsidiary transactional sales orders table at **daily order grain** (`order_id`, `date`, `customer_code`, `product_code`, `sold_quantity`).
* **`fmcg.gold.sb_dim_products`**: Subsidiary product dimension with original numeric `product_id` preserved alongside `product_code`.
* **`fmcg.gold.sb_dim_gross_price`**: Subsidiary pricing dimension before corporate merge.
* **`fmcg.gold.sb_dim_customers`**: Subsidiary customer dimension with raw `customer_id`.

---

## 3. Silver Layer (Cleaned & Conformed)

* **`fmcg.silver.orders`**: Cleansed orders with parsed dates and numeric sentinels.
* **`fmcg.silver.products`**: Cleansed products with extracted variants.
* **`fmcg.silver.gross_price`**: Month-by-month historical pricing with rectified negative values.
* **`fmcg.silver.customers`**: Cleansed customer entries with standardized city names.

---

## 4. Bronze Layer & Metadata Logging

* **`fmcg.bronze.orders`**, **`fmcg.bronze.products`**, **`fmcg.bronze.gross_price`**, **`fmcg.bronze.customers`**: Raw ingested records enriched with `_metadata.file_name`, `_metadata.file_size`, and `read_timestamp`.
* **`fmcg.bronze._pipeline_logs`**: Persistent audit table tracking `run_id`, `notebook`, `stage`, `table`, `row_count`, `status`, `message`, and `logged_at`.
