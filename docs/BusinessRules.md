# 📋 Business Rules & Transformation Logic Specification

This document explicitly surfaces every business assumption and judgment call implemented across the **Atlikon_DE** Medallion data pipeline.

---

## 1. Pricing & Revenue Rules

### Rule 1.1: Negative Price Inversion (`clean_price_column`)
* **Logic:** When `gross_price` is a negative valid number (e.g. `"-180.50"`), it is multiplied by `-1.0` to become positive (`180.50`).
* **Business Rationale:** In the subsidiary's legacy ERP, operators occasionally prefixed prices with a dash or minus sign as a typo rather than entering a legitimate credit note.
* **Trade-off & Governance:** True discounts or credit memos must NOT be passed through the standard price ingestion; they should be recorded in a dedicated promotional adjustments table.

### Rule 1.2: Unparseable Price Defaulting
* **Logic:** If `gross_price` contains non-numeric strings, blanks, or corrupted text (e.g. `"FREE_SAMPLE"`, `"N/A"`), it is defaulted to `0.0`.
* **Business Rationale:** Prevents schema casting crashes and avoids dropping entire product rows from downstream dimension joins.
* **Audit Impact:** Filtered downstream in pricing window ranking so non-zero prices are always preferred over `0.0` sentinels.

### Rule 1.3: Annual Price Resolution (Window Ranking)
* **Logic:** `dim_gross_price` stores exactly **one price per product per year**, resolved via:
  ```python
  Window.partitionBy("product_code", "year").orderBy(col("is_zero"), col("month").desc())
  ```
* **Business Rationale:** Conforms subsidiary pricing to AtliQ parent enterprise dimensional reporting, which operates on an annual gross pricing index.
* **Trade-off:** Within-year price fluctuations are smoothed out at Gold. Any analysis requiring month-by-month historical pricing must query `fmcg.silver.gross_price`.

---

## 2. Customer & Geographic Normalization

### Rule 2.1: City Typos & Whitelist Protection (`clean_cities`)
* **Logic:**
  1. A curated dictionary corrects common spelling variants:
     * `"Bengaluruu"`, `"Bengalore"` ➔ `"Bengaluru"`
     * `"Hyderabadd"`, `"Hyderbad"` ➔ `"Hyderabad"`
     * `"NewDelhi"`, `"NewDheli"`, `"NewDelhee"` ➔ `"New Delhi"`
  2. Any city string not contained in `["Bengaluru", "Hyderabad", "New Delhi"]` is explicitly set to `NULL`.
* **Business Rationale:** Ensures marketing dashboards and regional BI aggregations only display validated geographical markets, preventing misspelled cities from creating fragmented report buckets.

### Rule 2.2: Hardcoded Customer City Fallback
* **Logic:** A manual lookup dictionary patches 4 historical high-value customer accounts where the raw file completely omitted city metadata.
* **Business Rationale:** Preserves regional revenue attribution for top enterprise clients.

---

## 3. Key Integrity & Referential Rules

### Rule 3.1: Sentinel ID for Corrupted Foreign Keys (`clean_sentinel_id`)
* **Logic:** Non-numeric identifiers in `customer_id` or `product_id` (e.g., `"abc-123"`, blanks) are transformed to `'999999'`.
* **Business Rationale:** Dropping these rows at Bronze/Silver would silently discard sales revenue from corporate fact totals. Converting to `'999999'` keeps revenue intact while surfacing unlinked records for audit review.

### Rule 3.2: Deterministic Product Surrogate Key (`generate_product_hash`)
* **Logic:** `product_code` is computed as `sha2(product_name, 256)`.
* **Business Rationale:** Provides an idempotent, globally reproducible join key across heterogeneous systems without requiring an expensive cross-system sequence coordinator.
* **Trade-off:** If a product name is formally rebranded or renamed, a new hash is produced. This is treated as a new product version.

---

## 4. Grain Realignment & Idempotent Incremental Loads

### Rule 4.1: Daily to Monthly Grain Reduction
* **Logic:** Subsidiary order dates are truncated to the first day of the calendar month:
  ```python
  month_start = F.trunc("date", "MM")  # e.g., '2025-07-14' -> '2025-07-01'
  ```
  Transactions are grouped by `(month_start, product_code, customer_code)` and `sold_quantity` is summed.
* **Business Rationale:** Matches parent enterprise reporting standard in `fmcg.gold.fact_orders`.

### Rule 4.2: Dynamic Affected-Month Recalculation
* **Logic:** During daily incremental runs:
  1. Inspect the incoming daily batch to find which `month_start` periods were touched.
  2. Pull **all existing daily rows** from `fmcg.gold.sb_fact_orders` for those specific touched months.
  3. Recompute complete monthly totals from scratch and execute a targeted Delta `MERGE`.
* **Business Rationale:** Prevents double-counting or partial-sum errors that occur when new daily rows arrive for a month that was already previously loaded.
