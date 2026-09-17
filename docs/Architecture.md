# 🏛️ System Architecture & Data Lineage

This document provides a comprehensive technical architecture of the **AtliQ FMCG (`Atlikon_DE`) Medallion Data Integration Pipeline**.

---

## 1. End-to-End Medallion Architecture

The pipeline ingests heterogeneous CSV feeds from the acquired subsidiary (`s3://spartsbar-2355/`), cleanses and conforms records to AtliQ corporate dimensional models, and merges them into unified Gold star schema tables.

```mermaid
flowchart TD
    subgraph Sources["Raw S3 Landing Sources"]
        S1["s3://spartsbar-2355/products/*.csv"]
        S2["s3://spartsbar-2355/gross_price/*.csv"]
        S3["s3://spartsbar-2355/customers/*.csv"]
        S4["s3://spartsbar-2355/orders/landing/*.csv"]
    end

    subgraph Bronze["🥉 Bronze Layer (Raw Storage + Audit)"]
        B1["fmcg.bronze.products"]
        B2["fmcg.bronze.gross_price"]
        B3["fmcg.bronze.customers"]
        B4["fmcg.bronze.orders<br/>(append history)"]
        B4_S["fmcg.bronze.staging_orders<br/>(incremental batch)"]
    end

    subgraph Silver["🥈 Silver Layer (Cleaned & Conformed)"]
        SV1["fmcg.silver.products<br/>• SHA-256 product_code<br/>• Package variant extraction"]
        SV2["fmcg.silver.gross_price<br/>• Negative price inversion<br/>• Multi-format dates"]
        SV3["fmcg.silver.customers<br/>• City typos & whitelist<br/>• Sentinel 999999"]
        SV4["fmcg.silver.orders<br/>• Order qty > 0 check<br/>• Compound dedup"]
    end

    subgraph GoldChild["🥇 Gold Layer — Child Subsidiary Models"]
        GC1["fmcg.gold.sb_dim_products"]
        GC2["fmcg.gold.sb_dim_gross_price<br/>(Window ranking: 1 price/year)"]
        GC3["fmcg.gold.sb_dim_customers"]
        GC4["fmcg.gold.sb_fact_orders<br/>(Daily order grain)"]
    end

    subgraph GoldParent["🏆 Gold Layer — Unified AtliQ Enterprise Star Schema"]
        P_DATE["fmcg.gold.dim_date<br/>(Calendar spine)"]
        P1["fmcg.gold.dim_products"]
        P2["fmcg.gold.dim_gross_price"]
        P3["fmcg.gold.dim_customers"]
        P4["fmcg.gold.fact_orders<br/>(Monthly aggregated grain)"]
    end

    S1 --> B1 --> SV1 --> GC1
    S2 --> B2 --> SV2 --> GC2
    S3 --> B3 --> SV3 --> GC3
    S4 --> B4 & B4_S --> SV4 --> GC4

    GC1 -->|MERGE on product_code| P1
    GC2 -->|MERGE on product_code + year| P2
    GC3 -->|MERGE on customer_code| P3
    GC4 -->|Monthly Grain Aggregation & Recompute| P4

    P_DATE -.-> P4
    P1 -.-> P4
    P3 -.-> P4
    P2 -.-> P4
```

---

## 2. The Incremental Dynamic Month-Recomputation Flow

To resolve the grain mismatch (subsidiary records **daily** orders, whereas the parent enterprise fact table operates at a **monthly aggregated** grain), the incremental pipeline executes an idempotent month-scoped recalculation:

```mermaid
sequenceDiagram
    autonumber
    participant Landing as S3 Landing Zone
    participant Staging as Bronze/Silver Staging
    participant ChildGold as sb_fact_orders (Daily)
    participant Recalc as In-Memory Month Recalculation
    participant ParentGold as fact_orders (Monthly)

    Landing->>Staging: Ingest newly-arrived daily orders
    Staging->>ChildGold: MERGE new daily rows into child daily history
    Note over Staging,ChildGold: Child table now has complete history up to today

    Staging->>Recalc: Step 1: Identify distinct months touched (F.trunc('date', 'MM'))
    ChildGold->>Recalc: Step 2: Inner join with touched months to pull ALL daily rows for those months
    Note over Recalc: Step 3: Recompute complete monthly sums for touched periods only
    Recalc->>ParentGold: Step 4: MERGE replacement totals matching on (date, product_code, customer_code)
    Note over ParentGold: Updates stale monthly totals idempotently without double-counting!
```

---

## 3. Layer SLA and Architecture Principles

| Layer | Catalog & Schema | Ingestion / Write Mode | Optimizations |
|:---|:---|:---|:---|
| **Bronze** | `fmcg.bronze.*` | Append / Overwrite + Change Data Feed | Raw preservation, `_metadata` file audit, explicit `StructType` |
| **Silver** | `fmcg.silver.*` | Delta `MERGE` (Idempotent compound key) | Cleansed, sanitized, surrogate hashed, `broadcast(df_products)` |
| **Gold (Child)** | `fmcg.gold.sb_*` | Delta `MERGE` | Subsidiary star schema, daily grain preserved |
| **Gold (Parent)**| `fmcg.gold.*` | Delta `MERGE` | Unified corporate schema, monthly grain aggregation, ZORDER |
