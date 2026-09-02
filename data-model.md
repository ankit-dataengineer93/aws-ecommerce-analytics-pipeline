# Data Model — Star Schema

The warehouse layer (Redshift) uses a classic **star schema**: one central fact table surrounded by dimension tables. This design optimizes for fast analytical queries (aggregations, filtering by time/region/product) at the cost of some data redundancy in the dimensions — the standard trade-off for OLAP workloads.

## Entity-Relationship Diagram

```
DIM_CUSTOMER ||--o{ FACT_ORDERS : has
DIM_PRODUCT  ||--o{ FACT_ORDERS : has
DIM_DATE     ||--o{ FACT_ORDERS : has

DIM_CUSTOMER {
    varchar customer_id PK
    varchar region
}

DIM_PRODUCT {
    varchar product_id PK
    varchar product_name
}

DIM_DATE {
    int      date_id PK
    date     full_date
    int      year
    int      month
    int      day
    int      hour
}

FACT_ORDERS {
    varchar   event_id PK
    varchar   customer_id FK
    varchar   product_id FK
    int       date_id FK
    varchar   event_type
    double    price
    timestamp event_timestamp
}
```

*(Render this block with any Mermaid-compatible viewer — e.g. paste it into the [Mermaid Live Editor](https://mermaid.live) — or view the rendered version in the project write-up.)*

## Table Details

### `fact_orders` (fact table)
One row per event (view, add_to_cart, or purchase). Holds the measures and foreign keys.

| Column | Type | Notes |
|---|---|---|
| `event_id` | VARCHAR(100) | Primary key — unique per event |
| `customer_id` | VARCHAR(50) | FK → `dim_customer` |
| `product_id` | VARCHAR(50) | FK → `dim_product` |
| `date_id` | INT | FK → `dim_date`, format `YYYYMMDD` |
| `event_type` | VARCHAR(20) | `view`, `add_to_cart`, or `purchase` |
| `price` | DOUBLE PRECISION | Item price at time of event |
| `event_timestamp` | TIMESTAMP | Exact event time |

### `dim_customer` (dimension)
One row per unique customer.

| Column | Type | Notes |
|---|---|---|
| `customer_id` | VARCHAR(50) | Primary key |
| `region` | VARCHAR(10) | US, EU, IN, UK, CA |

### `dim_product` (dimension)
One row per unique product.

| Column | Type | Notes |
|---|---|---|
| `product_id` | VARCHAR(50) | Primary key |
| `product_name` | VARCHAR(100) | Human-readable name |

### `dim_date` (dimension)
One row per unique hour bucket — lets you group/filter by any time grain without repeated date-function calls in every query.

| Column | Type | Notes |
|---|---|---|
| `date_id` | INT | Primary key, format `YYYYMMDD` |
| `full_date` | DATE | Calendar date |
| `year` / `month` / `day` / `hour` | INT | Pre-extracted for fast `GROUP BY` |

## Why a star schema (vs. a single flat table)

- **Query performance:** Redshift's columnar storage + sort/dist keys work best when fact tables are narrow (mostly IDs and measures) and joins fan out to small dimension tables — much cheaper than repeatedly scanning a wide denormalized table.
- **Storage efficiency:** Customer region and product name are stored once per entity, not once per event — meaningful savings at scale (millions of events vs. hundreds of customers/products).
- **Analytical flexibility:** Adding a new dimension (e.g. `dim_campaign` for marketing attribution) doesn't require reshaping the fact table — just a new join.
- **Trade-off accepted:** Every query needs joins (vs. a flat table with zero joins). At this data volume the join cost is negligible, and the schema clarity is worth it for interview/portfolio purposes — it demonstrates deliberate warehouse design, not just "dump everything in one table."

## How data flows into this model

1. Curated Parquet data (cleaned by Glue) lands in a flat `staging_orders` table via `COPY` — same shape as the source, no fact/dim split yet.
2. `INSERT ... SELECT DISTINCT` statements populate `dim_customer`, `dim_product`, and `dim_date` from the staging table, skipping any values already present (idempotent re-runs).
3. A final `INSERT ... SELECT` populates `fact_orders`, joining in the correct `date_id` computed from each event's timestamp.

See [`warehouse/load_data.sql`](../warehouse/load_data.sql) for the exact SQL.
