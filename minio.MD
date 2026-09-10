# Data Lakehouse Storage Architecture

This project implements a streaming Kappa architecture using a **Domain-First Medallion structure** hosted on local S3-compatible object storage (MinIO). All data is stored using the **Delta Lake** open table format to provide ACID transactions, time travel, and schema enforcement.

## Storage Hierarchy

```text
lakehouse/  
└── cold-chain-telemetry/                <-- Domain Namespace
    ├── bronze/                          <-- Raw Ingestion (Atomic Equivalent)
    ├── silver/                          <-- Cleaned & Parsed (Atomic Parquet Equivalent)
    └── gold/                            <-- Aggregated Analytics (Pre-report Equivalent)
```

## 1. Bronze Layer (Raw Ingestion)

**Purpose**: The immutable landing zone for all raw data ingested from Kafka.

*   **Format**: Delta Lake (Parquet files + transaction log)
*   **Schema**: Schema-on-read; preserves raw JSON structure with minimal transformation
*   **Partitioning**: None (Query performance relies on Delta Lake Z-Ordering and statistics)
*   **Data Model**: "One-row-per-event" (Atomic records)

**Retention**: Indefinite (or defined by Data Retention Policy, e.g., 7 years)
**Update Frequency**: Continuous (micro-batches)

**Sample File Structure**:

```
lakehouse/cold-chain-telemetry/bronze/vehicle_id=VEHICLE_001/
├── _delta_log/                          <-- Transaction log
├── part-00000-....snappy.parquet          <-- Raw JSON data
└── part-00001-....snappy.parquet
```

## 2. Silver Layer (Cleaned & Parsed)

**Purpose**: The "Single Source of Truth" for clean, structured data.

*   **Format**: Delta Lake
*   **Schema**: Schema-on-write; normalized and validated
*   **Partitioning**: `vehicle_id`, `date` (for time-based queries)
*   **Data Model**: Type-casted, de-duplicated, enriched with metadata

**Retention**: Indefinite (or defined by Data Retention Policy)
**Update Frequency**: Continuous (micro-batches)

## 3. Gold Layer (Aggregated Analytics)

**Purpose**: Business-level aggregates optimized for BI tools and reporting.

*   **Format**: Delta Lake (Optimized for analytics)
*   **Schema**: Business-specific schemas with denormalized data
*   **Partitioning**: `vehicle_id`, `date`
*   **Data Model**: Pre-joined tables, roll-ups, window functions

**Retention**: Defined by business requirements (e.g., 90 days for "Hot Data", archive longer-term)
**Update Frequency**: Hourly or Daily (batch jobs)

## Storage Technology: Delta Lake on S3

### Benefits

| Feature | Description | Business Value |
| :--- | :--- | :--- |
| **ACID Transactions** | Ensures data consistency during concurrent writes | Prevents data corruption during streaming updates |
| **Schema Evolution** | Graceful handling of schema changes | Reduces maintenance overhead |
| **Time Travel** | Query historical versions of data | Audit trails, rollback capabilities |
| **Upsert/Delete** | Merge and delete records (CDP capabilities) | Support for compliance and data quality |
| **Z-Ordering** | Data layout optimization for query performance | Faster analytics without manual partitioning |

### Location: MinIO

*   **Endpoint**: `http://minio:9000`
*   **Bucket**: `lakehouse`
*   **Credentials**:
    *   Access Key: `admin`
    *   Secret Key: `password`

## S3 Folder Structure Reference

```text
lakehouse/
├── cold-chain-telemetry/
│   ├── bronze/                  # Raw, immutable event logs
│   │   ├── _delta_log/
│   │   └── data/
│   ├── silver/                  # Clean, validated, type-casted data
│   │   ├── _delta_log/
│   │   ├── vehicle_id=.../
│   │   └── date=.../
│   └── gold/                    # Business-level analytics, dashboards
│       ├── _delta_log/
│       ├── vehicle_id=.../
│       └── date=.../
```

## Retention Policies

| Layer | Retention | Storage Tier |
| :--- | :--- | :--- |
| **Bronze** | 7 Years | Standard |
| **Silver** | Indefinite | Standard |
| **Gold** | 90 Days Hot, 5 Years Archive | Standard/Glacier (MinIO Tiers) |

*Note: MinIO object storage can be configured with lifecycle policies to automatically transition data to cheaper storage tiers based on age.*
*Not there yet*
