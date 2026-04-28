# ducklake-pipeline

A containerized data pipeline built around [DuckLake](https://ducklake.select/) — an open table format that uses DuckDB as the query engine, PostgreSQL as the metadata catalog, and S3-compatible object storage (MinIO) for Parquet data files.

## What it does

JSON records are read from a file, published to a Kafka topic, consumed in batches, and persisted into a DuckLake table stored in MinIO — with the full table catalog managed in PostgreSQL.

```
geonames.json → data-reader → Kafka → data-writer → DuckLake
                                                    ├── Catalog: PostgreSQL (dl1 / schema1)
                                                    └── Data:    MinIO (s3://ducklake1/)
```

## Services

| Service | Image | Purpose |
|---|---|---|
| `postgres` | postgres:16-alpine | DuckLake metadata catalog |
| `pgadmin` | dpage/pgadmin4 | PostgreSQL admin UI |
| `kafka` | bitnami/kafka:latest | Message broker (KRaft, no Zookeeper) – Kafka 4.0 |
| `kafka-ui` | kafbat/kafka-ui | Kafka management UI |
| `minio` | minio/minio | S3-compatible object storage |
| `ducklake-initializer` | (built) | One-time setup: DuckLake, schema, table |
| `data-reader` | (built) | Reads JSON file, publishes to Kafka |
| `data-writer` | (built) | Consumes Kafka, writes to DuckLake in batches |

## Quick Start

```bash
# 1. Place your data file
cp your-data.json data/geonames.json

# 2. Copy and configure environment
cp .env.example .env
# edit .env with your credentials

# 3. Start infrastructure
podman-compose up postgres kafka kafka-ui minio minio-init pgadmin

# 4. Build and run all services
podman-compose up --build
```

## URLs

| UI | URL | Credentials (default) |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin_secret |
| pgAdmin | http://localhost:5050 | admin@ducklake.com / admin |

## Tech Stack

- **Java 21** · **Spring Boot 3.4** · **Maven Multi-Module**
- **DuckDB JDBC 1.5.2** with `ducklake` + `postgres` + `httpfs` extensions
- **DuckLake 1.0** – open table format (inlining disabled in data-writer)
- **Apache Kafka 4.0** (KRaft mode) · **Spring Kafka**
- **Jackson** for JSON processing
- **Hexagonal Architecture** across all three services

## Project Structure

```
ducklake-pipeline/
├── common/                  # Shared domain model (Geoname)
├── ducklake-initializer/    # Service 1: one-time DuckLake setup
├── data-reader/             # Service 2: file → Kafka producer
├── data-writer/             # Service 3: Kafka consumer → DuckLake
├── data/                    # Input JSON files (mounted into data-reader)
├── .env                     # Credentials and config (not committed)
├── .env.example             # Template for .env
└── docker-compose.yml
```

## Configuration

All configuration is in `.env`. The `docker-compose.yml` references variables via `${VARIABLE}`.

For local development in IntelliJ, set Run Configuration environment variables:

```
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=geoname-events
KAFKA_GROUP_ID=data-writer-group

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=dl1
POSTGRES_USER=ducklake
POSTGRES_PASSWORD=ducklake_secret

# MinIO
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin_secret
MINIO_BUCKET=ducklake1

# DuckLake
DUCKLAKE_NAME=ducklake1
DUCKLAKE_SCHEMA=schema1
DUCKLAKE_TABLE=geoname

# data-reader only
DATA_FILE_PATH=/path/to/ducklake-pipeline/data/geonames.json
```

Note: Use `localhost:9092` from IntelliJ (EXTERNAL listener). Containers use `kafka:9094` (INTERNAL listener).

## Querying DuckLake locally (DuckDB CLI)

```sql
-- Store credentials permanently (once only)
CREATE PERSISTENT SECRET minio_secret (
    TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin_secret',
    ENDPOINT 'localhost:9000', URL_STYLE 'path',
    USE_SSL false, REGION 'us-east-1'
);

-- Attach (DATA_PATH only needed on first attach per session)
ATTACH IF NOT EXISTS 'ducklake:postgres:user=ducklake password=ducklake_secret host=localhost port=5432 dbname=dl1'
    AS ducklake1 (DATA_PATH 's3://ducklake1/', DATA_INLINING_ROW_LIMIT 0);

USE ducklake1;

-- Query
SELECT * FROM schema1.geoname LIMIT 10;
SELECT count(*) FROM schema1.geoname;

-- Flush inlined data to Parquet (if needed)
CALL ducklake_flush_inlined_data('ducklake1');

-- Merge small Parquet files
CALL ducklake_merge_adjacent_files('ducklake1', 'geoname', schema => 'schema1');
```

## DuckLake Inlining

DuckLake 1.0 stores small inserts directly in the PostgreSQL catalog (inlining) instead of writing Parquet files. The default threshold is **10 rows per insert**.

The `data-writer` disables inlining via `DATA_INLINING_ROW_LIMIT 0` in the ATTACH so all data goes directly to MinIO as Parquet. If you work with the DuckDB CLI and have existing inlined data, flush it first:

```sql
CALL ducklake_flush_inlined_data('ducklake1');
```

## Kafka Listeners

Two listeners are configured for dual access:

| Listener | Port | Advertised as | Used by |
|---|---|---|---|
| EXTERNAL | 9092 | localhost:9092 | Mac / IntelliJ |
| INTERNAL | 9094 | kafka:9094 | Containers |
| CONTROLLER | 9093 | — | KRaft internal |

## Notes

- **pgAdmin email**: `.local` domains are rejected. Use `.com` for `PGADMIN_DEFAULT_EMAIL`.
- **Apple Silicon**: `apache/kafka` and `confluentinc/cp-kafka` lack native ARM64 images. `bitnami/kafka:latest` is used instead.
- **DuckDB extensions**: `ducklake`, `postgres`, and `httpfs` are all required. Downloaded on first run.
- **Single-writer**: DuckDB in-process is single-writer. Run `data-writer` as a single instance only.
- **`.env`**: Never commit `.env` to Git. Use `.env.example` as a template.