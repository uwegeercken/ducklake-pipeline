# ducklake-pipeline

A containerized data pipeline built around [DuckLake](https://ducklake.select/) — an open table format that uses DuckDB as the query engine, PostgreSQL as the metadata catalog, and S3-compatible object storage (MinIO) for the actual data files.

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
| `kafka` | bitnami/kafka:latest | Message broker (KRaft, no Zookeeper) |
| `kafka-ui` | kafbat/kafka-ui | Kafka management UI |
| `minio` | minio/minio | S3-compatible object storage |
| `ducklake-initializer` | (built) | One-time setup: DuckLake, schema, table |
| `data-reader` | (built) | Reads JSON file, publishes to Kafka |
| `data-writer` | (built) | Consumes Kafka, writes to DuckLake in batches |

## Quick Start

```bash
# 1. Place your data file
cp your-data.json data/geonames.json

# 2. Start infrastructure
podman-compose up postgres kafka kafka-ui minio minio-init pgadmin

# 3. Build and run all services
podman-compose up --build
```

## URLs

| UI | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin_secret |
| pgAdmin | http://localhost:5050 | admin@ducklake.com / admin |

## Tech Stack

- **Java 21** · **Spring Boot 3.4** · **Maven Multi-Module**
- **DuckDB JDBC 1.2** with `ducklake` extension
- **Apache Kafka** (KRaft mode) · **Spring Kafka**
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
└── docker-compose.yml
```

## Configuration

All services are configured via environment variables defined in `docker-compose.yml`. For local development in IntelliJ, set the following run configuration variables:

```
POSTGRES_HOST=localhost  POSTGRES_PORT=5432  POSTGRES_DATABASE=dl1
POSTGRES_USER=ducklake   POSTGRES_PASSWORD=ducklake_secret
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin  MINIO_SECRET_KEY=minioadmin_secret
MINIO_BUCKET=ducklake1   DUCKLAKE_NAME=ducklake1
DUCKLAKE_SCHEMA=schema1  DUCKLAKE_TABLE=geoname
KAFKA_BOOTSTRAP_SERVERS=localhost:9092  KAFKA_TOPIC=geoname-events
```