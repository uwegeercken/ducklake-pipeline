# DuckLake Streaming Pipeline

A streaming data pipeline that ingests randomly generated JSON records into a
[DuckLake](https://ducklake.select) lakehouse using Apache Kafka, MinIO (S3-compatible
object storage) and PostgreSQL as the DuckLake metadata catalog.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [Data Flow](#data-flow)
- [Table Creation](#table-creation)
    - [Schema-driven (JSON Schema)](#schema-driven-json-schema)
    - [Automatic (first record)](#automatic-first-record)
- [Data Inlining](#data-inlining)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Maintenance](#maintenance)

---

## Overview

The pipeline demonstrates how to stream data continuously into a DuckLake lakehouse
without the traditional "small files problem". It uses DuckLake's data inlining feature
to buffer small writes in PostgreSQL and flush them to Parquet files in MinIO on demand.

```
datagenerator2 ──► MinIO (raw JSON) ──► Kafka (metadata) ──► data-writer ──► DuckLake
                                                                               │
                                                                    PostgreSQL (catalog)
                                                                    MinIO (Parquet files)
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Docker Compose Stack                                                   │
│                                                                         │
│  ┌───────────────┐     ┌──────────┐     ┌──────────────────────────┐   │
│  │ data-generator│────►│  MinIO   │     │       DuckLake           │   │
│  │               │     │ (raw     │     │  ┌────────┐ ┌─────────┐  │   │
│  │ datagenerator2│     │  bucket) │     │  │Postgres│ │  MinIO  │  │   │
│  └───────┬───────┘     └──────────┘     │  │catalog │ │ Parquet │  │   │
│          │ metadata                     │  └────────┘ └─────────┘  │   │
│          ▼                              └──────────────────────────┘   │
│  ┌───────────────┐                               ▲                     │
│  │     Kafka     │                               │                     │
│  └───────┬───────┘                               │                     │
│          │                              ┌────────┴───────┐             │
│          └─────────────────────────────►│  data-writer   │             │
│                                         └────────────────┘             │
│                                                                         │
│  ┌─────────────────────┐   ┌──────────┐   ┌──────────┐                │
│  │ ducklake-initializer│   │ kafka-ui │   │ pgadmin  │                 │
│  └─────────────────────┘   └──────────┘   └──────────┘                │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Services

| Service | Image | Description |
|---|---|---|
| `data-generator` | custom | Generates random JSON records via [datagenerator2](https://github.com/uwegeercken/datagenerator2), persists them to MinIO and publishes file metadata to Kafka |
| `ducklake-initializer` | custom | One-shot Spring Boot app that sets up DuckLake: loads extensions, creates S3 secret, attaches the catalog, creates schema and (optionally) the target table |
| `data-writer` | custom | Spring Boot Kafka consumer that reads file metadata from Kafka and inserts the referenced JSON files from MinIO into DuckLake via `read_json_auto` |
| `kafka` | confluentinc/cp-kafka | Message broker |
| `minio` | minio/minio | S3-compatible object storage — holds raw JSON files (rawdata bucket) and DuckLake Parquet files (ducklake bucket) |
| `minio-init` | minio/mc | One-shot container that creates the required MinIO buckets on startup |
| `postgres` | postgres | DuckLake metadata catalog — stores table definitions, snapshots, and optionally inlined row data |
| `kafka-ui` | provectuslabs/kafka-ui | Web UI for inspecting Kafka topics and consumer groups |
| `pgadmin` | dpage/pgadmin4 | Web UI for inspecting the PostgreSQL DuckLake catalog |

---

## Data Flow

1. **data-generator** uses a `datagenerator2` YAML configuration to produce random JSON
   records. Each record is written as an individual JSON file to the MinIO `rawdata` bucket
   under a configurable prefix (e.g. `person/`).

2. After writing each file, **data-generator** publishes a metadata message to a Kafka
   topic. The message contains the MinIO bucket, prefix and filename — enough for the
   consumer to locate and read the file.

   ```json
   {
     "bucket": "ducklake-raw",
     "path": "person/2026-05-03T12-53-39.759Z-5f7effab.json"
   }
   ```

3. **ducklake-initializer** runs once at startup and prepares the DuckLake catalog
   (see [Table Creation](#table-creation) below).

4. **data-writer** polls Kafka in configurable batches (`max-poll-records`). For each
   batch it builds a single `INSERT INTO … SELECT * FROM read_json_auto([…])` statement
   covering all files in the batch. Because each Kafka poll produces exactly one `INSERT`,
   the Kafka poll size is directly coupled to the DuckLake inlining decision
   (see [Data Inlining](#data-inlining) below).

---

## Table Creation

The **ducklake-initializer** supports two strategies for creating the DuckLake target
table. The choice is made via the `json-schema-path` property.

### Schema-driven (JSON Schema)

**Use this when your data has optional fields, requires specific types (e.g. `UUID`,
`TIMESTAMP`, nested `STRUCT`), or you want explicit `NOT NULL` constraints.**

Set `json-schema-path` to the path of a JSON Schema file. The initializer uses the
[json-schema-duckdb](https://github.com/uwegeercken/json-schema-duckdb) library to
convert the schema into a `CREATE TABLE IF NOT EXISTS` statement and executes it against
DuckLake.

Example JSON Schema (`person.json`):

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["id", "first_name", "last_name", "email", "age", "address", "created_at"],
  "properties": {
    "id":         { "type": "string", "format": "uuid" },
    "first_name": { "type": "string" },
    "last_name":  { "type": "string" },
    "email":      { "type": "string" },
    "age":        { "type": "integer" },
    "address": {
      "type": "object",
      "required": ["street", "housenumber", "city", "postalcode", "country"],
      "properties": {
        "street":      { "type": "string" },
        "housenumber": { "type": "integer", "format": "int32" },
        "city":        { "type": "string" },
        "postalcode":  { "type": "string" },
        "country":     { "type": "string" }
      }
    },
    "created_at": { "type": "string", "format": "date-time" }
  }
}
```

Resulting `CREATE TABLE` statement executed against DuckLake:

```sql
CREATE TABLE IF NOT EXISTS ducklake1.schema1.person (
  id         UUID      NOT NULL,
  first_name VARCHAR   NOT NULL,
  last_name  VARCHAR   NOT NULL,
  email      VARCHAR   NOT NULL,
  age        BIGINT    NOT NULL,
  address    STRUCT(
               street      VARCHAR NOT NULL,
               housenumber INTEGER NOT NULL,
               city        VARCHAR NOT NULL,
               postalcode  VARCHAR NOT NULL,
               country     VARCHAR NOT NULL
             )         NOT NULL,
  created_at TIMESTAMP NOT NULL
);
```

All fields are `NOT NULL` because every property appears in `required`. Nested `STRUCT`
fields inherit the `NOT NULL` constraint from the `required` list of the `address` object.

Configuration:

```yaml
ducklake:
  catalog:
    json-schema-path: /etc/ducklake/person.json   # absolute path on the host
```

### Automatic (first record)

**Use this when all records share the same layout and you are happy to let DuckDB infer
the schema from the data.**

Leave `json-schema-path` empty (or omit it entirely). The **ducklake-initializer** will
skip `CREATE TABLE`. Instead, the **data-writer** checks on its first Kafka batch whether
the target table already exists. If it does not, it creates it by running:

```sql
CREATE TABLE ducklake1.schema1.person
AS SELECT * FROM read_json_auto('s3://ducklake-raw/person/person_….json')
LIMIT 0;
```

`LIMIT 0` means no rows are inserted — only the schema is derived from the JSON
structure. All columns are nullable. Subsequent inserts then use the normal batch path.

Configuration:

```yaml
ducklake:
  catalog:
    json-schema-path:   # leave empty → automatic table creation on first message
```

**Trade-offs:**

| | JSON Schema | Automatic |
|---|---|---|
| Optional fields | ✅ correctly nullable | ✅ all nullable |
| `NOT NULL` constraints | ✅ from `required` | ❌ none |
| Nested `STRUCT` types | ✅ explicit | ✅ inferred |
| `UUID` / `TIMESTAMP` types | ✅ via `format` | ⚠️ DuckDB best-effort inference |
| Setup effort | YAML schema file required | zero |
| Safe for schema evolution | ✅ | ⚠️ depends on data consistency |

---

## Data Inlining

DuckLake supports writing small inserts directly into the PostgreSQL metadata catalog
instead of creating a Parquet file in MinIO for every write. This eliminates the
"small files problem" for streaming workloads.

The threshold is controlled by `data-inlining-row-limit`:

- inserts with **fewer rows** than the limit → written to PostgreSQL (inlined)
- inserts with **at least as many rows** as the limit → written directly to MinIO as Parquet

**Important:** the limit is evaluated per `INSERT` statement, not cumulatively across
multiple inserts. Since the data-writer produces exactly one `INSERT` per Kafka poll,
the actual number of rows in that insert depends on how many messages Kafka delivers in
that poll — which can be less than `max-poll-records` on a slow or low-volume topic.

This means a batch of 90 records will be inlined even if `max-poll-records` is set to
500 and `data-inlining-row-limit` is set to 100. Inlined data stays in PostgreSQL until
flushed manually (see [Maintenance](#maintenance)).

```
Kafka poll (N records, N varies)
    → INSERT INTO … SELECT * FROM read_json_auto([file1, file2, …, fileN])
    → if N <  data-inlining-row-limit  → written to PostgreSQL (inlined)
    → if N >= data-inlining-row-limit  → written to Parquet in MinIO
```

Setting `data-inlining-row-limit: 0` disables inlining entirely — every insert goes
directly to Parquet in MinIO regardless of batch size. This is the safest and simplest
option for a streaming pipeline where you want data to land in MinIO predictably.

```yaml
ducklake:
  catalog:
    data-inlining-row-limit: 0   # 0 = always write to Parquet (recommended default)
```

If you choose to enable inlining (e.g. `data-inlining-row-limit: 100`) you must either
ensure your batches reliably exceed the threshold, or periodically flush inlined data
manually using `ducklake_flush_inlined_data` (see [Maintenance](#maintenance)).

---

## Configuration

All three custom services are configured via `application.yml` and environment variables.
The shared DuckLake connection settings (`postgres`, `minio`, `catalog`) follow the same
structure across services.

### Shared `ducklake` properties

```yaml
ducklake:
  postgres:
    host:     ${POSTGRES_HOST:localhost}
    port:     ${POSTGRES_PORT:5432}
    database: ${POSTGRES_DATABASE:dl1}
    user:     ${POSTGRES_USER:ducklake}
    password: ${POSTGRES_PASSWORD:ducklake_secret}

  minio:
    endpoint:   ${MINIO_ENDPOINT:http://localhost:9000}
    access-key: ${MINIO_ACCESS_KEY:minioadmin}
    secret-key: ${MINIO_SECRET_KEY:minioadmin_secret}
    bucket:     ${MINIO_BUCKET:ducklake1}

  catalog:
    name:                    ${DUCKLAKE_NAME:ducklake1}
    schema:                  ${DUCKLAKE_SCHEMA:schema1}
    table:                   ${DUCKLAKE_TABLE:person}
    data-inlining-row-limit: ${DUCKLAKE_INLINING_LIMIT:0}
    json-schema-path:        ${DUCKLAKE_JSON_SCHEMA_PATH:}   # empty = automatic
```

### data-writer specific

```yaml
spring:
  kafka:
    consumer:
      max-poll-records: 500   # batch size — coupled to inlining threshold (see above)
```

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Ports `9000` (MinIO), `9090` (MinIO console), `5432` (Postgres), `5050` (pgAdmin),
  `8080` (Kafka UI) available on your machine

### Start the full stack

```bash
docker compose up -d
```

The startup order is managed via `depends_on` and health checks:

1. `postgres`, `minio`, `kafka` start first
2. `minio-init` creates the required buckets (`ducklake-raw`, `ducklake1`)
3. `ducklake-initializer` sets up the DuckLake catalog and (if configured) creates the table
4. `data-generator` and `data-writer` start once the initializer has completed

### Provide a JSON Schema (optional)

Mount your JSON Schema file into the initializer container and set the path:

```yaml
# docker-compose.yml
ducklake-initializer:
  volumes:
    - ./config/person.json:/etc/ducklake/person.json
  environment:
    DUCKLAKE_JSON_SCHEMA_PATH: /etc/ducklake/person.json
```

Leave `DUCKLAKE_JSON_SCHEMA_PATH` unset to use automatic table creation from the first
arriving record instead.

### Inspect the pipeline

| UI | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9090 | minioadmin / minioadmin_secret |
| Kafka UI | http://localhost:8080 | — |
| pgAdmin | http://localhost:5050 | see docker-compose.yml |

### Stop and clean up

```bash
docker compose down -v   # -v removes volumes (deletes all data)
```

---

## Maintenance

### Flush inlined data manually

If inlining is enabled (`data-inlining-row-limit > 0`) and batches are smaller than the
threshold, data will accumulate in PostgreSQL. Flush it to Parquet in MinIO by connecting
to DuckDB and running:

```sql
CALL ducklake_flush_inlined_data('ducklake1');

-- or for a specific table only:
CALL ducklake_flush_inlined_data('ducklake1', table_name => 'person');
```

### Compact small Parquet files

Over time, many small Parquet files accumulate (one per Kafka batch). Compact them into
fewer larger files:

```sql
CALL ducklake_merge_adjacent_files('ducklake1', 'person');
```

### Expire old snapshots

DuckLake keeps full snapshot history for time-travel queries. Expire snapshots older than
a given age to reclaim storage:

```sql
CALL ducklake_expire_snapshots('ducklake1', older_than => INTERVAL '7 days');
```

### Time travel

Query the table as it looked at a previous snapshot or timestamp:

```sql
-- by snapshot version
SELECT * FROM ducklake1.schema1.person AT (VERSION => 5);

-- by timestamp
SELECT * FROM ducklake1.schema1.person AT (TIMESTAMP => '2026-05-01 12:00:00'::TIMESTAMP);
```

---

## Related Projects

- [DuckLake](https://ducklake.select) — lakehouse format built on DuckDB
- [datagenerator2](https://github.com/uwegeercken/datagenerator2) — random data generator
- [json-schema-duckdb](https://github.com/uwegeercken/json-schema-duckdb) — JSON Schema to DuckDB DDL converter