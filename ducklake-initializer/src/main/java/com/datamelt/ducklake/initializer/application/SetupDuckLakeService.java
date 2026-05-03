package com.datamelt.ducklake.initializer.application;

import com.datamelt.ducklake.initializer.domain.port.in.SetupDuckLakeUseCase;
import com.datamelt.ducklake.initializer.domain.port.out.DuckLakeRepository;
import com.datamelt.ducklake.initializer.infrastructure.config.DuckLakeProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Orchestriert das einmalige DuckLake-Setup:
 *  1. DuckDB Extensions laden  (ducklake, httpfs)
 *  2. S3 Secret für MinIO anlegen
 *  3. DuckLake ATTACH gegen Postgres-Katalog
 *  4. Schema anlegen
 *  5. Tabelle anlegen
 */
@Service
public class SetupDuckLakeService implements SetupDuckLakeUseCase {

    private static final Logger log = LoggerFactory.getLogger(SetupDuckLakeService.class);

    private final DuckLakeRepository repository;
    private final DuckLakeProperties properties;

    public SetupDuckLakeService(DuckLakeRepository repository, DuckLakeProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    public void setup() {
        var pg      = properties.getPostgres();
        var minio   = properties.getMinio();
        var catalog = properties.getCatalog();

        log.info("Starting DuckLake setup – catalog={}, schema={}, table={}",
                catalog.getName(), catalog.getSchema(), catalog.getTable());

        repository.loadExtensions();

        repository.createS3Secret(
                minio.getAccessKey(),
                minio.getSecretKey(),
                minio.toEndpointWithoutProtocol());

        repository.attachDuckLake(
                pg.toCatalogConnectionString(),
                minio.toDataPath(),
                catalog.getName(),
                catalog.getDataInliningRowLimit());

        repository.createSchemaIfNotExists(
                catalog.getName(),
                catalog.getSchema());

        // ── Only create table when a schema is explicitly configured ──────────
        var columnDdl = catalog.getTableSchema();
        if (columnDdl != null && !columnDdl.isBlank()) {
            repository.createTableIfNotExists(
                    catalog.getName(),
                    catalog.getSchema(),
                    catalog.getTable(),
                    columnDdl.strip());
        } else {
            log.info("No table-schema configured – skipping CREATE TABLE");
        }

        log.info("DuckLake setup completed");
    }
}