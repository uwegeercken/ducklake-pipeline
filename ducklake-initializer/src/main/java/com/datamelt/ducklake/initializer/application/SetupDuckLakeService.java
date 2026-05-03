package com.datamelt.ducklake.initializer.application;

import com.datamelt.ducklake.initializer.domain.port.in.SetupDuckLakeUseCase;
import com.datamelt.ducklake.initializer.domain.port.out.DuckLakeRepository;
import com.datamelt.ducklake.initializer.infrastructure.config.DuckLakeProperties;
import com.datamelt.utilities.schema.duckdb.ddl.DdlGenerator;
import com.datamelt.utilities.schema.duckdb.ddl.DdlResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Path;

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

        if (catalog.hasJsonSchema()) {
            var schemaPath = Path.of(catalog.getJsonSchemaPath());
            var fqt        = catalog.toFullyQualifiedTable();

            log.info("Generating DDL from JSON Schema: {}", schemaPath);
            DdlResult result;
            try {
                result = DdlGenerator.generateDdl(fqt, schemaPath);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to generate DDL from JSON Schema: " + schemaPath, e);
            }

            result.getErrors().forEach(e ->
                    log.error("Schema DDL error   [{}]: {}", e.getField(), e.getMessage()));
            result.getWarnings().forEach(w ->
                    log.warn ("Schema DDL warning [{}]: {}", w.getField(), w.getMessage()));

            if (!result.getErrors().isEmpty()) {
                throw new IllegalStateException("JSON Schema conversion produced errors – aborting. Schema: " + schemaPath);
            }

            repository.executeDDL(result.getDdl());
        } else {
            log.info("No json-schema-path configured – skipping CREATE TABLE");
        }

        log.info("DuckLake setup completed");
    }
}