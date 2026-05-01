package com.datamelt.ducklake.writer.infrastructure.duckdb;

import com.datamelt.ducklake.common.model.S3FileReference;
import com.datamelt.ducklake.writer.domain.port.out.DuckLakeRepository;
import com.datamelt.ducklake.writer.infrastructure.config.DataWriterProperties;
import org.duckdb.DuckDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Outbound Adapter: schreibt S3-Dateien in DuckLake via:
 *
 *   INSERT INTO ducklake1.schema1.person
 *   SELECT * FROM read_json_auto('s3://ducklake-raw/person/2026-....json');
 *
 * DuckDB übernimmt automatisch das Type-Casting (STRUCT, UUID, TIMESTAMP etc.)
 * entsprechend der Tabellendefinition – keine manuelle Feldmappings nötig.
 *
 * Das S3 Secret für MinIO ist bereits durch den ATTACH-Schritt konfiguriert
 * und gilt für alle nachfolgenden S3-Zugriffe in dieser Connection.
 */
@Component
public class DuckLakeS3Repository implements DuckLakeRepository {

    private static final Logger log = LoggerFactory.getLogger(DuckLakeS3Repository.class);

    private final DataWriterProperties properties;
    private Connection connection;

    public DuckLakeS3Repository(DataWriterProperties properties) {
        this.properties = properties;
    }

    @Override
    public void insertFromS3(List<S3FileReference> references) {
        var catalog = properties.getDucklake().getCatalog();
        var table   = catalog.toFullyQualifiedTable();

        try {
            var conn = getOrCreateConnection();
            for (var reference : references) {
                insertOne(conn, table, reference);
            }
        } catch (SQLException e) {
            throw new DuckDbWriteException("Failed to insert from S3", e);
        }
    }

    private void insertOne(Connection conn, String table, S3FileReference reference)
            throws SQLException {
        var sql = "INSERT INTO %s SELECT * FROM read_json_auto('%s')"
                .formatted(table, reference.toS3Url());

        log.debug("Executing: {}", sql);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ── DuckDB connection setup ───────────────────────────────────────────────

    private Connection getOrCreateConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            log.info("Initializing DuckDB connection and attaching DuckLake");
            connection = openConnection();
            loadExtensions(connection);
            createS3Secret(connection);
            attachDuckLake(connection);
        }
        return connection;
    }

    private Connection openConnection() throws SQLException {
        var props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
        return DriverManager.getConnection("jdbc:duckdb:", props);
    }

    private void loadExtensions(Connection conn) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute("INSTALL ducklake"); stmt.execute("LOAD ducklake");
            stmt.execute("INSTALL postgres");  stmt.execute("LOAD postgres");
            stmt.execute("INSTALL httpfs");    stmt.execute("LOAD httpfs");
        }
        log.debug("DuckDB extensions loaded");
    }

    private void createS3Secret(Connection conn) throws SQLException {
        var minio    = properties.getDucklake().getMinio();
        var endpoint = minio.toEndpointWithoutProtocol();

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE OR REPLACE SECRET minio_secret (
                        TYPE        S3,
                        KEY_ID      '%s',
                        SECRET      '%s',
                        ENDPOINT    '%s',
                        URL_STYLE   'path',
                        USE_SSL     false,
                        REGION      'us-east-1'
                    )
                    """.formatted(minio.getAccessKey(), minio.getSecretKey(), endpoint));
        }
        log.debug("S3 secret created for endpoint: {}", endpoint);
    }

    private void attachDuckLake(Connection conn) throws SQLException {
        var pg      = properties.getDucklake().getPostgres();
        var minio   = properties.getDucklake().getMinio();
        var catalog = properties.getDucklake().getCatalog();

        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    ATTACH IF NOT EXISTS 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)
                    """.formatted(
                    pg.toCatalogConnectionString(),
                    catalog.getName(),
                    minio.toDataPath()));
        }
        log.info("DuckLake '{}' attached", catalog.getName());
    }
}