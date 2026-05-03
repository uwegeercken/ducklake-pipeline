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
import java.util.stream.Collectors;

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
    private boolean tableVerified = false;

    public DuckLakeS3Repository(DataWriterProperties properties) {
        this.properties = properties;
    }

    @Override
    public void insertFromS3(List<S3FileReference> references) {
        if (references.isEmpty()) return;

        var table = properties.getDucklake().getCatalog().toFullyQualifiedTable();
        var catalog = properties.getDucklake().getCatalog().getName();

        try {
            var conn = getOrCreateConnection();
            ensureTableExists(conn, table, references.get(0));
            insertBatch(conn, table, references);
            flushInlinedData(conn, catalog);                // ← always flush after insert
        } catch (SQLException e) {
            throw new DuckDbWriteException("Failed to insert from S3", e);
        }
    }

    private void flushInlinedData(Connection conn, String catalog) throws SQLException {
        log.debug("Flushing inlined data for catalog '{}'", catalog);
        try (var stmt = conn.createStatement()) {
            stmt.execute("CALL ducklake_flush_inlined_data('%s')".formatted(catalog));
        }
    }

    private void insertBatch(Connection conn, String table, List<S3FileReference> references)
            throws SQLException {

        var urls = references.stream()
                .map(r -> "'" + r.toS3Url() + "'")
                .collect(Collectors.joining(", "));

        var sql = "INSERT INTO %s SELECT * FROM read_json_auto([%s])"
                .formatted(table, urls);

        log.debug("Inserting batch of {} files into {}", references.size(), table);
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ── Table auto-creation ───────────────────────────────────────────────────

    private void ensureTableExists(Connection conn, String fullyQualifiedTable,
                                   S3FileReference sample) throws SQLException {
        if (tableVerified) return;

        if (!tableExists(conn)) {
            log.info("Table '{}' not found – auto-creating from sample file: {}",
                    fullyQualifiedTable, sample.toS3Url());
            autoCreateTable(conn, fullyQualifiedTable, sample);
        } else {
            log.debug("Table '{}' already exists", fullyQualifiedTable);
        }
        tableVerified = true;
    }

    private boolean tableExists(Connection conn) throws SQLException {
        var catalog  = properties.getDucklake().getCatalog();
        var sql = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_catalog = ?
                  AND table_schema  = ?
                  AND table_name    = ?
                """;
        try (var ps = conn.prepareStatement(sql)) {
            ps.setString(1, catalog.getName());
            ps.setString(2, catalog.getSchema());
            ps.setString(3, catalog.getTable());
            try (var rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private void autoCreateTable(Connection conn, String fullyQualifiedTable,
                                 S3FileReference sample) throws SQLException {
        var sql = "CREATE TABLE %s AS SELECT * FROM read_json_auto('%s') LIMIT 0"
                .formatted(fullyQualifiedTable, sample.toS3Url());
        log.debug("Auto-create SQL: {}", sql);
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ── Insert ────────────────────────────────────────────────────────────────

    private void insertOne(Connection conn, String table, S3FileReference reference)
            throws SQLException {
        var sql = "INSERT INTO %s SELECT * FROM read_json_auto('%s')"
                .formatted(table, reference.toS3Url());
        log.debug("Executing: {}", sql);
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ── Connection / setup ────────────────────────────────────────────────────

    private Connection getOrCreateConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            log.info("Initializing DuckDB connection and attaching DuckLake");
            connection = openConnection();
            loadExtensions(connection);
            createS3Secret(connection);
            attachDuckLake(connection);
            tableVerified = false;    // reset if connection was re-opened
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
            stmt.execute("INSTALL postgres"); stmt.execute("LOAD postgres");
            stmt.execute("INSTALL httpfs");   stmt.execute("LOAD httpfs");
        }
    }

    private void createS3Secret(Connection conn) throws SQLException {
        var minio    = properties.getDucklake().getMinio();
        var endpoint = minio.toEndpointWithoutProtocol();
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE OR REPLACE SECRET minio_secret (
                        TYPE      S3,
                        KEY_ID    '%s',
                        SECRET    '%s',
                        ENDPOINT  '%s',
                        URL_STYLE 'path',
                        USE_SSL   false,
                        REGION    'us-east-1'
                    )
                    """.formatted(minio.getAccessKey(), minio.getSecretKey(), endpoint));
        }
    }

    private void attachDuckLake(Connection conn) throws SQLException {
        var pg      = properties.getDucklake().getPostgres();
        var minio   = properties.getDucklake().getMinio();
        var catalog = properties.getDucklake().getCatalog();
        try (var stmt = conn.createStatement()) {
            stmt.execute("""
                    ATTACH IF NOT EXISTS 'ducklake:postgres:%s' AS %s \
                    (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT %d)
                    """.formatted(
                    pg.toCatalogConnectionString(),
                    catalog.getName(),
                    minio.toDataPath(),
                    catalog.getDataInliningRowLimit()));          // ← NEW
        }
        log.info("DuckLake '{}' attached (inlining limit: {})",
                catalog.getName(), catalog.getDataInliningRowLimit());
    }
}