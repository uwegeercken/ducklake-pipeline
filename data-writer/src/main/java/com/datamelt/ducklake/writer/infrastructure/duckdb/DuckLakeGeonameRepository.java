package com.datamelt.ducklake.writer.infrastructure.duckdb;

import com.datamelt.ducklake.common.model.Geoname;
import com.datamelt.ducklake.writer.domain.port.out.GeonameRepository;
import com.datamelt.ducklake.writer.infrastructure.config.DataWriterProperties;
import org.duckdb.DuckDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Outbound Adapter: schreibt Geoname-Batches per DuckDB JDBC in den DuckLake.
 *
 * Beim ersten Aufruf wird die DuckDB in-process Connection aufgebaut
 * inklusive Extensions, S3-Secret und ATTACH des DuckLake.
 * Danach wird dieselbe Connection für alle weiteren Batch-Inserts wiederverwendet.
 *
 * DuckDB ist single-writer – der KafkaConsumerAdapter sorgt dafür, dass
 * Batches sequenziell verarbeitet werden.
 */
@Component
public class DuckLakeGeonameRepository implements GeonameRepository {

    private static final Logger log = LoggerFactory.getLogger(DuckLakeGeonameRepository.class);

    private final DataWriterProperties properties;
    private Connection connection;

    public DuckLakeGeonameRepository(DataWriterProperties properties) {
        this.properties = properties;
    }

    @Override
    public void saveBatch(List<Geoname> geonames) {
        var table = properties.getDucklake().getCatalog().toFullyQualifiedTable();
        var sql   = "INSERT INTO %s (geonameid, name, asciiname, country_code, population, latitude, longitude, timezone) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                .formatted(table);

        try {
            var conn = getOrCreateConnection();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (var g : geonames) {
                    ps.setLong  (1, g.getGeonameid());
                    ps.setString(2, g.getName());
                    ps.setString(3, g.getAsciiname());
                    ps.setString(4, g.getCountryCode());
                    ps.setObject(5, g.getPopulation());   // nullable
                    ps.setObject(6, g.getLatitude());     // nullable
                    ps.setObject(7, g.getLongitude());    // nullable
                    ps.setString(8, g.getTimezone());
                    ps.addBatch();
                }
                ps.executeBatch();
                log.debug("Inserted {} rows into {}", geonames.size(), table);
            }
        } catch (SQLException e) {
            throw new DuckDbWriteException("Failed to write batch to DuckLake", e);
        }
    }

    // ── Setup ─────────────────────────────────────────────────────────────────

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
            stmt.execute("INSTALL ducklake");
            stmt.execute("LOAD ducklake");
            stmt.execute("INSTALL postgres");
            stmt.execute("LOAD postgres");
            stmt.execute("INSTALL httpfs");
            stmt.execute("LOAD httpfs");
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

        String attachStatement = """
                    ATTACH IF NOT EXISTS 'ducklake:postgres:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)
                    """.formatted(
                pg.toCatalogConnectionString(),
                catalog.getName(),
                minio.toDataPath());
        try (var stmt = conn.createStatement()) {
            stmt.execute(attachStatement);
        }
        log.info("DuckLake '{}' attached", catalog.getName());
    }
}