package com.datamelt.ducklake.initializer.infrastructure.duckdb;

import com.datamelt.ducklake.initializer.domain.port.out.DuckLakeRepository;
import org.duckdb.DuckDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Outbound Adapter: implementiert DuckLakeRepository via DuckDB JDBC.
 *
 * Hält eine einzelne in-process DuckDB Connection für alle Operationen.
 * Für diesen einmaligen Setup-Job ist single-threaded Zugriff ausreichend.
 */
@Component
public class DuckDbRepositoryAdapter implements DuckLakeRepository {

    private static final Logger log = LoggerFactory.getLogger(DuckDbRepositoryAdapter.class);

    private Connection connection;

    @Override
    public void loadExtensions() {
        log.info("Loading DuckDB extensions: ducklake, httpfs");
        withConnection(conn -> {
            execute(conn, "INSTALL ducklake");
            execute(conn, "LOAD ducklake");
            execute(conn, "INSTALL httpfs");
            execute(conn, "LOAD httpfs");
        });
    }

    @Override
    public void createS3Secret(String keyId, String secret, String endpoint) {
        log.info("Creating S3 secret for endpoint: {}", endpoint);
        withConnection(conn -> execute(conn, """
                CREATE OR REPLACE SECRET minio_secret (
                    TYPE        S3,
                    KEY_ID      '%s',
                    SECRET      '%s',
                    ENDPOINT    '%s',
                    URL_STYLE   'path',
                    USE_SSL     false,
                    REGION      'us-east-1'
                )
                """.formatted(keyId, secret, endpoint)));
    }

    @Override
    public void attachDuckLake(String catalogConnectionString, String dataPath, String name) {
        log.info("Attaching DuckLake '{}' (data path: {})", name, dataPath);
        withConnection(conn -> execute(conn, """
                ATTACH IF NOT EXISTS 'ducklake:postgres:%s' AS %s (DATA_PATH '%s')
                """.formatted(catalogConnectionString, name, dataPath)));
    }

    @Override
    public void createSchemaIfNotExists(String duckLakeName, String schema) {
        log.info("Creating schema if not exists: {}.{}", duckLakeName, schema);
        withConnection(conn -> execute(conn,
                "CREATE SCHEMA IF NOT EXISTS %s.%s".formatted(duckLakeName, schema)));
    }

    @Override
    public void createGeonameTableIfNotExists(String duckLakeName, String schema, String table) {
        log.info("Creating table if not exists: {}.{}.{}", duckLakeName, schema, table);
        String createTableStatement = """
                    create table if not exists %s.%s.%s (
                      id  uuid,
                      first_name varchar,
                      last_name varchar,
                       email varchar,
                       age bigint,
                       address  struct(street varchar, housenumber bigint, city varchar, postalcode varchar, country varchar),
                       created_at timestamp
                    );
                    """.formatted(duckLakeName, schema, table);

        withConnection(conn -> execute(conn, createTableStatement.formatted(duckLakeName, schema, table)));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void withConnection(SqlConsumer action) {
        try {
            action.accept(getOrCreateConnection());
        } catch (SQLException e) {
            throw new DuckDbException("DuckDB operation failed", e);
        }
    }

    private Connection getOrCreateConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            log.debug("Opening DuckDB in-process connection");
            var props = new Properties();
            props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
            connection = DriverManager.getConnection("jdbc:duckdb:", props);
        }
        return connection;
    }

    private void execute(Connection conn, String sql) throws SQLException {
        log.debug("SQL: {}", sql.strip().replaceAll("\\s+", " "));
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    @FunctionalInterface
    private interface SqlConsumer {
        void accept(Connection conn) throws SQLException;
    }
}