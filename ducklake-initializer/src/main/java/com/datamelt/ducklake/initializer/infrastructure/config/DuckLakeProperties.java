package com.datamelt.ducklake.initializer.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ducklake")
public class DuckLakeProperties {

    private PostgresProperties postgres = new PostgresProperties();
    private MinioProperties minio = new MinioProperties();
    private CatalogProperties catalog = new CatalogProperties();

    public PostgresProperties getPostgres() { return postgres; }
    public void setPostgres(PostgresProperties postgres) { this.postgres = postgres; }

    public MinioProperties getMinio() { return minio; }
    public void setMinio(MinioProperties minio) { this.minio = minio; }

    public CatalogProperties getCatalog() { return catalog; }
    public void setCatalog(CatalogProperties catalog) { this.catalog = catalog; }

    // ── Postgres ──────────────────────────────────────────────────────────────

    public static class PostgresProperties {
        private String host;
        private int port;
        private String database;
        private String user;
        private String password;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }

        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }

        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }

        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }

        /**
         * Liefert den Connection-String im Format, das die DuckDB
         * ducklake Extension erwartet:
         *   postgresql://user:password@host:port/database
         */
        public String toCatalogConnectionString() {
            // libpq format required by DuckLake postgres extension
            return "user=%s password=%s host=%s port=%d dbname=%s"
                    .formatted(user, password, host, port, database);
        }
    }

    // ── MinIO ─────────────────────────────────────────────────────────────────

    public static class MinioProperties {
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private String bucket;

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }

        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }

        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }

        /** s3://bucket/ als Datenpfad für DuckLake */
        public String toDataPath() {
            return "s3://" + bucket + "/";
        }

        /** Endpoint ohne http:// für DuckDB S3 SECRET */
        public String toEndpointWithoutProtocol() {
            return endpoint.replace("http://", "").replace("https://", "");
        }
    }

    // ── Catalog ───────────────────────────────────────────────────────────────

    public static class CatalogProperties {
        private String name;
        private String schema;
        private String table;
        private int     dataInliningRowLimit = 0;   // NEW
        private String  jsonSchemaPath;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }

        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }

        public int getDataInliningRowLimit()
        {
            return dataInliningRowLimit;
        }

        public void setDataInliningRowLimit(int dataInliningRowLimit)
        {
            this.dataInliningRowLimit = dataInliningRowLimit;
        }

        public String getJsonSchemaPath()
        {
            return jsonSchemaPath;
        }

        public void setJsonSchemaPath(String jsonSchemaPath)
        {
            this.jsonSchemaPath = jsonSchemaPath;
        }

        public boolean hasJsonSchema() {
            return jsonSchemaPath != null && !jsonSchemaPath.isBlank();
        }

        public String toFullyQualifiedTable() {
            return "%s.%s.%s".formatted(name, schema, table);
        }
    }
}