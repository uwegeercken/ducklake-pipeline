package com.datamelt.ducklake.writer.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "data-writer")
public class DataWriterProperties {

    private KafkaProperties kafka = new KafkaProperties();
    private DuckLakeProperties ducklake = new DuckLakeProperties();
    private BatchProperties batch = new BatchProperties();

    public KafkaProperties getKafka() { return kafka; }
    public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }

    public DuckLakeProperties getDucklake() { return ducklake; }
    public void setDucklake(DuckLakeProperties ducklake) { this.ducklake = ducklake; }

    public BatchProperties getBatch() { return batch; }
    public void setBatch(BatchProperties batch) { this.batch = batch; }

    // ── Kafka ─────────────────────────────────────────────────────────────────

    public static class KafkaProperties {
        private String topic;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    // ── DuckLake ──────────────────────────────────────────────────────────────

    public static class DuckLakeProperties {
        private PostgresProperties postgres = new PostgresProperties();
        private MinioProperties minio = new MinioProperties();
        private CatalogProperties catalog = new CatalogProperties();

        public PostgresProperties getPostgres() { return postgres; }
        public void setPostgres(PostgresProperties postgres) { this.postgres = postgres; }

        public MinioProperties getMinio() { return minio; }
        public void setMinio(MinioProperties minio) { this.minio = minio; }

        public CatalogProperties getCatalog() { return catalog; }
        public void setCatalog(CatalogProperties catalog) { this.catalog = catalog; }
    }

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

        public String toCatalogConnectionString() {
            // libpq format required by DuckLake postgres extension
            return "user=%s password=%s host=%s port=%d dbname=%s"
                    .formatted(user, password, host, port, database);
        }
    }

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

        public String toDataPath() {
            return "s3://" + bucket + "/";
        }

        public String toEndpointWithoutProtocol() {
            return endpoint.replace("http://", "").replace("https://", "");
        }
    }

    public static class CatalogProperties {
        private String name;
        private String schema;
        private String table;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }

        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }

        public String toFullyQualifiedTable() {
            return "%s.%s.%s".formatted(name, schema, table);
        }
    }

    // ── Batch ─────────────────────────────────────────────────────────────────

    public static class BatchProperties {
        private int size = 100;

        public int getSize() { return size; }
        public void setSize(int size) { this.size = size; }
    }
}