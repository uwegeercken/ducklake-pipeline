package com.datamelt.ducklake.generator.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "data-generator")
public class DataGeneratorProperties {

    private KafkaProperties kafka = new KafkaProperties();
    private GeneratorProperties generator = new GeneratorProperties();
    private S3Properties s3 = new S3Properties();

    public KafkaProperties getKafka() { return kafka; }
    public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }

    public GeneratorProperties getGenerator() { return generator; }
    public void setGenerator(GeneratorProperties generator) { this.generator = generator; }

    public S3Properties getS3() { return s3; }
    public void setS3(S3Properties s3) { this.s3 = s3; }

    // ── Kafka ─────────────────────────────────────────────────────────────────

    public static class KafkaProperties {
        private String topic;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    // ── Generator ─────────────────────────────────────────────────────────────

    public static class GeneratorProperties {
        private String dataConfig;
        private int rowsPerInterval = 100;
        private long intervalMs = 0;

        public String getDataConfig() { return dataConfig; }
        public void setDataConfig(String dataConfig) { this.dataConfig = dataConfig; }

        public int getRowsPerInterval() { return rowsPerInterval; }
        public void setRowsPerInterval(int rowsPerInterval) { this.rowsPerInterval = rowsPerInterval; }

        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }

        public boolean isRunOnce() { return intervalMs <= 0; }
    }

    // ── S3 ────────────────────────────────────────────────────────────────────

    public static class S3Properties {
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private String rawBucket;
        private String tableName;

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }

        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }

        public String getRawBucket() { return rawBucket; }
        public void setRawBucket(String rawBucket) { this.rawBucket = rawBucket; }

        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
    }
}