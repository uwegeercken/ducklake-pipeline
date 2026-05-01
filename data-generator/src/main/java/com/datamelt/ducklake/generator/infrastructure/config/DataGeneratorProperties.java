package com.datamelt.ducklake.generator.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "data-generator")
public class DataGeneratorProperties {

    private KafkaProperties kafka = new KafkaProperties();
    private GeneratorProperties generator = new GeneratorProperties();

    public KafkaProperties getKafka() { return kafka; }
    public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }

    public GeneratorProperties getGenerator() { return generator; }
    public void setGenerator(GeneratorProperties generator) { this.generator = generator; }

    public static class KafkaProperties {
        private String topic;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

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
}