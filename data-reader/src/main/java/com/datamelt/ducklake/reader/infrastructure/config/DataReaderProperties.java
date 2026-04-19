package com.datamelt.ducklake.reader.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "data-reader")
public class DataReaderProperties {

    private KafkaProperties kafka = new KafkaProperties();
    private FileProperties file = new FileProperties();

    public KafkaProperties getKafka() { return kafka; }
    public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }

    public FileProperties getFile() { return file; }
    public void setFile(FileProperties file) { this.file = file; }

    public static class KafkaProperties {
        private String topic;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    public static class FileProperties {
        private String path;

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
    }
}