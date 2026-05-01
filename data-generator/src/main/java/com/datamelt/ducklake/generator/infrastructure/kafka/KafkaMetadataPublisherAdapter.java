package com.datamelt.ducklake.generator.infrastructure.kafka;

import com.datamelt.ducklake.common.model.S3FileReference;
import com.datamelt.ducklake.generator.domain.port.out.KafkaMetadataPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

/**
 * Outbound Adapter: sendet eine S3FileReference als JSON nach Kafka.
 *
 * Message Key:   S3 Pfad der Datei (eindeutig, gut für Log-Compaction)
 * Message Value: {"bucket": "ducklake-raw", "path": "person/2026-...json"}
 */
@Component
public class KafkaMetadataPublisherAdapter implements KafkaMetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaMetadataPublisherAdapter.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaMetadataPublisherAdapter(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(S3FileReference reference, String topic) {
        try {
            var key   = reference.getPath();
            var value = objectMapper.writeValueAsString(reference);

            kafkaTemplate.send(topic, key, value).get();

            log.debug("Sent metadata to topic '{}': {}", topic, value);

        } catch (JsonProcessingException e) {
            throw new KafkaPublishException("Failed to serialize S3FileReference", e);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Failed to send metadata to Kafka", e);
        }
    }
}