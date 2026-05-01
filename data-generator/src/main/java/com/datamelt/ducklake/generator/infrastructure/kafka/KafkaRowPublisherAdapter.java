package com.datamelt.ducklake.generator.infrastructure.kafka;

import com.datamelt.ducklake.generator.domain.port.out.RowPublisher;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Outbound Adapter: serialisiert jede Row-Map zu JSON und sendet sie nach Kafka.
 *
 * Message Key:   Wert des "rownumber" Feldes (auto-generated von datagenerator2),
 *                fallback auf den Listen-Index wenn rownumber fehlt.
 * Message Value: vollständige Row als JSON String.
 *
 * send().get() blockiert bis zur Broker-Bestätigung (acks=all).
 */
@Component
public class KafkaRowPublisherAdapter implements RowPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRowPublisherAdapter.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaRowPublisherAdapter(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(List<JsonNode> jsonRows, String topic) {
        for (int i = 0; i < jsonRows.size(); i++) {
            publishOne(jsonRows.get(i), i, topic);
        }
        log.info("Published {} messages to topic '{}'", jsonRows.size(), topic);
    }

    private void publishOne(JsonNode jsonRow, int index, String topic) {
        try {
            var key   = "generatedrow_" + index;
            var value = jsonRow.asText();

            kafkaTemplate.send(topic, key, value).get();

            log.debug("Sent row key={} to topic '{}'", key, topic);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Failed to send row at index " + index, e);
        }
    }
}