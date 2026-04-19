package com.datamelt.ducklake.reader.infrastructure.kafka;

import com.datamelt.ducklake.common.model.Geoname;
import com.datamelt.ducklake.reader.domain.port.out.GeonameEventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Outbound Adapter: sendet Geoname-Datensätze als JSON-Nachrichten nach Kafka.
 *
 * Jeder Datensatz wird als eigenständige Nachricht gesendet.
 * Key: geonameid (als String) – ermöglicht geordnete Verarbeitung
 * Value: JSON-serialisiertes Geoname-Objekt
 *
 * send().get() blockiert bis zur Bestätigung durch den Broker (acks=all).
 */
@Component
public class KafkaProducerAdapter implements GeonameEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerAdapter.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaProducerAdapter(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(List<Geoname> geonames, String topic) {
        geonames.forEach(geoname -> publishOne(geoname, topic));
        log.info("Published {} messages to topic '{}'", geonames.size(), topic);
    }

    private void publishOne(Geoname geoname, String topic) {
        try {
            var key   = String.valueOf(geoname.getGeonameid());
            var value = objectMapper.writeValueAsString(geoname);

            kafkaTemplate.send(topic, key, value).get();

            log.debug("Sent geonameid={} to topic '{}'", geoname.getGeonameid(), topic);
        } catch (JsonProcessingException e) {
            throw new KafkaPublishException(
                    "Failed to serialize geoname: " + geoname.getGeonameid(), e);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(
                    "Failed to send message to Kafka for geonameid: " + geoname.getGeonameid(), e);
        }
    }
}