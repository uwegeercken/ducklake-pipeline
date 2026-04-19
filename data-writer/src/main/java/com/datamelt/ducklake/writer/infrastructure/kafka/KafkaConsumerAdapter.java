package com.datamelt.ducklake.writer.infrastructure.kafka;

import com.datamelt.ducklake.common.model.Geoname;
import com.datamelt.ducklake.writer.domain.port.in.PersistGeonamesUseCase;
import com.datamelt.ducklake.writer.infrastructure.config.DataWriterProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Inbound Adapter: empfängt Nachrichten vom Kafka Topic und
 * leitet deserialisierte Batches an den PersistGeonamesUseCase weiter.
 *
 * Batching erfolgt über spring.kafka.consumer.max-poll-records.
 * Manueller Commit (Acknowledgment) nach erfolgreichem Persistieren.
 *
 * Nachrichten, die nicht deserialisiert werden können, werden
 * geloggt und übersprungen (dead-letter-Strategie kann hier ergänzt werden).
 */
@Component
public class KafkaConsumerAdapter {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerAdapter.class);

    private final PersistGeonamesUseCase persistUseCase;
    private final ObjectMapper objectMapper;
    private final DataWriterProperties properties;

    public KafkaConsumerAdapter(PersistGeonamesUseCase persistUseCase,
                                ObjectMapper objectMapper,
                                DataWriterProperties properties) {
        this.persistUseCase = persistUseCase;
        this.objectMapper   = objectMapper;
        this.properties     = properties;
    }

    @KafkaListener(
            topics   = "${data-writer.kafka.topic}",
            groupId  = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("Received batch of {} records from topic '{}'",
                records.size(), properties.getKafka().getTopic());

        var geonames = records.stream()
                .map(record -> deserialize(record.value()))
                .flatMap(Optional::stream)
                .collect(Collectors.toList());

        if (!geonames.isEmpty()) {
            persistUseCase.persist(geonames);
        }

        ack.acknowledge();
    }

    private Optional<Geoname> deserialize(String json) {
        try {
            return Optional.of(objectMapper.readValue(json, Geoname.class));
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize message, skipping: {}", json, e);
            return Optional.empty();
        }
    }
}