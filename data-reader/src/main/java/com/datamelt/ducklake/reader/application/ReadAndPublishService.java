package com.datamelt.ducklake.reader.application;

import com.datamelt.ducklake.reader.domain.port.in.ReadAndPublishUseCase;
import com.datamelt.ducklake.reader.domain.port.out.GeonameEventPublisher;
import com.datamelt.ducklake.reader.domain.port.out.GeonameFileRepository;
import com.datamelt.ducklake.reader.infrastructure.config.DataReaderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Orchestriert den Anwendungsfall "Datei lesen und nach Kafka senden":
 *  1. JSON-Datei einlesen → List<Geoname>
 *  2. Jeden Datensatz als JSON-Nachricht nach Kafka senden
 */
@Service
public class ReadAndPublishService implements ReadAndPublishUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReadAndPublishService.class);

    private final GeonameFileRepository fileRepository;
    private final GeonameEventPublisher eventPublisher;
    private final DataReaderProperties properties;

    public ReadAndPublishService(GeonameFileRepository fileRepository,
                                 GeonameEventPublisher eventPublisher,
                                 DataReaderProperties properties) {
        this.fileRepository = fileRepository;
        this.eventPublisher = eventPublisher;
        this.properties = properties;
    }

    @Override
    public void readAndPublish() {
        var filePath = properties.getFile().getPath();
        var topic    = properties.getKafka().getTopic();

        log.info("Reading geonames from: {}", filePath);
        var geonames = fileRepository.readAll(filePath);
        log.info("Read {} records, publishing to topic '{}'", geonames.size(), topic);

        eventPublisher.publish(geonames, topic);

        log.info("All records published successfully");
    }
}