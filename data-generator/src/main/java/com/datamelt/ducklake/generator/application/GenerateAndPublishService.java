package com.datamelt.ducklake.generator.application;

import com.datamelt.ducklake.generator.domain.port.in.GenerateAndPublishUseCase;
import com.datamelt.ducklake.generator.domain.port.out.DataGeneratorRepository;
import com.datamelt.ducklake.generator.domain.port.out.KafkaMetadataPublisher;
import com.datamelt.ducklake.generator.domain.port.out.S3FilePublisher;
import com.datamelt.ducklake.generator.infrastructure.config.DataGeneratorProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestriert den Anwendungsfall "Daten generieren, nach S3 schreiben, Metadaten nach Kafka senden":
 *
 *  Für jede generierte Row:
 *  1. Row als JSON Datei nach MinIO schreiben → S3FileReference
 *  2. S3FileReference als Kafka Nachricht senden
 */
@Service
public class GenerateAndPublishService implements GenerateAndPublishUseCase {

    private static final Logger log = LoggerFactory.getLogger(GenerateAndPublishService.class);

    private final DataGeneratorRepository generatorRepository;
    private final S3FilePublisher s3FilePublisher;
    private final KafkaMetadataPublisher kafkaMetadataPublisher;
    private final DataGeneratorProperties properties;

    public GenerateAndPublishService(DataGeneratorRepository generatorRepository,
                                     S3FilePublisher s3FilePublisher,
                                     KafkaMetadataPublisher kafkaMetadataPublisher,
                                     DataGeneratorProperties properties) {
        this.generatorRepository  = generatorRepository;
        this.s3FilePublisher      = s3FilePublisher;
        this.kafkaMetadataPublisher = kafkaMetadataPublisher;
        this.properties           = properties;
    }

    @Override
    public void generateAndPublish() {
        var gen       = properties.getGenerator();
        var topic     = properties.getKafka().getTopic();
        var bucket    = properties.getS3().getRawBucket();
        var tableName = properties.getS3().getTableName();

        log.info("Generating {} rows, writing to s3://{}/{}", gen.getRowsPerInterval(), bucket, tableName);

        List<JsonNode> rows = generatorRepository.generate(gen.getDataConfig(), gen.getRowsPerInterval());

        rows.forEach(row -> {
            var reference = s3FilePublisher.publish(row, bucket, tableName);
            kafkaMetadataPublisher.publish(reference, topic);
            log.debug("Written and published: {}", reference);
        });

        log.info("Generated and published {} rows", rows.size());
    }
}