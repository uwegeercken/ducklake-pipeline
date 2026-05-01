package com.datamelt.ducklake.generator.application;

import com.datamelt.ducklake.generator.domain.port.in.GenerateAndPublishUseCase;
import com.datamelt.ducklake.generator.domain.port.out.DataGeneratorRepository;
import com.datamelt.ducklake.generator.domain.port.out.RowPublisher;
import com.datamelt.ducklake.generator.infrastructure.config.DataGeneratorProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GenerateAndPublishService implements GenerateAndPublishUseCase {

    private static final Logger log = LoggerFactory.getLogger(GenerateAndPublishService.class);

    private final DataGeneratorRepository generatorRepository;
    private final RowPublisher rowPublisher;
    private final DataGeneratorProperties properties;

    public GenerateAndPublishService(DataGeneratorRepository generatorRepository,
                                     RowPublisher rowPublisher,
                                     DataGeneratorProperties properties) {
        this.generatorRepository = generatorRepository;
        this.rowPublisher = rowPublisher;
        this.properties = properties;
    }

    @Override
    public void generateAndPublish() {
        var gen   = properties.getGenerator();
        var topic = properties.getKafka().getTopic();

        log.info("Generating {} rows from config: {}", gen.getRowsPerInterval(), gen.getDataConfig());

        List<JsonNode> jsonRows = generatorRepository.generate(gen.getDataConfig(), gen.getRowsPerInterval());

        log.info("Generated {} rows, publishing to topic '{}'", jsonRows.size(), topic);

        rowPublisher.publish(jsonRows, topic);

        log.info("Published {} rows successfully", jsonRows.size());
    }
}