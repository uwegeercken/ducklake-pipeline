package com.datamelt.ducklake.writer.application;

import com.datamelt.ducklake.common.model.S3FileReference;
import com.datamelt.ducklake.writer.domain.port.in.PersistFromS3UseCase;
import com.datamelt.ducklake.writer.domain.port.out.DuckLakeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestriert den Anwendungsfall "S3 Dateien in DuckLake schreiben":
 *  1. Liste von S3FileReferences empfangen (aus Kafka Batch)
 *  2. Für jede Referenz: INSERT INTO ducklake SELECT * FROM read_json_auto(s3://...)
 */
@Service
public class PersistFromS3Service implements PersistFromS3UseCase {

    private static final Logger log = LoggerFactory.getLogger(PersistFromS3Service.class);

    private final DuckLakeRepository repository;

    public PersistFromS3Service(DuckLakeRepository repository) {
        this.repository = repository;
    }

    @Override
    public void persist(List<S3FileReference> references) {
        if (references.isEmpty()) {
            log.debug("Empty batch, skipping");
            return;
        }
        log.info("Persisting {} files from S3 to DuckLake", references.size());
        repository.insertFromS3(references);
        log.info("Batch of {} files persisted successfully", references.size());
    }
}