package com.datamelt.ducklake.generator.domain.port.out;

import com.datamelt.ducklake.common.model.S3FileReference;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Outbound Port – schreibt eine generierte Row als JSON Datei nach S3/MinIO
 * und gibt die Dateireferenz zurück.
 */
public interface S3FilePublisher {

    S3FileReference publish(JsonNode row, String bucket, String tableName);
}