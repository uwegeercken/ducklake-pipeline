package com.datamelt.ducklake.generator.domain.port.out;

import com.datamelt.ducklake.common.model.S3FileReference;

/**
 * Outbound Port – sendet eine S3FileReference als Kafka Nachricht.
 */
public interface KafkaMetadataPublisher {

    void publish(S3FileReference reference, String topic);
}