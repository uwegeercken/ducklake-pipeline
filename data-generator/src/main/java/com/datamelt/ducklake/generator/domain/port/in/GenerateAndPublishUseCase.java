package com.datamelt.ducklake.generator.domain.port.in;

/**
 * Inbound Port – Anwendungsfall "Daten generieren und nach Kafka senden".
 */
public interface GenerateAndPublishUseCase {

    void generateAndPublish();
}