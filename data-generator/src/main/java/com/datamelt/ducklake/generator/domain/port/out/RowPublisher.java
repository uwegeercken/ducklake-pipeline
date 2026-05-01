package com.datamelt.ducklake.generator.domain.port.out;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * Outbound Port – sendet generierte Rows als JSON nach Kafka.
 *
 * Jede Row wird als Map übergeben (field name → value),
 * wobei Dot-Notation bereits zu verschachtelten Maps aufgelöst wurde.
 */
public interface RowPublisher {

    void publish(List<JsonNode> jsonRows, String topic);
}