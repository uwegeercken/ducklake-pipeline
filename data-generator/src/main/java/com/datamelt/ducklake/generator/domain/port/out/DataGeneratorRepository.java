package com.datamelt.ducklake.generator.domain.port.out;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/**
 * Outbound Port – generiert Rows via datagenerator2.
 */
public interface DataGeneratorRepository {

    List<JsonNode> generate(String dataConfigPath, int numberOfRows);
}