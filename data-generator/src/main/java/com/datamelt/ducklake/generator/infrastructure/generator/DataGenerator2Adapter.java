package com.datamelt.ducklake.generator.infrastructure.generator;

import com.datamelt.ducklake.generator.domain.port.out.DataGeneratorRepository;
import com.datamelt.utilities.datagenerator.application.RowGenerator;
import com.datamelt.utilities.datagenerator.config.model.DataConfiguration;
import com.datamelt.utilities.datagenerator.error.Try;
import com.datamelt.utilities.datagenerator.format.JsonFormatter;
import com.datamelt.utilities.datagenerator.generate.Row;
import com.datamelt.utilities.datagenerator.generate.RowField;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Outbound Adapter: nutzt datagenerator2 RowGenerator um Rows zu erzeugen.
 *
 * DataConfiguration wird via Jackson + YAMLFactory aus der YAML-Datei geladen.
 * ProgramConfiguration wird nicht benötigt – sie ist nur für die CLI-Variante.
 *
 * Konvertierung Row → Map<String, Object>:
 *   - Flache Felder:  "name"         → {"name": "Anna"}
 *   - Dot-Notation:   "address.city" → {"address": {"city": "Berlin"}}
 *   - Mehrfach-tief:  "a.b.c"        → {"a": {"b": {"c": "value"}}}
 */
@Component
public class DataGenerator2Adapter implements DataGeneratorRepository {

    private static final Logger log = LoggerFactory.getLogger(DataGenerator2Adapter.class);

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    @Override
    public List<JsonNode> generate(String dataConfigurationFile, int numberOfRows)
    {
        try {
            RowGenerator rowGenerator = new RowGenerator(dataConfigurationFile);
            List<JsonNode> result = rowGenerator.generateRows(numberOfRows)
                    .filter(Try::isSuccess)
                    .map(Try::getResult)
                    .map(this::convertToJsonNode)
                    .collect(Collectors.toList());

            log.debug("Generated {} rows successfully", result.size());
            return result;

        } catch (Exception e) {
            throw new DataGeneratorException("Failed to generate rows", e);
        }
    }

    private JsonNode convertToJsonNode(Row row)
    {
        try
        {
            return JsonFormatter.convertToJsonNode(row);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
    }
}