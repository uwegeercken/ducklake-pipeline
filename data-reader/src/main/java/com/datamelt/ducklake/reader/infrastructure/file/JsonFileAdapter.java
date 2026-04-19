package com.datamelt.ducklake.reader.infrastructure.file;

import com.datamelt.ducklake.common.model.Geoname;
import com.datamelt.ducklake.reader.domain.port.out.GeonameFileRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Outbound Adapter: liest eine JSON-Datei mit einem Array von Geoname-Datensätzen.
 *
 * Erwartet Format: [ { "geonameid": 1, "name": "..." }, ... ]
 */
@Component
public class JsonFileAdapter implements GeonameFileRepository {

    private static final Logger log = LoggerFactory.getLogger(JsonFileAdapter.class);
    private static final TypeReference<List<Geoname>> GEONAME_LIST_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    public JsonFileAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public List<Geoname> readAll(String filePath) {
        var path = Path.of(filePath);

        if (!Files.exists(path)) {
            throw new FileReadException("Data file not found: " + filePath);
        }

        log.debug("Reading file: {}", path.toAbsolutePath());

        try {
            var geonames = objectMapper.readValue(path.toFile(), GEONAME_LIST_TYPE);
            log.info("Successfully read {} records from {}", geonames.size(), filePath);
            return geonames;
        } catch (IOException e) {
            throw new FileReadException("Failed to parse JSON file: " + filePath, e);
        }
    }
}