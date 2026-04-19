package com.datamelt.ducklake.writer.application;

import com.datamelt.ducklake.common.model.Geoname;
import com.datamelt.ducklake.writer.domain.port.in.PersistGeonamesUseCase;
import com.datamelt.ducklake.writer.domain.port.out.GeonameRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Anwendungsfall-Service: persistiert einen Batch von Geoname-Datensätzen
 * in den DuckLake.
 */
@Service
public class PersistGeonamesService implements PersistGeonamesUseCase {

    private static final Logger log = LoggerFactory.getLogger(PersistGeonamesService.class);

    private final GeonameRepository repository;

    public PersistGeonamesService(GeonameRepository repository) {
        this.repository = repository;
    }

    @Override
    public void persist(List<Geoname> geonames) {
        if (geonames.isEmpty()) {
            log.debug("Empty batch received, skipping");
            return;
        }
        log.info("Persisting batch of {} geonames to DuckLake", geonames.size());
        repository.saveBatch(geonames);
        log.info("Batch persisted successfully");
    }
}