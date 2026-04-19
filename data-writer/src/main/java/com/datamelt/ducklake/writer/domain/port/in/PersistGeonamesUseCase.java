package com.datamelt.ducklake.writer.domain.port.in;

import com.datamelt.ducklake.common.model.Geoname;

import java.util.List;

public interface PersistGeonamesUseCase {

    void persist(List<Geoname> geonames);
}