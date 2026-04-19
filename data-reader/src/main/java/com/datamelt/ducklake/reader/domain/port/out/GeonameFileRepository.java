package com.datamelt.ducklake.reader.domain.port.out;

import com.datamelt.ducklake.common.model.Geoname;

import java.util.List;

public interface GeonameFileRepository {

    List<Geoname> readAll(String filePath);
}