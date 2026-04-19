package com.datamelt.ducklake.writer.domain.port.out;

import com.datamelt.ducklake.common.model.Geoname;

import java.util.List;

public interface GeonameRepository {

    void saveBatch(List<Geoname> geonames);
}