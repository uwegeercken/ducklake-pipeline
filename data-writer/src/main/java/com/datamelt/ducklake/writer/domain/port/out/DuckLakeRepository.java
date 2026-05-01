package com.datamelt.ducklake.writer.domain.port.out;

import com.datamelt.ducklake.common.model.S3FileReference;

import java.util.List;

public interface DuckLakeRepository {

    void insertFromS3(List<S3FileReference> references);
}