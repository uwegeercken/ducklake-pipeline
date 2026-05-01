package com.datamelt.ducklake.writer.domain.port.in;

import com.datamelt.ducklake.common.model.S3FileReference;

import java.util.List;

public interface PersistFromS3UseCase {

    void persist(List<S3FileReference> references);
}