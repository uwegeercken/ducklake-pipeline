package com.datamelt.ducklake.initializer.domain.port.out;

public interface DuckLakeRepository {

    void loadExtensions();

    void createS3Secret(String keyId, String secret, String endpoint);

    void attachDuckLake(String catalogConnectionString, String dataPath, String name, int dataInliningRowLimit);

    void createSchemaIfNotExists(String duckLakeName, String schema);

    void executeDDL(String ddl); }