package com.datamelt.ducklake.writer.infrastructure.duckdb;

public class DuckDbWriteException extends RuntimeException {

    public DuckDbWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}