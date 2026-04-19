package com.datamelt.ducklake.initializer.infrastructure.duckdb;

public class DuckDbException extends RuntimeException {

    public DuckDbException(String message, Throwable cause) {
        super(message, cause);
    }
}