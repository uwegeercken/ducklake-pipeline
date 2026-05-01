package com.datamelt.ducklake.generator.infrastructure.minio;

public class MinioPublishException extends RuntimeException
{
        public MinioPublishException(String message, Throwable cause) {
            super(message, cause);
        }
}
