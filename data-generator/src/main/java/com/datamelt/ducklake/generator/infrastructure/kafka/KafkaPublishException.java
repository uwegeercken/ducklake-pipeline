package com.datamelt.ducklake.generator.infrastructure.kafka;

public class KafkaPublishException extends RuntimeException
{
        public KafkaPublishException(String message, Throwable cause) {
            super(message, cause);
        }
}
