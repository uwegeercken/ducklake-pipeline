package com.datamelt.ducklake.writer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DataWriterApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataWriterApplication.class, args);
    }
}