package com.datamelt.ducklake.generator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DataGeneratorApplication
{

    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorApplication.class, args);
    }
}