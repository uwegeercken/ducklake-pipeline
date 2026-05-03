package com.datamelt.ducklake.generator;

import com.datamelt.ducklake.generator.infrastructure.config.DataGeneratorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DataGeneratorApplication
{
    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorApplication.class, args);

//        var ctx = SpringApplication.run(DataGeneratorApplication.class, args);
//        var props = ctx.getBean(DataGeneratorProperties.class);
//        System.out.println("S3 endpoint: " + props.getS3().getEndpoint());
//        System.out.println("S3 accessKey: " + props.getS3().getAccessKey());
//        System.out.println("S3 rawBucket: " + props.getS3().getRawBucket());
//        System.out.println();
    }
}