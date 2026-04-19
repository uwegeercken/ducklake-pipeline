package com.datamelt.ducklake.reader.infrastructure.config;

import com.datamelt.ducklake.reader.domain.port.in.ReadAndPublishUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ReaderRunner {

    private static final Logger log = LoggerFactory.getLogger(ReaderRunner.class);

    @Bean
    public CommandLineRunner run(ReadAndPublishUseCase useCase) {
        return args -> {
            log.info("=== data-reader starting ===");
            useCase.readAndPublish();
            log.info("=== data-reader finished ===");
        };
    }
}