package com.datamelt.ducklake.initializer.infrastructure.config;

import com.datamelt.ducklake.initializer.domain.port.in.SetupDuckLakeUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SetupRunner {

    private static final Logger log = LoggerFactory.getLogger(SetupRunner.class);

    @Bean
    public CommandLineRunner run(SetupDuckLakeUseCase useCase) {
        return args -> {
            log.info("=== ducklake-initializer starting ===");
            useCase.setup();
            log.info("=== ducklake-initializer finished ===");
        };
    }
}