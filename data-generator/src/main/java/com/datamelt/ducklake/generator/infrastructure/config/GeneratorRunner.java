package com.datamelt.ducklake.generator.infrastructure.config;

import com.datamelt.ducklake.generator.domain.port.in.GenerateAndPublishUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Startet den Generator-Anwendungsfall:
 *
 *  - intervalMs = 0 (default): einmalig ausführen und beenden (wie data-reader)
 *  - intervalMs > 0:           wiederholt alle N Millisekunden generieren und senden,
 *                              läuft bis der Prozess beendet wird (SIGTERM / CTRL+C)
 */
@Configuration
public class GeneratorRunner {

    private static final Logger log = LoggerFactory.getLogger(GeneratorRunner.class);

    @Bean
    public CommandLineRunner run(GenerateAndPublishUseCase useCase,
                                 DataGeneratorProperties properties) {
        return args -> {
            var gen = properties.getGenerator();

            if (gen.isRunOnce()) {
                runOnce(useCase);
            } else {
                runOnSchedule(useCase, gen.getIntervalMs());
            }
        };
    }

    private void runOnce(GenerateAndPublishUseCase useCase) {
        log.info("=== data-generator starting (run-once mode) ===");
        useCase.generateAndPublish();
        log.info("=== data-generator finished ===");
        System.exit(0);
    }

    private void runOnSchedule(GenerateAndPublishUseCase useCase, long intervalMs) {
        log.info("=== data-generator starting (interval mode: every {}ms) ===", intervalMs);

        var running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping generator ...");
            running.set(false);
        }));

        var scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            if (running.get()) {
                try {
                    useCase.generateAndPublish();
                } catch (Exception e) {
                    log.error("Error during generation cycle", e);
                }
            } else {
                scheduler.shutdown();
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);

        // Warte bis Scheduler beendet wird
        try {
            scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("=== data-generator stopped ===");
    }
}