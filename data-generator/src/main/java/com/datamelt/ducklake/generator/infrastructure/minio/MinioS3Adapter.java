package com.datamelt.ducklake.generator.infrastructure.minio;

import com.datamelt.ducklake.common.model.S3FileReference;
import com.datamelt.ducklake.generator.domain.port.out.S3FilePublisher;
import com.datamelt.ducklake.generator.infrastructure.config.DataGeneratorProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

/**
 * Outbound Adapter: schreibt eine Row als JSON Datei nach MinIO via AWS S3 SDK.
 *
 * Dateipfad: <tableName>/<timestamp>-<uuid>.json
 * Beispiel:  person/2026-05-01T10-00-00.123Z-a9b3c4d5.json
 *
 * Der S3Client wird einmalig beim ersten Aufruf initialisiert (lazy).
 */
@Component
public class MinioS3Adapter implements S3FilePublisher {

    private static final Logger log = LoggerFactory.getLogger(MinioS3Adapter.class);
    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final DataGeneratorProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private S3Client s3Client;

    public MinioS3Adapter(DataGeneratorProperties properties) {
        this.properties   = properties;
    }

    @Override
    public S3FileReference publish(JsonNode row, String bucket, String tableName) {
        try {
            var client  = getOrCreateS3Client();
            var path    = buildPath(tableName);
            var content = objectMapper.writeValueAsBytes(row);

            var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .contentType("application/json")
                    .contentLength((long) content.length)
                    .build();

            client.putObject(request, RequestBody.fromBytes(content));

            log.debug("Written s3://{}/{}", bucket, path);
            return new S3FileReference(bucket, path);

        } catch (Exception e) {
            throw new MinioPublishException("Failed to write row to MinIO", e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String buildPath(String tableName) {
        var timestamp = TIMESTAMP_FMT.format(Instant.now());
        var uid       = UUID.randomUUID().toString().substring(0, 8);
        return tableName + "/" + timestamp + "-" + uid + ".json";
    }

    private S3Client getOrCreateS3Client() {
        if (s3Client == null) {
            var s3props = properties.getS3();
            var credentials = AwsBasicCredentials.create(s3props.getAccessKey(), s3props.getSecretKey());

            s3Client = S3Client.builder()
                    .endpointOverride(URI.create(s3props.getEndpoint()))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)   // MinIO benötigt path-style (nicht virtual-hosted)
                    .build();

            log.info("S3 client initialized for endpoint: {}", s3props.getEndpoint());
        }
        return s3Client;
    }
}