package com.datamelt.ducklake.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Domain-Objekt: Referenz auf eine Datei in S3/MinIO.
 * Wird vom data-generator nach Kafka gesendet und vom data-writer konsumiert.
 */
public class S3FileReference {

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("path")
    private String path;

    public S3FileReference() {}

    public S3FileReference(String bucket, String path) {
        this.bucket = bucket;
        this.path = path;
    }

    public String getBucket() { return bucket; }
    public void setBucket(String bucket) { this.bucket = bucket; }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }

    /** Vollständige S3 URL: s3://bucket/path */
    public String toS3Url() {
        return "s3://" + bucket + "/" + path;
    }

    @Override
    public String toString() {
        return "S3FileReference{bucket='" + bucket + "', path='" + path + "'}";
    }
}