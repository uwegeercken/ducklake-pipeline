package com.datamelt.ducklake.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Domain-Modell: ein GeoNames-Datensatz.
 *
 * Wird von data-reader (JSON lesen + nach Kafka senden) und
 * data-writer (aus Kafka lesen + in DuckLake schreiben) genutzt.
 *
 * @JsonIgnoreProperties erlaubt zusätzliche Felder in der JSON-Quelldatei.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Geoname {

    @JsonProperty("geonameid")
    private Long geonameid;

    @JsonProperty("name")
    private String name;

    @JsonProperty("asciiname")
    private String asciiname;

    @JsonProperty("country_code")
    private String countryCode;

    @JsonProperty("population")
    private Long population;

    @JsonProperty("latitude")
    private Double latitude;

    @JsonProperty("longitude")
    private Double longitude;

    @JsonProperty("timezone")
    private String timezone;

    public Geoname() {}

    public Long getGeonameid() { return geonameid; }
    public void setGeonameid(Long geonameid) { this.geonameid = geonameid; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getAsciiname() { return asciiname; }
    public void setAsciiname(String asciiname) { this.asciiname = asciiname; }

    public String getCountryCode() { return countryCode; }
    public void setCountryCode(String countryCode) { this.countryCode = countryCode; }

    public Long getPopulation() { return population; }
    public void setPopulation(Long population) { this.population = population; }

    public Double getLatitude() { return latitude; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }

    public Double getLongitude() { return longitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }

    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }

    @Override
    public String toString() {
        return "Geoname{geonameid=" + geonameid
                + ", name='" + name + "'"
                + ", country=" + countryCode
                + ", population=" + population + "}";
    }
}