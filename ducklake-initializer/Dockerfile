FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

# POMs zuerst für Layer-Caching
COPY pom.xml ./pom.xml
COPY common/pom.xml ./common/pom.xml
COPY ducklake-initializer/pom.xml ./ducklake-initializer/pom.xml
COPY data-reader/pom.xml ./data-reader/pom.xml
COPY data-writer/pom.xml ./data-writer/pom.xml

# Dependencies offline holen (nur für common + initializer)
RUN mvn dependency:go-offline -pl common,ducklake-initializer -am -q

# Source
COPY common/src ./common/src
COPY ducklake-initializer/src ./ducklake-initializer/src

RUN mvn package -pl ducklake-initializer -am -DskipTests -q

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/ducklake-initializer/target/ducklake-initializer-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]