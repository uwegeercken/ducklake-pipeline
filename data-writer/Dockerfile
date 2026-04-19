FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom.xml ./pom.xml
COPY common/pom.xml ./common/pom.xml
COPY ducklake-initializer/pom.xml ./ducklake-initializer/pom.xml
COPY data-reader/pom.xml ./data-reader/pom.xml
COPY data-writer/pom.xml ./data-writer/pom.xml

RUN mvn dependency:go-offline -pl common,data-writer -am -q

COPY common/src ./common/src
COPY data-writer/src ./data-writer/src

RUN mvn package -pl data-writer -am -DskipTests -q

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/data-writer/target/data-writer-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]