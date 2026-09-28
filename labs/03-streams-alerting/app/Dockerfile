# Fully qualified base images: Podman does not guess a registry for short names.
FROM docker.io/library/maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM docker.io/library/eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/payment-alerting.jar app.jar
ENTRYPOINT ["java", "-Xmx256m", "-jar", "app.jar"]
