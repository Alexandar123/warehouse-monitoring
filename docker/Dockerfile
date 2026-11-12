# One parameterised image for both services: --build-arg SERVICE=warehouse|central
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY common/pom.xml common/
COPY warehouse/pom.xml warehouse/
COPY central/pom.xml central/
# Cached layer: dependencies change less often than sources
RUN ./mvnw -B -q dependency:go-offline
COPY common common
COPY warehouse warehouse
COPY central central
ARG SERVICE
RUN ./mvnw -B -q package -DskipTests -pl ${SERVICE} -am

FROM eclipse-temurin:21-jre-alpine
ARG SERVICE
COPY --from=build /src/${SERVICE}/target/${SERVICE}.jar /app/app.jar
COPY --from=build /src/${SERVICE}/target/lib /app/lib
# exec form: the JVM is PID 1 and receives SIGTERM, so the graceful shutdown runs
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
