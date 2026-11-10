FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
RUN sh ./gradlew --no-daemon --version
COPY src src
RUN sh ./gradlew --no-daemon installDist -x test

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/install/sensor-measurement-service/lib lib
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/app/lib/*", "com.kevrsc.sensor.IngestMain"]
