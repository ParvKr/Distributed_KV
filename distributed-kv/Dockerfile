# Build stage: compile and package the runnable jar (tests are skipped here; run them with `mvn test`).
FROM maven:3-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml .
COPY .mvn .mvn
COPY src src
RUN mvn -q -B -DskipTests package

# Runtime stage: just a JRE and the jar, running as an unprivileged user.
FROM eclipse-temurin:25-jre
RUN groupadd --system raftkv && useradd --system --gid raftkv --home-dir /data raftkv \
    && mkdir -p /data && chown raftkv:raftkv /data
COPY --from=build /build/target/raftkv-jar-with-dependencies.jar /app/raftkv.jar
USER raftkv
WORKDIR /data
# Raft state (term, vote, log, snapshots) lives here; mount a volume so it survives container restarts.
VOLUME /data
EXPOSE 8001
# Arguments are the node's flags, e.g. --id=1 --port=8001 --dataDir=/data --peers=2:node2:8001,3:node3:8001
ENTRYPOINT ["java", "-jar", "/app/raftkv.jar"]
