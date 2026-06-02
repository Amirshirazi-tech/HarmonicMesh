# HarmonicMesh CEP Jobs (Java)

Flink CEP jobs for HarmonicMesh, built as a fat JAR for submission to the
Docker compose cluster (Flink 1.19, see `docker-compose.yml`).

The Python CEP API does not exist in any released PyFlink version. All CEP
work in this project lives here, in Java. Other phases (machine simulator,
LangGraph agent) remain Python.

## Prerequisites

- JDK 11 (`java -version` should report `11.x`)
- Maven 3.6+ — this repo's local install is at
  `/home/amir/maven/apache-maven-3.9.9/bin/mvn`. If not on `PATH`, alias it:

      alias mvn=/home/amir/maven/apache-maven-3.9.9/bin/mvn

## Build

    mvn -f flink_jobs/java/pom.xml clean package

The shaded JAR lands at `flink_jobs/java/target/harmonicmesh-cep-1.0-SNAPSHOT.jar`.

## Test

    mvn -f flink_jobs/java/pom.xml test

The six tests in `ThermalVibrationCascadeJobTest` run against an embedded
Flink mini-cluster fed by `fromCollection(...)` — no Kafka required.

## Submit to the cluster

The `docker-compose.yml` already pipes `KAFKA_SASL_USERNAME` and
`KAFKA_SASL_PASSWORD` into the Flink services.  Rebuild the JAR first, then
copy and submit:

    mvn -f flink_jobs/java/pom.xml clean package
    docker cp flink_jobs/java/target/harmonicmesh-cep-1.0-SNAPSHOT.jar \
              harmonicmesh-flink-jm:/tmp/harmonicmesh-cep.jar

### Pattern 1 — ThermalVibrationCascade (default mainClass)

    docker compose exec flink-jobmanager flink run \
        /tmp/harmonicmesh-cep.jar

### Pattern 2 — MissingHeartbeat

    docker compose exec flink-jobmanager flink run \
        -c com.harmonicmesh.MissingHeartbeatJob \
        /tmp/harmonicmesh-cep.jar

### Pattern 3 — EDISequenceViolation

    docker compose exec flink-jobmanager flink run \
        -c com.harmonicmesh.EDISequenceViolationJob \
        /tmp/harmonicmesh-cep.jar

All three jobs can run concurrently in the same cluster; each uses a distinct
consumer group ID and writes to a separate sink topic.

## Configuration

CLI flags (all optional except SASL via env):

| Flag | Default | Notes |
|---|---|---|
| `--bootstrap` | `kafka:29092` | Or `KAFKA_BOOTSTRAP_SERVERS` env. |
| `--source-topic` | `harmonicmesh.sensors.machine-03` | |
| `--sink-topic` | `harmonicmesh.patterns.machine-03` | |
| `--group-id` | `harmonicmesh-cep-thermal-vibration` | |
| `--machine-id` | `Machine-03` | Key into `machine_baselines.yaml`. |
| `--baselines` | _(classpath resource)_ | External YAML override. |
| `--starting-offsets` | `latest` | Or `earliest`. |
| `--parallelism` | `1` | |

`KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` are read from the env at
startup and required.

## Threshold mapping

See `docs/patterns.md` for the cascade threshold predicates and rationale.
The single source of truth for the values themselves is
`flink_jobs/config/machine_baselines.yaml`, included on the classpath at
build time.
