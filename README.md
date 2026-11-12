# Warehouse Monitoring

Two independently runnable services:

- **warehouse** receives sensor readings over UDP and forwards every valid reading to Kafka.
- **central** reads all readings from Kafka and logs an `ALARM` line when a reading is above its threshold.

```
sensors (netcat, UDP)
  :3344 temperature --+        WAREHOUSE (JVM 1)
  :3355 humidity    --+--> UdpIngress, one per port          [Netty event loop]
                           bytes -> text, parse line, invalid: log + count + drop
                           Measurement(warehouseId, sensorId, type of port, value, receivedAt)
                           drop if no demand (counted)
                               |                             [kafka-send thread]
                           KafkaProducer.send  key = warehouseId:sensorId, JSON value
                               v
                    Kafka topic "measurements" (3 partitions)
                               v                             CENTRAL (JVM 2)
                           KafkaConsumer.poll                [kafka-poll thread]
                           decode + validate JSON, invalid: log + skip
                           value > threshold -> log ALARM (WARN)
```

Java 21, Project Reactor + Reactor Netty (UDP only), plain `kafka-clients`, Jackson, SLF4J/Logback. No Spring.
Maven multi-module build: `common` (message contract), `warehouse`, `central`.

## Build and test

```
./mvnw verify
```

Runs all tests (the Maven wrapper downloads Maven 3.9 on first use; a local Maven 3.8+ works too). The end-to-end tests start Kafka with Testcontainers and are skipped when Docker is not available.

## Run with Docker Compose

```
docker compose up --build
```

This starts Kafka (single-node KRaft), creates the topic (`kafka-init`), then `central`, `warehouse` (id `w1`, UDP
3344/3355) and `warehouse-2` (id `w2`, UDP 4344/4355 on the host). Watch alarms with `docker compose logs -f central`.

If `docker compose build` fails with `unknown flag: --allow`, the local buildx plugin is older than Compose expects.
Build the images directly and start without building:

```
docker build -f docker/Dockerfile --build-arg SERVICE=central   -t warehouse-monitoring/central .
docker build -f docker/Dockerfile --build-arg SERVICE=warehouse -t warehouse-monitoring/warehouse .
docker compose up --no-build
```

## Run without Docker for the services

Start only Kafka and the topic with `docker compose up kafka kafka-init`, then on the host:

```
./mvnw package -DskipTests
java -jar central/target/central.jar
WAREHOUSE_ID=w1 java -jar warehouse/target/warehouse.jar
```

Each jar finds its dependencies in the `lib/` directory next to it.

Both default to `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, which is the host listener of the Compose Kafka.

## Simulating sensors with netcat

Tested with OpenBSD netcat (`nc -h` prints `OpenBSD netcat`). `-u` is UDP, `-w1` quits one second after EOF.
Other variants may need `-q1` instead.

```
echo "sensor_id=t1; value=30"   | nc -u -w1 localhost 3344   # no alarm
echo "sensor_id=t1; value=36"   | nc -u -w1 localhost 3344   # temperature alarm
echo "sensor_id=h1; value=51"   | nc -u -w1 localhost 3355   # humidity alarm
echo "garbage"                  | nc -u -w1 localhost 3344   # dropped, processing continues
echo "sensor_id=t1; value=37"   | nc -u -w1 localhost 4344   # alarm from warehouse w2
```

Expected central output:

```
WARN  [kafka-poll-1] ALARM - ALARM temperature above threshold: warehouse=w1 sensor=t1 value=36.0 threshold=35.0 receivedAt=2026-11-12T15:13:16.063Z
```

Stop central (`docker compose stop central`), send readings, start it again: the alarms appear late, with the
original `receivedAt`.

## Configuration

Environment variables only. All are validated at startup; every problem is reported at once and the process exits
with code 2.

| Variable                  | Service   | Default              | Validation                                               |
|---------------------------|-----------|----------------------|----------------------------------------------------------|
| `WAREHOUSE_ID`            | warehouse | required             | 1-64 characters of `A-Z a-z 0-9 _ -`                     |
| `TEMPERATURE_PORT`        | warehouse | `3344`               | integer 0-65535 (0 = any free port)                      |
| `HUMIDITY_PORT`           | warehouse | `3355`               | integer 0-65535, different from `TEMPERATURE_PORT` unless both are 0 |
| `BIND_HOST`               | warehouse | `0.0.0.0`            | not blank                                                |
| `KAFKA_BOOTSTRAP_SERVERS` | both      | `localhost:9092`     | not blank                                                |
| `KAFKA_TOPIC`             | both      | `measurements`       | not blank                                                |
| `KAFKA_GROUP_ID`          | central   | `central-monitoring` | not blank                                                |
| `TEMPERATURE_THRESHOLD`   | central   | `35`                 | finite number                                            |
| `HUMIDITY_THRESHOLD`      | central   | `50`                 | finite number between 0 and 100                          |

## Assumptions

- **A1** An alarm is raised when the value is strictly greater than the threshold. Equal is not an alarm.
- **A2** Values may be decimal and negative (`-?digits(.digits)?`). NaN, Infinity, exponents, `30f`, `.5`, `30.` are rejected.
- **A3** Whitespace around keys, values, `=` and `;` is tolerated, as is a trailing newline and one trailing `;`.
  Keys are case-sensitive; exactly `sensor_id` and `value`, in any order. One datagram may carry several lines.
- **A4** The sensor type is decided by the UDP port, never by the sensor id. `h7` on 3344 is a temperature reading.
- **A5** Every reading above the threshold produces one alarm line (no edge triggering).
- **A6** Each warehouse has a unique configured `WAREHOUSE_ID`, attached to every forwarded reading.
- **A7** Sensor to warehouse is at-most-once (UDP); warehouse to central is at-least-once (Kafka). Nothing is exactly-once.
- **A8** Malformed input is logged (DEBUG), counted and dropped. It never stops processing.
- **A9** Duplicates are not removed: the sensor format has no message id, so a duplicate cannot be told apart from
  two equal genuine readings.
- **A10** Physical ranges (for example humidity above 100 %) are not validated; the specification does not define them.

## Delivery guarantees, ordering, overload

Sensor to warehouse is at-most-once; warehouse to alarm is at-least-once; there is no end-to-end guarantee and no
exactly-once claim. Ordering is preserved per sensor from the moment the warehouse receives a reading (record key
`warehouseId:sensorId`, idempotent producer, synchronous processing in central). Duplicates are possible after a
central crash (the last uncommitted batch is processed again) and are intentionally not removed.

| Where data can be lost                 | Possible             | Mitigation                          |
|----------------------------------------|----------------------|-------------------------------------|
| Network between sensor and warehouse   | yes, invisible       | none possible with UDP              |
| Kernel UDP receive buffer full         | yes, OS counters     | event loop does no blocking work    |
| Drop before the send thread            | yes, counted         | bounded by design                   |
| Producer timeout or full buffer        | yes, logged (WARN)   | bounded by design                   |
| Warehouse killed with SIGKILL          | yes, producer buffer | graceful shutdown covers SIGTERM    |
| Single broker, replication factor 1    | yes, on disk loss    | replication in production           |
| Kafka to central                       | no                   | at-least-once (auto-commit after synchronous processing) |

**Overload policy.** There are exactly three bounded buffers and no unbounded one: the kernel UDP receive buffer, the
`publishOn` queue (256) and the producer's `buffer.memory`. When Kafka is slow or down the producer buffer fills, `send`
fails within `max.block.ms` (5 s), the reading is logged and dropped, and the event loop is never blocked. Readings
arriving while the send thread is busy are dropped and counted. Backpressure does not reach the sensors: it stops at
the UDP socket, and the design chooses where loss happens and makes it visible. Counters
(`accepted`, `rejected`, `dropped`, `failed`) are logged at shutdown.

| Failure                                   | Behaviour                                                       |
|-------------------------------------------|-----------------------------------------------------------------|
| Malformed datagram or line                | dropped, DEBUG line, reject counter                             |
| Datagram above 1 KB                       | dropped, WARN line                                              |
| Kafka unreachable at warehouse startup    | exits non-zero before any port is bound                         |
| Kafka down while warehouse runs           | producer retries; after timeout readings are dropped (WARN); recovers by itself |
| Central down                              | readings wait in Kafka; processed on restart with original `receivedAt` |
| Poison record in Kafka                    | WARN with partition and offset, skipped                         |
| Port already in use                       | exits non-zero with an ERROR line                               |
| Invalid configuration                     | exits with code 2, every problem listed                         |

## Design decisions

- **Reactor Netty for UDP**: non-blocking receive on the event loop; only the UDP server is used, no HTTP.
- **Plain `kafka-clients`** instead of Reactor Kafka, which is discontinued. The two blocking points are bridged by hand:
  `send` runs on a dedicated `kafka-send` thread and its callback becomes a `Mono`; `poll` runs on a dedicated
  `kafka-poll` thread, and the consumer is touched only there (shutdown uses `wakeup()`).
- **Kafka instead of HTTP/TCP**: a central outage loses nothing, there is no retry amplification against central,
  per-sensor ordering comes from the key, and central scales with partitions and consumer groups. Cost: one more piece
  of infrastructure and slower end-to-end tests.
- **One topic** for both sensor types; the type is a field. Split it only if the types need different retention or consumers.
- **JSON** with Jackson; the consumer reads raw bytes and decodes per record, so a bad record is skipped, never a poison pill.
- **Fail fast at startup, keep running during an outage**: a misconfigured bootstrap address is a deployment error;
  a broker restart at runtime is an operational event the producer already handles.
- **No Spring, no DI framework**: the object graph is small enough to wire by hand in each `main`.
- **Reactor in central** brings little performance (polling is blocking by design). Its value there is a composable,
  testable chain with per-record error isolation and an explicit threading model.

## Testing

| Layer           | Infrastructure                              | Classes                                                          |
|-----------------|---------------------------------------------|------------------------------------------------------------------|
| Unit            | none                                        | `MeasurementParserTest`, `MeasurementJsonTest`, `ThresholdEvaluatorTest`, `AlarmLoggerTest`, `*ConfigTest` |
| UDP integration | real `UdpServer` on port 0, `MockProducer`  | `UdpIngressTest`, `WarehouseStartupTest`                          |
| Consumer        | `MockConsumer`                              | `MeasurementConsumerTest`                                        |
| End-to-end      | Testcontainers Kafka, both services in-process | `EndToEndTest`                                               |

Tests run with `io.netty.leakDetection.level=paranoid`.

| Failure mode                              | Covered by                                                              |
|-------------------------------------------|-------------------------------------------------------------------------|
| Malformed input stops processing          | `malformedDatagramDoesNotStopProcessing`, `garbageOnOnePortDoesNotAffectTheOther` |
| `send` fails (callback or synchronous)    | `sendFailingThroughTheCallbackIsIsolated`, `sendThrowingSynchronouslyIsIsolated` |
| `send` blocks the event loop              | `producerSendRunsOnKafkaSendThreadNeverOnEventLoop`                     |
| Overload buffers without bound            | `overloadIsDroppedAndCountedInsteadOfBuffered`                          |
| Oversized datagram                        | `datagramAboveSizeLimitIsRejectedWithoutParsing`                        |
| Port in use / rebinding                   | `secondServerOnUsedPortFailsAtStartup`, `disposedPortCanBeBoundAgain`   |
| Per-sensor order                          | `readingsOfOneSensorKeepTheirOrder`                                     |
| Kafka unreachable at startup              | `startupFailsFastWhenKafkaIsUnreachableAndNoPortIsBound`                |
| Poison record                             | `garbageRecordIsSkippedAndPositionMovesPastIt`, `validJsonWithInvalidContentIsSkipped` |
| Clean consumer shutdown                   | `wakeupEndsTheLoopAndClosesConsumerOnPollThread`                        |
| Threshold boundary / swapped thresholds   | `alarmOnlyStrictlyAboveThreshold`, `sameValueIsJudgedByItsOwnTypeThreshold` |
| Central down, backlog                     | `readingsSentWhileCentralIsDownRaiseAlarmsOnceItStarts`                 |
| Multiple warehouses                       | `alarmsFromTwoWarehousesAreDistinguished`                               |

## Out of scope and future work

- Authentication of UDP senders; TLS/SASL for Kafka.
- Metrics and health endpoints; alerting integration instead of log lines.
- Edge-triggered alarms (alarm on state change), which need a bounded state map because sensor ids are attacker-controlled.
- Physical range validation per sensor type.
- Replication and schema management for production.
- Not yet tested: re-processing after a consumer restart in the same group, producer flush on shutdown under high
  `linger.ms`, and a Kafka pause while the warehouse runs.
