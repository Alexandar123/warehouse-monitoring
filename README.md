# Warehouse Monitoring

Two reactive services. **warehouse** receives sensor readings over UDP and publishes them to Kafka.
**central** consumes them and logs an `ALARM` when a reading is above its threshold.

```
sensors --UDP 3344 (temperature)-> warehouse --Kafka topic "measurements"--> central --> ALARM in log
        --UDP 3355 (humidity)----->            key = warehouseId:sensorId
```

Java 21, Project Reactor + Reactor Netty, plain `kafka-clients`, Jackson, Maven. No Spring.
Modules: `common` (message format), `warehouse`, `central`.

## Run

```
docker compose up --build
docker compose logs -f central
```

Send readings (OpenBSD netcat; other variants may need `-q1` instead of `-w1`):

```
echo "sensor_id=t1; value=30" | nc -u -w1 localhost 3344   # no alarm
echo "sensor_id=t1; value=36" | nc -u -w1 localhost 3344   # temperature alarm
echo "sensor_id=h1; value=51" | nc -u -w1 localhost 3355   # humidity alarm
echo "sensor_id=t1; value=37" | nc -u -w1 localhost 4344   # alarm from warehouse w2
```

```
WARN [kafka-poll-1] ALARM - ALARM temperature above threshold: warehouse=w1 sensor=t1 value=36.0 threshold=35.0 receivedAt=...
```

Build and test: `./mvnw verify` (end-to-end tests use Testcontainers and are skipped without Docker).
CI (`.github/workflows/ci.yml`) runs the tests and publishes both images to GitHub Container Registry from `main`.

## Configuration (environment variables)

| Variable                  | Service   | Default              |
|---------------------------|-----------|----------------------|
| `WAREHOUSE_ID`            | warehouse | required             |
| `TEMPERATURE_PORT` / `HUMIDITY_PORT` | warehouse | `3344` / `3355` |
| `KAFKA_BOOTSTRAP_SERVERS` | both      | `localhost:9092`     |
| `KAFKA_TOPIC`             | both      | `measurements`       |
| `KAFKA_GROUP_ID`          | central   | `central-monitoring` |
| `TEMPERATURE_THRESHOLD` / `HUMIDITY_THRESHOLD` | central | `35` / `50` |

Invalid configuration is reported all at once and the service exits.

## Assumptions

- Alarm when the value is **strictly greater** than the threshold; one alarm per reading.
- Sensor type comes from the UDP port, not the sensor id.
- Values may be negative or decimal; `NaN`, `Infinity`, exponents are rejected. Malformed lines are logged and dropped.
- Every warehouse has a unique `WAREHOUSE_ID`, attached to each reading.
- Duplicates are not removed: readings have no message id.

## Design decisions

- **Kafka between the services**: central can be down without losing data, scales with consumer groups, and
  ordering per sensor comes from the record key. Cost: one more piece of infrastructure.
- **Threads**: UDP is received on the Netty event loop, which never blocks; `producer.send` runs on a dedicated
  `kafka-send` thread; central polls on one `kafka-poll` thread and processes each batch before the next poll.
- **Guarantees**: sensor to warehouse is at-most-once (UDP); warehouse to central is at-least-once. Under overload
  readings are dropped and counted rather than buffered without limit.
- **Fail fast**: the warehouse refuses to start if Kafka is unreachable, but survives a Kafka outage at runtime.

## Future work

Edge-triggered alarms, sending alarms back to warehouses, metrics and health checks, Kafka security.
