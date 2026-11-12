package dev.monitoring.common;

import java.util.regex.Pattern;

/**
 * One sensor reading as forwarded from a warehouse to the central service.
 *
 * @param receivedAt epoch millis at which the warehouse received the datagram
 */
public record Measurement(String warehouseId, String sensorId, SensorType type, double value, long receivedAt) {

    /** Allowed shape of warehouse and sensor ids; also keeps control characters out of log lines. */
    public static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    /** Kafka record key: keeps all readings of one sensor in one partition, so in order. */
    public String key() {
        return warehouseId + ":" + sensorId;
    }
}
