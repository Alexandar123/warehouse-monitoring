package dev.monitoring.common;

import java.util.regex.Pattern;

public record Measurement(String warehouseId, String sensorId, SensorType type, double value, long receivedAt) {

    public static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    public String key() {
        return warehouseId + ":" + sensorId;
    }
}
