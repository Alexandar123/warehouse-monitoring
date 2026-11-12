package dev.monitoring.warehouse;

import dev.monitoring.common.Measurement;

import java.util.regex.Pattern;

/**
 * Parses one line of the sensor protocol: {@code sensor_id=<id>; value=<number>}.
 * Pure and stateless; knows nothing about ports, threads or Kafka.
 */
public final class MeasurementParser {

    /** Optional minus, digits, optional fraction. Double.parseDouble alone would also accept NaN, 3e2, 30f, 0x1p5. */
    private static final Pattern NUMBER = Pattern.compile("-?[0-9]+(\\.[0-9]+)?");
    private static final int MAX_NUMBER_LENGTH = 20;

    public record Reading(String sensorId, double value) {
    }

    public Reading parse(String line) {
        String[] segments = line.strip().split(";", -1);
        int count = segments.length;
        if (count > 1 && segments[count - 1].isBlank()) {
            count--; // one trailing ';' is tolerated
        }
        String sensorId = null;
        String value = null;
        for (int i = 0; i < count; i++) {
            String segment = segments[i].strip();
            if (segment.isEmpty()) {
                throw new InvalidMeasurementException("empty segment");
            }
            int eq = segment.indexOf('=');
            if (eq < 0) {
                throw new InvalidMeasurementException("missing '='");
            }
            String key = segment.substring(0, eq).strip();
            String raw = segment.substring(eq + 1).strip();
            switch (key) {
                case "sensor_id" -> {
                    if (sensorId != null) {
                        throw new InvalidMeasurementException("duplicate key sensor_id");
                    }
                    sensorId = raw;
                }
                case "value" -> {
                    if (value != null) {
                        throw new InvalidMeasurementException("duplicate key value");
                    }
                    value = raw;
                }
                default -> throw new InvalidMeasurementException("unknown key");
            }
        }
        if (sensorId == null) {
            throw new InvalidMeasurementException("missing key sensor_id");
        }
        if (value == null) {
            throw new InvalidMeasurementException("missing key value");
        }
        if (!Measurement.ID_PATTERN.matcher(sensorId).matches()) {
            throw new InvalidMeasurementException("invalid sensor_id");
        }
        if (value.length() > MAX_NUMBER_LENGTH || !NUMBER.matcher(value).matches()) {
            throw new InvalidMeasurementException("invalid number");
        }
        double number = Double.parseDouble(value);
        if (!Double.isFinite(number)) {
            throw new InvalidMeasurementException("invalid number");
        }
        return new Reading(sensorId, number);
    }
}
