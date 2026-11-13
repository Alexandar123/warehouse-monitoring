package dev.monitoring.warehouse;

import dev.monitoring.common.Measurement;

import java.util.regex.Pattern;

public final class MeasurementParser {

    private static final Pattern NUMBER = Pattern.compile("-?[0-9]+(\\.[0-9]+)?");
    private static final int MAX_NUMBER_LENGTH = 20;
    private static final String SENSOR_ID_KEY = "sensor_id";
    private static final String VALUE_KEY = "value";

    public record Reading(String sensorId, double value) {
    }

    public Reading parse(String line) {
        String[] segments = line.strip().split(";", -1);
        int count = segments.length;

        if (count > 1 && segments[count - 1].isBlank()) {
            count--;
        }
        String sensorId = null;
        String value = null;

        for (int i = 0; i < count; i++) {
            String segment = segments[i].strip();

            if (segment.isEmpty()) {
                throw new InvalidMeasurementException("empty segment");
            }
            int equal = segment.indexOf('=');

            if (equal < 0) {
                throw new InvalidMeasurementException("missing '='");
            }

            String key = segment.substring(0, equal).strip();
            String raw = segment.substring(equal + 1).strip();

            switch (key) {
                case SENSOR_ID_KEY -> {
                    if (sensorId != null) {
                        throw new InvalidMeasurementException("duplicate key " + SENSOR_ID_KEY);
                    }
                    sensorId = raw;
                }
                case VALUE_KEY -> {
                    if (value != null) {
                        throw new InvalidMeasurementException("duplicate key " + VALUE_KEY);
                    }
                    value = raw;
                }
                default -> throw new InvalidMeasurementException("unknown key");
            }
        }

        validate(sensorId, value);

        double number = Double.parseDouble(value);

        if (!Double.isFinite(number)) {
            throw new InvalidMeasurementException("invalid number");
        }
        return new Reading(sensorId, number);
    }

    private static void validate(String sensorId, String value) {
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
    }
}
