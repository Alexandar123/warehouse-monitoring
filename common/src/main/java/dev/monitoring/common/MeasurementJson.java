package dev.monitoring.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;

public final class MeasurementJson {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private MeasurementJson() {
    }

    public static byte[] toBytes(Measurement measurement) {
        try {
            return MAPPER.writeValueAsBytes(measurement);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Measurement fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new MeasurementDecodingException("empty record value");
        }

        JsonNode node;

        try {
            node = MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new MeasurementDecodingException("not valid JSON");
        }
        if (node == null || !node.isObject()) {
            throw new MeasurementDecodingException("not a JSON object");
        }

        String warehouseId = id(node, "warehouseId");
        String sensorId = id(node, "sensorId");
        SensorType type = type(node);
        double value = value(node);
        long receivedAt = receivedAt(node);

        return new Measurement(warehouseId, sensorId, type, value, receivedAt);
    }

    private static String id(JsonNode node, String field) {
        JsonNode id = node.get(field);

        if (id == null || !id.isTextual()) {
            throw new MeasurementDecodingException("missing or non-text field " + field);
        }

        if (!Measurement.ID_PATTERN.matcher(id.textValue()).matches()) {
            throw new MeasurementDecodingException("invalid " + field);
        }

        return id.textValue();
    }

    private static SensorType type(JsonNode node) {
        JsonNode type = node.get("type");

        if (type == null || !type.isTextual()) {
            throw new MeasurementDecodingException("missing or non-text field type");
        }

        try {
            return SensorType.valueOf(type.textValue());
        } catch (IllegalArgumentException e) {
            throw new MeasurementDecodingException("unknown type");
        }
    }

    private static double value(JsonNode node) {
        JsonNode value = node.get("value");

        if (value == null || !value.isNumber() || !Double.isFinite(value.doubleValue())) {
            throw new MeasurementDecodingException("missing or non-finite field value");
        }

        return value.doubleValue();
    }

    private static long receivedAt(JsonNode node) {
        JsonNode receivedAt = node.get("receivedAt");

        if (receivedAt == null || !receivedAt.isIntegralNumber() || !receivedAt.canConvertToLong()) {
            throw new MeasurementDecodingException("missing or non-integer field receivedAt");
        }

        return receivedAt.longValue();
    }
}
