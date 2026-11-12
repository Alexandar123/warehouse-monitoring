package dev.monitoring.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MeasurementJsonTest {

    private static final Measurement SAMPLE =
            new Measurement("w1", "t1", SensorType.TEMPERATURE, -36.5, 1_794_000_000_123L);

    @Test
    void roundTripKeepsEveryField() {
        assertEquals(SAMPLE, MeasurementJson.fromBytes(MeasurementJson.toBytes(SAMPLE)));
    }

    @Test
    void encodedJsonHasExactlyTheContractFields() throws Exception {
        var node = new ObjectMapper().readTree(MeasurementJson.toBytes(SAMPLE));
        var names = new java.util.HashSet<String>();
        node.fieldNames().forEachRemaining(names::add);
        assertEquals(Set.of("warehouseId", "sensorId", "type", "value", "receivedAt"), names);
        assertEquals("TEMPERATURE", node.get("type").textValue());
    }

    @Test
    void keyIsWarehouseAndSensor() {
        assertEquals("w1:t1", SAMPLE.key());
    }

    @Test
    void unknownFieldIsIgnored() {
        var m = decode("{\"warehouseId\":\"w1\",\"sensorId\":\"h1\",\"type\":\"HUMIDITY\",\"value\":51,"
                + "\"receivedAt\":1,\"firmware\":\"2.0\"}");
        assertEquals(new Measurement("w1", "h1", SensorType.HUMIDITY, 51, 1), m);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // missing field
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"receivedAt\":1}",
            "{\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":1,\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":1}",
            // null value
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":null,\"receivedAt\":1}",
            // unknown type
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"PRESSURE\",\"value\":1,\"receivedAt\":1}",
            // NaN and overflow to infinity
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":NaN,\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":1e999,\"receivedAt\":1}",
            // value as string, id with a newline, fractional timestamp
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":\"1\",\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t\\n1\",\"type\":\"TEMPERATURE\",\"value\":1,\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":1,\"receivedAt\":1.5}",
            // not an object
            "[]", "42", "null", "{", "garbage"
    })
    void invalidJsonIsRejected(String json) {
        assertThrows(MeasurementDecodingException.class, () -> decode(json));
    }

    @Test
    void emptyNullAndRandomBytesAreRejectedWithoutCrashing() {
        assertThrows(MeasurementDecodingException.class, () -> MeasurementJson.fromBytes(new byte[0]));
        assertThrows(MeasurementDecodingException.class, () -> MeasurementJson.fromBytes(null));
        var random = new Random(42);
        for (int i = 0; i < 200; i++) {
            byte[] bytes = new byte[1 + random.nextInt(64)];
            random.nextBytes(bytes);
            assertThrows(MeasurementDecodingException.class, () -> MeasurementJson.fromBytes(bytes));
        }
    }

    private static Measurement decode(String json) {
        return MeasurementJson.fromBytes(json.getBytes(StandardCharsets.UTF_8));
    }
}
