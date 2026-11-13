package dev.monitoring.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MeasurementJsonTest {

    @Test
    void roundTripKeepsEveryField() {
        var measurement = new Measurement("w1", "t1", SensorType.TEMPERATURE, -36.5, 1_794_000_000_123L);
        assertEquals(measurement, MeasurementJson.fromBytes(MeasurementJson.toBytes(measurement)));
    }

    @Test
    void unknownFieldIsIgnored() {
        var measurement = decode("{\"warehouseId\":\"w1\",\"sensorId\":\"h1\",\"type\":\"HUMIDITY\",\"value\":51,"
                + "\"receivedAt\":1,\"firmware\":\"2.0\"}");
        assertEquals(new Measurement("w1", "h1", SensorType.HUMIDITY, 51, 1), measurement);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":null,\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"PRESSURE\",\"value\":1,\"receivedAt\":1}",
            "{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"TEMPERATURE\",\"value\":NaN,\"receivedAt\":1}",
            "garbage",
            "",
    })
    void invalidRecordIsRejected(String json) {
        assertThrows(MeasurementDecodingException.class, () -> decode(json));
    }

    private static Measurement decode(String json) {
        return MeasurementJson.fromBytes(json.getBytes(StandardCharsets.UTF_8));
    }
}
