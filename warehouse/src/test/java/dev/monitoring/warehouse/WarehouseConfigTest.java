package dev.monitoring.warehouse;

import dev.monitoring.common.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WarehouseConfigTest {

    @Test
    void onlyWarehouseIdIsRequiredEverythingElseHasDefaults() {
        assertEquals(new WarehouseConfig("w1", 3344, 3355, "0.0.0.0", "localhost:9092", "measurements"),
                WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w1")));
    }

    @Test
    void readsEveryVariable() {
        var config = WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w-2", "TEMPERATURE_PORT", "4000",
                "HUMIDITY_PORT", "4001", "BIND_HOST", "127.0.0.1", "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092",
                "KAFKA_TOPIC", "t"));
        assertEquals(new WarehouseConfig("w-2", 4000, 4001, "127.0.0.1", "kafka:29092", "t"), config);
    }

    @Test
    void missingWarehouseIdIsNamed() {
        var e = assertThrows(ConfigException.class, () -> WarehouseConfig.fromEnv(Map.of()));
        assertEquals(java.util.List.of("WAREHOUSE_ID is required"), e.errors());
    }

    @ParameterizedTest
    @ValueSource(strings = {"w 1", "", "w1\n", "w.1"})
    void invalidWarehouseIdIsRejected(String id) {
        assertThrows(ConfigException.class, () -> WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", id)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "65536", "abc", ""})
    void invalidPortIsRejectedWithVariableAndValue(String port) {
        var e = assertThrows(ConfigException.class,
                () -> WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w1", "TEMPERATURE_PORT", port)));
        assertEquals(1, e.errors().size());
        assertEquals(true, e.errors().get(0).startsWith("TEMPERATURE_PORT='" + port + "'"), e.errors().toString());
    }

    @Test
    void boundaryPortsAreAccepted() {
        var config = WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w1", "TEMPERATURE_PORT", "0",
                "HUMIDITY_PORT", "65535"));
        assertEquals(0, config.temperaturePort());
        assertEquals(65535, config.humidityPort());
    }

    @Test
    void equalPortsAreRejectedUnlessBothAreZero() {
        assertThrows(ConfigException.class, () -> WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w1",
                "TEMPERATURE_PORT", "4000", "HUMIDITY_PORT", "4000")));
        var config = WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w1", "TEMPERATURE_PORT", "0",
                "HUMIDITY_PORT", "0"));
        assertEquals(0, config.humidityPort());
    }

    @Test
    void reportsAllErrorsTogether() {
        var e = assertThrows(ConfigException.class, () -> WarehouseConfig.fromEnv(Map.of(
                "TEMPERATURE_PORT", "abc", "KAFKA_TOPIC", " ")));
        assertEquals(3, e.errors().size(), e.errors().toString());
    }
}
