package dev.monitoring.warehouse;

import dev.monitoring.common.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.List;
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
    void invalidPortNamesTheVariableAndTheValue() {
        var exception = assertThrows(ConfigException.class,
                () -> WarehouseConfig.fromEnv(Map.of("WAREHOUSE_ID", "w1", "TEMPERATURE_PORT", "abc")));
        assertEquals(List.of("TEMPERATURE_PORT='abc' is not an integer"), exception.errors());
    }

    @Test
    void reportsAllErrorsTogether() {
        var exception = assertThrows(ConfigException.class, () -> WarehouseConfig.fromEnv(Map.of(
                "TEMPERATURE_PORT", "4000", "HUMIDITY_PORT", "4000", "KAFKA_TOPIC", " ")));
        assertEquals(List.of("WAREHOUSE_ID is required", "HUMIDITY_PORT='4000' must differ from TEMPERATURE_PORT",
                "KAFKA_TOPIC must not be blank"), exception.errors());
    }
}
