package dev.monitoring.central;

import dev.monitoring.common.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CentralConfigTest {

    @Test
    void emptyEnvironmentUsesDefaults() {
        assertEquals(new CentralConfig("localhost:9092", "measurements", "central-monitoring", new Thresholds(35, 50)),
                CentralConfig.fromEnv(Map.of()));
    }

    @Test
    void reportsAllErrorsTogether() {
        var exception = assertThrows(ConfigException.class, () -> CentralConfig.fromEnv(Map.of(
                "TEMPERATURE_THRESHOLD", "abc", "HUMIDITY_THRESHOLD", "200")));
        assertEquals(List.of("TEMPERATURE_THRESHOLD='abc' is not a number",
                "HUMIDITY_THRESHOLD='200' must be between 0 and 100"), exception.errors());
    }
}
