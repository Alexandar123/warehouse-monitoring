package dev.monitoring.central;

import dev.monitoring.common.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CentralConfigTest {

    @Test
    void emptyEnvironmentUsesDefaults() {
        var config = CentralConfig.fromEnv(Map.of());
        assertEquals(new CentralConfig("localhost:9092", "measurements", "central-monitoring", new Thresholds(35, 50)),
                config);
    }

    @Test
    void readsEveryVariable() {
        var config = CentralConfig.fromEnv(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092", "KAFKA_TOPIC", "t", "KAFKA_GROUP_ID", "g",
                "TEMPERATURE_THRESHOLD", "-5.5", "HUMIDITY_THRESHOLD", "100"));
        assertEquals(new CentralConfig("kafka:29092", "t", "g", new Thresholds(-5.5, 100)), config);
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "NaN", "Infinity", ""})
    void rejectsNonFiniteTemperatureThreshold(String value) {
        var e = assertThrows(ConfigException.class, () -> CentralConfig.fromEnv(Map.of("TEMPERATURE_THRESHOLD", value)));
        assertTrue(e.errors().get(0).startsWith("TEMPERATURE_THRESHOLD='" + value + "'"), e.errors().toString());
    }

    @Test
    void namesTheVariableAndTheBadValue() {
        var e = assertThrows(ConfigException.class, () -> CentralConfig.fromEnv(Map.of("TEMPERATURE_THRESHOLD", "abc")));
        assertEquals("TEMPERATURE_THRESHOLD='abc' is not a number", e.errors().get(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "100.5"})
    void rejectsHumidityThresholdOutsidePercentRange(String value) {
        assertThrows(ConfigException.class, () -> CentralConfig.fromEnv(Map.of("HUMIDITY_THRESHOLD", value)));
    }

    @Test
    void reportsAllErrorsTogether() {
        var e = assertThrows(ConfigException.class, () -> CentralConfig.fromEnv(Map.of(
                "KAFKA_TOPIC", " ", "TEMPERATURE_THRESHOLD", "x", "HUMIDITY_THRESHOLD", "200")));
        assertEquals(3, e.errors().size(), e.errors().toString());
    }
}
