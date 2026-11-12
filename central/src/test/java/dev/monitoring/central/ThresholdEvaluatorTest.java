package dev.monitoring.central;

import dev.monitoring.common.Measurement;
import dev.monitoring.common.SensorType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static dev.monitoring.common.SensorType.HUMIDITY;
import static dev.monitoring.common.SensorType.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThresholdEvaluatorTest {

    private final ThresholdEvaluator defaults = new ThresholdEvaluator(new Thresholds(35, 50));

    @ParameterizedTest
    @CsvSource({
            "TEMPERATURE, 34.9, false",
            "TEMPERATURE, 35, false",
            "TEMPERATURE, 35.0, false",
            "TEMPERATURE, 35.01, true",
            "HUMIDITY, 50, false",
            "HUMIDITY, 50.1, true",
            "TEMPERATURE, -40, false",
    })
    void alarmOnlyStrictlyAboveThreshold(SensorType type, double value, boolean alarm) {
        assertEquals(alarm, defaults.evaluate(reading(type, value)).isPresent());
    }

    @Test
    void sameValueIsJudgedByItsOwnTypeThreshold() {
        var alarm = defaults.evaluate(reading(TEMPERATURE, 40)).orElseThrow();
        assertEquals(35, alarm.threshold());
        assertTrue(defaults.evaluate(reading(HUMIDITY, 40)).isEmpty());
    }

    @Test
    void alarmsFollowConfiguredThresholds() {
        var custom = new ThresholdEvaluator(new Thresholds(20, 30));
        assertTrue(custom.evaluate(reading(TEMPERATURE, 21)).isPresent());
        assertTrue(custom.evaluate(reading(TEMPERATURE, 20)).isEmpty());
        assertTrue(custom.evaluate(reading(HUMIDITY, 31)).isPresent());
        assertTrue(custom.evaluate(reading(HUMIDITY, 30)).isEmpty());
    }

    private static Measurement reading(SensorType type, double value) {
        return new Measurement("w1", "s1", type, value, 0);
    }
}
