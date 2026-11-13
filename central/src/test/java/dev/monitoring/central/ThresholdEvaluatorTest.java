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

    private final ThresholdEvaluator evaluator = new ThresholdEvaluator(new Thresholds(35, 50));

    @ParameterizedTest
    @CsvSource({
            "TEMPERATURE, 35, false",
            "TEMPERATURE, 35.01, true",
            "HUMIDITY, 50, false",
            "HUMIDITY, 50.1, true",
    })
    void alarmOnlyStrictlyAboveThreshold(SensorType type, double value, boolean alarm) {
        assertEquals(alarm, evaluator.evaluate(reading(type, value)).isPresent());
    }

    @Test
    void sameValueIsJudgedByItsOwnTypeThreshold() {
        assertEquals(35, evaluator.evaluate(reading(TEMPERATURE, 40)).orElseThrow().threshold());
        assertTrue(evaluator.evaluate(reading(HUMIDITY, 40)).isEmpty());
    }

    private static Measurement reading(SensorType type, double value) {
        return new Measurement("w1", "s1", type, value, 0);
    }
}
