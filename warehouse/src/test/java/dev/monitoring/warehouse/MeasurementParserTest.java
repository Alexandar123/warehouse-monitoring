package dev.monitoring.warehouse;

import dev.monitoring.warehouse.MeasurementParser.Reading;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MeasurementParserTest {

    private final MeasurementParser parser = new MeasurementParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "sensor_id=t1; value=30",
            "sensor_id=t1; value=30\r\n",
            "  sensor_id = t1 ;value=30  ",
            "value=30; sensor_id=t1;",
    })
    void acceptsValidLines(String line) {
        assertEquals(new Reading("t1", 30), parser.parse(line));
    }

    @ParameterizedTest
    @ValueSource(strings = {"30.5", "-5", "0"})
    void acceptsDecimalAndNegativeNumbers(String number) {
        assertEquals(Double.parseDouble(number), parser.parse("sensor_id=t1; value=" + number).value());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "sensor_id=t1 value=30",
            "sensor_id=t1; value=30; unit=C",
            "sensor_id=t1; value=30; value=31",
            "Sensor_id=t1; value=30",
            "sensor_id=t1",
            "sensor_id=; value=30",
            "sensor_id=t 1; value=30",
            "sensor_id=t\n1; value=30",
            "",
    })
    void rejectsMalformedLines(String line) {
        assertThrows(InvalidMeasurementException.class, () -> parser.parse(line));
    }

    @ParameterizedTest
    @ValueSource(strings = {"NaN", "Infinity", "30f", "3e2", "0x1p5", "30,5", ".5", "123456789012345678901"})
    void rejectsNumbersThatDoubleParseDoubleWouldAccept(String number) {
        var exception = assertThrows(InvalidMeasurementException.class,
                () -> parser.parse("sensor_id=t1; value=" + number));
        assertEquals("invalid number", exception.getMessage());
    }

    @Test
    void sensorIdIsLimitedTo64Characters() {
        assertEquals(64, parser.parse("sensor_id=" + "a".repeat(64) + "; value=1").sensorId().length());
        assertThrows(InvalidMeasurementException.class,
                () -> parser.parse("sensor_id=" + "a".repeat(65) + "; value=1"));
    }
}
