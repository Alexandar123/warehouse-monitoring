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
            "sensor_id=t1; value=30\n",
            "sensor_id=t1; value=30\r\n",
            "  sensor_id = t1 ;  value =  30  ",
            "\tsensor_id=t1;value=30",
            "value=30; sensor_id=t1",
            "sensor_id=t1; value=30;",
            "sensor_id=t1; value=30 ; ",
    })
    void acceptsValidLines(String line) {
        assertEquals(new Reading("t1", 30), parser.parse(line));
    }

    @ParameterizedTest
    @ValueSource(strings = {"30.5", "-5", "0", "-0", "-12.25", "00012"})
    void acceptsDecimalAndNegativeNumbers(String number) {
        assertEquals(Double.parseDouble(number), parser.parse("sensor_id=t1; value=" + number).value());
    }

    @Test
    void acceptsIdOf64CharactersAndAllowedSymbols() {
        String id = "a".repeat(62) + "_-";
        assertEquals(id, parser.parse("sensor_id=" + id + "; value=1").sensorId());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "sensor_id=t1 value=30",            // no ';'
            "sensor_id=t1; value=30; unit=C",   // extra key
            "sensor_id=t1; sensor_id=t2; value=30",
            "sensor_id=t1; value=30; value=31",
            "Sensor_id=t1; value=30",           // wrong key case
            "sensor_id=t1; VALUE=30",
            "value=30",
            "sensor_id=t1",
            "sensor_id=; value=30",
            "sensor_id=t1; value=",
            "sensor_id=a=b; value=30",
            "sensor_id=t1;; value=30",          // empty segment in the middle
            "sensor_id=t1; value=30;;",         // more than one trailing ';'
            "sensor_id t1; value=30",           // segment without '='
            "",
            "   ",
    })
    void rejectsMalformedLines(String line) {
        assertThrows(InvalidMeasurementException.class, () -> parser.parse(line));
    }

    @ParameterizedTest
    @ValueSource(strings = {"t 1", "t\u00001", "t\u001b[31m1", "t\u20281", "t\u00e41", "t.1"})
    void rejectsIdsWithSpacesOrControlCharacters(String id) {
        assertThrows(InvalidMeasurementException.class, () -> parser.parse("sensor_id=" + id + "; value=1"));
    }

    @Test
    void rejectsIdWithEmbeddedNewline() {
        assertThrows(InvalidMeasurementException.class, () -> parser.parse("sensor_id=t\n1; value=1"));
    }

    @Test
    void rejectsIdOf65Characters() {
        assertThrows(InvalidMeasurementException.class,
                () -> parser.parse("sensor_id=" + "a".repeat(65) + "; value=1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "NaN", "Infinity", "-Infinity", "30d", "30f", "0x1p5", "3e2", "30,5", "abc", "30.", ".5",
            "+5", "--5", "1 000", "123456789012345678901"
    })
    void rejectsNumbersOutsideTheGrammar(String number) {
        var exception = assertThrows(InvalidMeasurementException.class,
                () -> parser.parse("sensor_id=t1; value=" + number));
        assertEquals("invalid number", exception.getMessage());
    }

    @Test
    void rejectionCarriesAReadableReason() {
        var exception = assertThrows(InvalidMeasurementException.class, () -> parser.parse("sensor_id=t1"));
        assertEquals("missing key value", exception.getMessage());
    }
}
