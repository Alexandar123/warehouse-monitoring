package dev.monitoring.central;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.monitoring.common.Measurement;
import dev.monitoring.common.SensorType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlarmLoggerTest {

    private final Logger alarmLogger = (Logger) LoggerFactory.getLogger(AlarmLogger.LOGGER_NAME);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void attach() {
        appender.start();
        alarmLogger.addAppender(appender);
    }

    @AfterEach
    void detach() {
        alarmLogger.detachAppender(appender);
    }

    @Test
    void writesOneWarnLineWithAllDetails() {
        long receivedAt = Instant.parse("2026-11-12T14:03:21.123Z").toEpochMilli();
        new AlarmLogger().log(new Alarm(new Measurement("w1", "t1", SensorType.TEMPERATURE, 36.5, receivedAt), 35));

        assertEquals(1, appender.list.size());
        ILoggingEvent event = appender.list.get(0);
        assertEquals(Level.WARN, event.getLevel());
        assertEquals("ALARM temperature above threshold: warehouse=w1 sensor=t1 value=36.5 threshold=35.0 "
                + "receivedAt=2026-11-12T14:03:21.123Z", event.getFormattedMessage());
    }
}
