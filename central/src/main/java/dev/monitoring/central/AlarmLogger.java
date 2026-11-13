package dev.monitoring.central;

import dev.monitoring.common.Measurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Locale;

public final class AlarmLogger {

    public static final String LOGGER_NAME = "ALARM";

    private static final Logger ALARM = LoggerFactory.getLogger(LOGGER_NAME);

    public void log(Alarm alarm) {
        Measurement measurement = alarm.measurement();
        // Ids were validated against Measurement.ID_PATTERN while decoding, so they are safe to log as-is.
        ALARM.warn("ALARM {} above threshold: warehouse={} sensor={} value={} threshold={} receivedAt={}",
                measurement.type().name().toLowerCase(Locale.ROOT), measurement.warehouseId(), measurement.sensorId(), measurement.value(),
                alarm.threshold(), Instant.ofEpochMilli(measurement.receivedAt()));
    }
}
