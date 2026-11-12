package dev.monitoring.warehouse;

/** A line that does not follow the sensor grammar. Caught per line inside the ingress pipeline. */
public class InvalidMeasurementException extends RuntimeException {

    public InvalidMeasurementException(String reason) {
        super(reason, null, false, false); // no stack trace: this is expected, frequent input
    }
}
