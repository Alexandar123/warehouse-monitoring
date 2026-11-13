package dev.monitoring.warehouse;

public class InvalidMeasurementException extends RuntimeException {

    public InvalidMeasurementException(String reason) {
        super(reason, null, false, false);
    }
}
