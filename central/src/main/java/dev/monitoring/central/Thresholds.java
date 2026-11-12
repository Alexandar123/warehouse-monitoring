package dev.monitoring.central;

import dev.monitoring.common.SensorType;

public record Thresholds(double temperature, double humidity) {

    public double forType(SensorType type) {
        return switch (type) {
            case TEMPERATURE -> temperature;
            case HUMIDITY -> humidity;
        };
    }
}
