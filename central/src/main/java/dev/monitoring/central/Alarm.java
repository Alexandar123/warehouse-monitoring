package dev.monitoring.central;

import dev.monitoring.common.Measurement;

public record Alarm(Measurement measurement, double threshold) {
}
