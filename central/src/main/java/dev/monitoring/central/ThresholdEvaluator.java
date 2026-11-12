package dev.monitoring.central;

import dev.monitoring.common.Measurement;

import java.util.Optional;

public final class ThresholdEvaluator {

    private final Thresholds thresholds;

    public ThresholdEvaluator(Thresholds thresholds) {
        this.thresholds = thresholds;
    }

    public Optional<Alarm> evaluate(Measurement measurement) {
        double threshold = thresholds.forType(measurement.type());
        return measurement.value() > threshold ? Optional.of(new Alarm(measurement, threshold)) : Optional.empty();
    }
}
