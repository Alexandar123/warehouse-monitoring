package dev.monitoring.central;

import dev.monitoring.common.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record CentralConfig(String kafkaBootstrapServers, String kafkaTopic, String kafkaGroupId,
                            Thresholds thresholds) {
    
    public static CentralConfig fromEnv(Map<String, String> env) {
        List<String> errors = new ArrayList<>();

        String bootstrap = notBlank(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092", errors);
        String topic = notBlank(env, "KAFKA_TOPIC", "measurements", errors);
        String groupId = notBlank(env, "KAFKA_GROUP_ID", "central-monitoring", errors);
        double temperature = number(env, "TEMPERATURE_THRESHOLD", "35", errors);
        double humidity = number(env, "HUMIDITY_THRESHOLD", "50", errors);

        if (Double.isFinite(humidity) && (humidity < 0 || humidity > 100)) {
            errors.add("HUMIDITY_THRESHOLD='" + env.get("HUMIDITY_THRESHOLD") + "' must be between 0 and 100");
        }

        if (!errors.isEmpty()) {
            throw new ConfigException(errors);
        }
        return new CentralConfig(bootstrap, topic, groupId, new Thresholds(temperature, humidity));
    }

    private static String notBlank(Map<String, String> env, String name, String defaultValue, List<String> errors) {
        String value = env.getOrDefault(name, defaultValue);

        if (value.isBlank()) {
            errors.add(name + " must not be blank");
        }

        return value.strip();
    }

    private static double number(Map<String, String> env, String name, String defaultValue, List<String> errors) {
        String value = env.getOrDefault(name, defaultValue);
        try {
            double number = Double.parseDouble(value.strip());
            if (Double.isFinite(number)) {
                return number;
            }
            errors.add(name + "='" + value + "' is not a finite number");
        } catch (NumberFormatException e) {
            errors.add(name + "='" + value + "' is not a number");
        }
        return Double.NaN;
    }
}
