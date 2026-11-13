package dev.monitoring.warehouse;

import dev.monitoring.common.ConfigException;
import dev.monitoring.common.Measurement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record WarehouseConfig(String warehouseId, int temperaturePort, int humidityPort, String bindHost,
                              String kafkaBootstrapServers, String kafkaTopic) {

    public static WarehouseConfig fromEnv(Map<String, String> env) {
        List<String> errors = new ArrayList<>();
        String warehouseId = env.get("WAREHOUSE_ID");

        if (warehouseId == null) {
            errors.add("WAREHOUSE_ID is required");
        } else if (!Measurement.ID_PATTERN.matcher(warehouseId).matches()) {
            errors.add("WAREHOUSE_ID='" + warehouseId + "' must be 1-64 characters of A-Z, a-z, 0-9, '_' or '-'");
        }

        int temperaturePort = port(env, "TEMPERATURE_PORT", "3344", errors);
        int humidityPort = port(env, "HUMIDITY_PORT", "3355", errors);

        if (temperaturePort == humidityPort && temperaturePort > 0) {
            errors.add("HUMIDITY_PORT='" + humidityPort + "' must differ from TEMPERATURE_PORT");
        }

        String bindHost = notBlank(env, "BIND_HOST", "0.0.0.0", errors);
        String bootstrap = notBlank(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092", errors);
        String topic = notBlank(env, "KAFKA_TOPIC", "measurements", errors);

        if (!errors.isEmpty()) {
            throw new ConfigException(errors);
        }

        return new WarehouseConfig(warehouseId, temperaturePort, humidityPort, bindHost, bootstrap, topic);
    }

    private static int port(Map<String, String> env, String name, String defaultValue, List<String> errors) {
        String value = env.getOrDefault(name, defaultValue);

        try {
            int port = Integer.parseInt(value.strip());
            if (port >= 0 && port <= 65535) {
                return port;
            }
            errors.add(name + "='" + value + "' must be between 0 and 65535");
        } catch (NumberFormatException e) {
            errors.add(name + "='" + value + "' is not an integer");
        }

        return -1;
    }

    private static String notBlank(Map<String, String> env, String name, String defaultValue, List<String> errors) {
        String value = env.getOrDefault(name, defaultValue);

        if (value.isBlank()) {
            errors.add(name + " must not be blank");
        }

        return value.strip();
    }
}
