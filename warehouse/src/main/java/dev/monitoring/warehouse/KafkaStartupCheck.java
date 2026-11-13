package dev.monitoring.warehouse;

import org.apache.kafka.clients.producer.Producer;

public final class KafkaStartupCheck {

    private KafkaStartupCheck() {
    }

    public static void verify(Producer<?, ?> producer, String topic, String bootstrapServers) {
        try {
            if (producer.partitionsFor(topic).isEmpty()) {
                throw new IllegalStateException("topic '" + topic + "' has no partitions");
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Kafka at " + bootstrapServers + " is not reachable or topic '" + topic
                    + "' does not exist: " + e.getMessage(), e);
        }
    }
}
