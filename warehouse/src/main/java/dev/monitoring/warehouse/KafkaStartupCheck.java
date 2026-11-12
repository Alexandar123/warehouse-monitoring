package dev.monitoring.warehouse;

import org.apache.kafka.clients.producer.Producer;

/** Fail fast: verify Kafka and the topic are reachable before any UDP port is bound. */
public final class KafkaStartupCheck {

    private KafkaStartupCheck() {
    }

    /** Blocks for at most max.block.ms of the producer. */
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
