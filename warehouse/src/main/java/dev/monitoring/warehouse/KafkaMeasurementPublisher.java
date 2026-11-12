package dev.monitoring.warehouse;

import dev.monitoring.common.Measurement;
import dev.monitoring.common.MeasurementJson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Mono;

/**
 * Bridges the asynchronous producer callback into a Mono. No Reactor retry or timeout on top:
 * the producer already retries idempotently, and cancelling the Mono would not cancel the send.
 */
public final class KafkaMeasurementPublisher {

    private final Producer<String, byte[]> producer;
    private final String topic;

    public KafkaMeasurementPublisher(Producer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    /** A synchronous exception from send (full buffer, serialisation) also ends up as an error of the Mono. */
    public Mono<RecordMetadata> publish(Measurement measurement) {
        return Mono.create(sink -> {
            var record = new ProducerRecord<>(topic, measurement.key(), MeasurementJson.toBytes(measurement));
            producer.send(record, (metadata, error) -> {
                if (error != null) {
                    sink.error(error);
                } else {
                    sink.success(metadata);
                }
            });
        });
    }
}
