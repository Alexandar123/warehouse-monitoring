package dev.monitoring.warehouse;

import dev.monitoring.common.Measurement;
import dev.monitoring.common.MeasurementJson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Mono;

public final class KafkaMeasurementPublisher {

    private final Producer<String, byte[]> producer;
    private final String topic;

    public KafkaMeasurementPublisher(Producer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public Mono<RecordMetadata> publish(Measurement measurement) {
        return Mono.create(sink -> {
            var producerRecord = new ProducerRecord<>(topic, measurement.key(), MeasurementJson.toBytes(measurement));

            producer.send(producerRecord, (metadata, error) -> {
                if (error != null) {
                    sink.error(error);
                } else {
                    sink.success(metadata);
                }
            });
        });
    }
}
