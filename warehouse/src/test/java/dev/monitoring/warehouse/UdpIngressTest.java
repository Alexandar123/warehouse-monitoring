package dev.monitoring.warehouse;

import dev.monitoring.common.Measurement;
import dev.monitoring.common.MeasurementJson;
import dev.monitoring.common.SensorType;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.monitoring.common.SensorType.HUMIDITY;
import static dev.monitoring.common.SensorType.TEMPERATURE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpIngressTest {

    private final Scheduler sendScheduler = Schedulers.newSingle("kafka-send");
    private final List<UdpIngress> ingresses = new ArrayList<>();
    private final AtomicBoolean fatalError = new AtomicBoolean();
    private final DatagramSocket client = newClient();
    private MockProducer<String, byte[]> producer = new TestProducer();

    @AfterEach
    void tearDown() {
        ingresses.forEach(ingress -> ingress.dispose(Duration.ofSeconds(5)));
        sendScheduler.dispose();
        client.close();
        producer.close();
        assertFalse(fatalError.get(), "pipeline must never die");
    }

    @Test
    void validReadingIsPublishedWithKeyAndJson() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=t1; value=-4.5");

        awaitRecords(1);
        var sent = producer.history().getFirst();
        assertEquals("measurements", sent.topic());
        assertEquals("w1:t1", sent.key());
        var measurement = decode(sent);
        assertEquals(new Measurement("w1", "t1", TEMPERATURE, -4.5, measurement.receivedAt()), measurement);
    }

    @Test
    void sensorTypeComesFromThePortNotFromTheSensorId() {
        var temperature = start(TEMPERATURE);
        var humidity = start(HUMIDITY);
        send(temperature, "sensor_id=h9; value=20");
        awaitRecords(1);
        send(humidity, "sensor_id=t9; value=20");
        awaitRecords(2);

        assertEquals(List.of(TEMPERATURE, HUMIDITY), producer.history().stream().map(r -> decode(r).type()).toList());
    }

    @Test
    void malformedDatagramDoesNotStopProcessing() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=t1; value=1");
        send(ingress, "sensor_id=t1; value=NaN");
        send(ingress, "sensor_id=a; value=2\nsensor_id=b; value=3\n");

        awaitRecords(3);
        assertEquals(List.of(1.0, 2.0, 3.0), values());
        assertEquals(1, ingress.rejected());
    }

    @Test
    void failedSendIsIsolatedWhetherItFailsInTheCallbackOrThrows() {
        var calls = new AtomicInteger();
        producer = new TestProducer() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r, Callback callback) {
                int call = calls.incrementAndGet();
                if (call == 2) {
                    callback.onCompletion(null, new KafkaException("simulated broker error"));
                    return null;
                }
                if (call == 4) {
                    throw new KafkaException("simulated full buffer");
                }
                return super.send(r, callback);
            }
        };
        var ingress = start(TEMPERATURE);
        for (int i = 1; i <= 5; i++) {
            send(ingress, "sensor_id=t1; value=" + i);
        }

        awaitRecords(3);
        assertEquals(List.of(1.0, 3.0, 5.0), values());
        await().until(() -> ingress.failed() == 2);
    }

    @Test
    void producerSendRunsOnKafkaSendThreadNeverOnEventLoop() {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        producer = new TestProducer() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r, Callback callback) {
                threads.add(Thread.currentThread().getName());
                return super.send(r, callback);
            }
        };
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=t1; value=1");
        send(ingress, "sensor_id=t1; value=2");

        awaitRecords(2);
        assertEquals(1, threads.size(), threads.toString());
        assertTrue(threads.iterator().next().startsWith("kafka-send"), threads.toString());
    }

    @Test
    void overloadIsDroppedAndCountedInsteadOfBuffered() {
        var release = new CountDownLatch(1);
        producer = new TestProducer() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r, Callback callback) {
                try {
                    release.await(); // simulates send blocked on a full buffer
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.send(r, callback);
            }
        };
        var ingress = start(TEMPERATURE);
        // 40 lines per datagram: enough readings to overflow the publishOn queue even if the kernel drops datagrams
        String batch = "sensor_id=t1; value=999\n".repeat(40);
        for (int i = 0; i < 30; i++) {
            send(ingress, batch);
        }

        await().until(() -> ingress.dropped() > 0);
        release.countDown();
        await().untilAsserted(() -> assertEquals(producer.history().size(), ingress.accepted()));
        assertTrue(ingress.accepted() + ingress.dropped() <= 30 * 40);
    }

    @Test
    void datagramAboveSizeLimitIsRejectedWithoutParsing() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=big; value=1" + " ".repeat(UdpIngress.MAX_DATAGRAM_BYTES));
        send(ingress, "sensor_id=marker; value=1");

        awaitRecords(1);
        assertEquals("w1:marker", producer.history().getFirst().key());
        assertEquals(1, ingress.rejected());
    }

    private UdpIngress start(SensorType type) {
        var ingress = new UdpIngress("127.0.0.1", 0, type, "w1", new MeasurementParser(),
                new KafkaMeasurementPublisher(producer, "measurements"), sendScheduler, () -> fatalError.set(true));
        ingresses.add(ingress);
        return ingress;
    }

    private void send(UdpIngress ingress, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        try {
            client.send(new DatagramPacket(bytes, bytes.length, InetAddress.getLoopbackAddress(), ingress.port()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void awaitRecords(int count) {
        await().atMost(Duration.ofSeconds(10)).until(() -> producer.history().size() >= count);
        assertEquals(count, producer.history().size());
    }

    private List<Double> values() {
        return producer.history().stream().map(r -> decode(r).value()).toList();
    }

    private static Measurement decode(ProducerRecord<String, byte[]> data) {
        return MeasurementJson.fromBytes(data.value());
    }

    private static DatagramSocket newClient() {
        try {
            return new DatagramSocket();
        } catch (SocketException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class TestProducer extends MockProducer<String, byte[]> {
        TestProducer() {
            super(true, null, new StringSerializer(), new ByteArraySerializer());
        }
    }
}
