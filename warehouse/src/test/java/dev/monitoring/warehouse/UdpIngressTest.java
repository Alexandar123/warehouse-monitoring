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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpIngressTest {

    private final Scheduler sendScheduler = Schedulers.newSingle("kafka-send");
    private final List<UdpIngress> ingresses = new ArrayList<>();
    private final AtomicBoolean fatalError = new AtomicBoolean();
    private final DatagramSocket client = newClient();
    private MockProducer<String, byte[]> producer = new TestProducer();

    @AfterEach
    void tearDown() {
        ingresses.forEach(i -> i.dispose(Duration.ofSeconds(5)));
        sendScheduler.dispose();
        client.close();
        producer.close();
        assertFalse(fatalError.get(), "pipeline must never die");
    }

    @Test
    void validReadingsArePublishedWithKeyAndJson() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=t1; value=30");
        send(ingress, "sensor_id=t2; value=-4.5\n");
        send(ingress, "value=36; sensor_id=t3");

        awaitRecords(3);
        assertEquals(List.of("w1:t1", "w1:t2", "w1:t3"), producer.history().stream()
                .map(ProducerRecord::key).toList());

        Measurement second = decode(producer.history().get(1));

        assertEquals("w1", second.warehouseId());
        assertEquals("t2", second.sensorId());
        assertEquals(TEMPERATURE, second.type());
        assertEquals(-4.5, second.value());
        assertTrue(second.receivedAt() > 0);
        assertEquals("measurements", producer.history().get(0).topic());

        await().until(() -> ingress.accepted() == 3);
    }

    @Test
    void humidityPortTagsReadingsAsHumidity() {
        var ingress = start(HUMIDITY);
        send(ingress, "sensor_id=h1; value=51");
        awaitRecords(1);
        assertEquals(HUMIDITY, decode(producer.history().getFirst()).type());
    }

    @Test
    void sensorTypeComesFromThePortNotFromTheSensorId() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=h9; value=20");
        awaitRecords(1);
        assertEquals(TEMPERATURE, decode(producer.history().get(0)).type());
    }

    @Test
    void malformedDatagramDoesNotStopProcessing() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=t1; value=1");
        send(ingress, "sensor_id=t1; value=NaN");
        send(ingress, "sensor_id=t1; value=3");

        awaitRecords(2);
        assertEquals(List.of(1.0, 3.0), values());
        assertEquals(1, ingress.rejected());
    }

    @Test
    void garbageOnOnePortDoesNotAffectTheOther() {
        var temperature = start(TEMPERATURE);
        var humidity = start(HUMIDITY);
        for (int i = 0; i < 20; i++) {
            send(temperature, "\u0000garbage\u0007 " + i);
        }
        send(humidity, "sensor_id=h1; value=55");

        awaitRecords(1);
        assertEquals(HUMIDITY, decode(producer.history().get(0)).type());
        await().until(() -> temperature.rejected() == 20);
    }

    @Test
    void severalLinesInOneDatagramBecomeSeveralReadings() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=a; value=1\r\nsensor_id=b; value=2\n\n");
        awaitRecords(2);
        assertEquals(List.of("w1:a", "w1:b"), producer.history().stream().map(ProducerRecord::key).toList());
    }

    @Test
    void blankDatagramsAreSkippedSilently() {
        var ingress = start(TEMPERATURE);
        send(ingress, "");
        send(ingress, "   ");
        send(ingress, "\n");
        send(ingress, "sensor_id=marker; value=1");

        awaitRecords(1);
        assertEquals(0, ingress.rejected());
    }

    @Test
    void sendFailingThroughTheCallbackIsIsolated() {
        var calls = new AtomicInteger();
        producer = new TestProducer() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r, Callback callback) {
                if (calls.incrementAndGet() == 2) {
                    callback.onCompletion(null, new KafkaException("simulated broker error"));
                    return null;
                }
                return super.send(r, callback);
            }
        };
        var ingress = start(TEMPERATURE);
        sendThree(ingress);

        awaitRecords(2);
        assertEquals(List.of(1.0, 3.0), values());
        await().until(() -> ingress.failed() == 1 && ingress.accepted() == 2);
    }

    @Test
    void sendThrowingSynchronouslyIsIsolated() {
        var calls = new AtomicInteger();
        producer = new TestProducer() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> r, Callback callback) {
                if (calls.incrementAndGet() == 2) {
                    throw new KafkaException("simulated full buffer");
                }
                return super.send(r, callback);
            }
        };
        var ingress = start(TEMPERATURE);
        sendThree(ingress);

        awaitRecords(2);
        assertEquals(List.of(1.0, 3.0), values());
        await().until(() -> ingress.failed() == 1);
    }

    @Test
    void readingsOfOneSensorKeepTheirOrder() {
        var ingress = start(TEMPERATURE);
        for (int i = 1; i <= 5; i++) {
            send(ingress, "sensor_id=t1; value=" + i);
        }
        awaitRecords(5);
        assertEquals(List.of(1.0, 2.0, 3.0, 4.0, 5.0), values());
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
        sendThree(ingress);

        awaitRecords(3);
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
        int datagrams = 30;
        int total = datagrams * 40;
        for (int i = 0; i < datagrams; i++) {
            send(ingress, batch);
        }

        await().until(() -> ingress.dropped() > 0);
        release.countDown();
        await().until(() -> ingress.accepted() > 0);
        await().untilAsserted(() -> assertEquals(producer.history().size(), ingress.accepted()));
        assertTrue(ingress.accepted() + ingress.rejected() + ingress.dropped() + ingress.failed() <= total);
    }

    @Test
    void datagramAboveSizeLimitIsRejectedWithoutParsing() {
        var ingress = start(TEMPERATURE);
        send(ingress, "sensor_id=big; value=1" + " ".repeat(UdpIngress.MAX_DATAGRAM_BYTES));
        send(ingress, "sensor_id=marker; value=1");

        awaitRecords(1);
        assertEquals("w1:marker", producer.history().get(0).key());
        assertEquals(1, ingress.rejected());
    }

    @Test
    void disposedPortCanBeBoundAgain() {
        var first = start(TEMPERATURE);
        int port = first.port();
        first.dispose(Duration.ofSeconds(5));
        ingresses.remove(first);

        var second = start(TEMPERATURE, port);
        send(second, "sensor_id=t1; value=1");
        awaitRecords(1);
    }

    @Test
    void secondServerOnUsedPortFailsAtStartup() {
        var first = start(TEMPERATURE);
        assertThrows(RuntimeException.class, () -> start(HUMIDITY, first.port()));
    }

    @Test
    void escapeRemovesControlCharactersAndShortensPayload() {
        assertEquals("a\\u000ab\\u001b[31m", UdpIngress.escape("a\nb\u001b[31m"));
        assertEquals(103, UdpIngress.escape("x".repeat(500)).length());
    }

    private UdpIngress start(SensorType type) {
        return start(type, 0);
    }

    private UdpIngress start(SensorType type, int port) {
        var ingress = new UdpIngress("127.0.0.1", port, type, "w1", new MeasurementParser(),
                new KafkaMeasurementPublisher(producer, "measurements"), sendScheduler, () -> fatalError.set(true));
        ingresses.add(ingress);
        return ingress;
    }

    private void sendThree(UdpIngress ingress) {
        for (int i = 1; i <= 3; i++) {
            send(ingress, "sensor_id=t1; value=" + i);
        }
    }

    private void send(UdpIngress ingress, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        try {
            client.send(new DatagramPacket(bytes, bytes.length, InetAddress.getLoopbackAddress(), ingress.port()));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
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
        } catch (java.net.SocketException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class TestProducer extends MockProducer<String, byte[]> {
        TestProducer() {
            super(true, null, new StringSerializer(), new ByteArraySerializer());
        }
    }
}
