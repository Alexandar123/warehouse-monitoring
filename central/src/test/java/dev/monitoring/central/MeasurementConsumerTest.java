package dev.monitoring.central;

import dev.monitoring.common.Measurement;
import dev.monitoring.common.MeasurementJson;
import dev.monitoring.common.SensorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementConsumerTest {

    private static final String TOPIC = "measurements";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private final AtomicReference<String> closedOnThread = new AtomicReference<>();
    private final MockConsumer<String, byte[]> mock = new MockConsumer<>("earliest") {
        @Override
        public synchronized void close() {
            closedOnThread.set(Thread.currentThread().getName());
            super.close();
        }
    };
    private final AtomicBoolean fatalError = new AtomicBoolean();
    private final MeasurementConsumer consumer = new MeasurementConsumer(mock, TOPIC,
            new ThresholdEvaluator(new Thresholds(35, 50)), new AlarmLogger(), () -> fatalError.set(true));

    @AfterEach
    void tearDown() {
        consumer.stop(Duration.ofSeconds(5));
        assertFalse(fatalError.get());
    }

    @Test
    void garbageRecordIsSkippedAndPositionMovesPastIt() {
        var position = new AtomicLong(-1);
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(PARTITION));
            mock.updateBeginningOffsets(Map.of(PARTITION, 0L));
            mock.addRecord(record(0, json("t1", 36)));
            mock.addRecord(record(1, "not json at all".getBytes(StandardCharsets.UTF_8)));
            mock.addRecord(record(2, json("t2", 20)));
        });
        mock.schedulePollTask(() -> position.set(mock.position(PARTITION)));
        consumer.start();

        await().until(() -> position.get() == 3);
        assertEquals(2, consumer.processed());
        assertEquals(1, consumer.rejected());
        assertEquals(1, consumer.alarms());
    }

    @Test
    void validJsonWithInvalidContentIsSkipped() {
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(PARTITION));
            mock.updateBeginningOffsets(Map.of(PARTITION, 0L));
            mock.addRecord(record(0, ("{\"warehouseId\":\"w1\",\"sensorId\":\"t1\",\"type\":\"PRESSURE\","
                    + "\"value\":99,\"receivedAt\":1}").getBytes(StandardCharsets.UTF_8)));
            mock.addRecord(record(1, null));
            mock.addRecord(record(2, json("t1", 99)));
        });
        consumer.start();

        await().until(() -> consumer.processed() == 1);
        assertEquals(2, consumer.rejected());
        assertEquals(1, consumer.alarms());
    }

    @Test
    void wakeupEndsTheLoopAndClosesConsumerOnPollThread() {
        consumer.start();
        await().until(() -> mock.subscription().contains(TOPIC));

        consumer.stop(Duration.ofSeconds(5));

        assertTrue(mock.closed());
        assertTrue(closedOnThread.get().startsWith("kafka-poll"), closedOnThread.get());
    }

    private static ConsumerRecord<String, byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "w1:t1", value);
    }

    private static byte[] json(String sensorId, double value) {
        return MeasurementJson.toBytes(new Measurement("w1", sensorId, SensorType.TEMPERATURE, value, 1));
    }
}
