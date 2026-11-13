package dev.monitoring.central;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.monitoring.warehouse.WarehouseApplication;
import dev.monitoring.warehouse.WarehouseConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class EndToEndTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    private final Logger alarmLogger = (Logger) LoggerFactory.getLogger(AlarmLogger.LOGGER_NAME);
    private final ListAppender<ILoggingEvent> alarms = new ListAppender<>();
    private final List<AutoCloseable> running = new ArrayList<>();
    private final String topic = "measurements-" + UUID.randomUUID();
    private DatagramSocket client;

    @BeforeEach
    void setUp() throws Exception {
        alarms.start();
        alarmLogger.addAppender(alarms);
        client = new DatagramSocket();
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get();
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable service : running.reversed()) {
            service.close();
        }
        client.close();
        alarmLogger.detachAppender(alarms);
    }

    @Test
    void readingsAboveThresholdRaiseTemperatureAndHumidityAlarms() {
        startCentral();
        var warehouse = startWarehouse("w1");

        send(warehouse.temperaturePort(), "sensor_id=t1; value=36");
        send(warehouse.humidityPort(), "sensor_id=h1; value=51");

        awaitAlarm("ALARM temperature above threshold: warehouse=w1 sensor=t1 value=36.0 threshold=35.0");
        awaitAlarm("ALARM humidity above threshold: warehouse=w1 sensor=h1 value=51.0 threshold=50.0");
    }

    @Test
    void alarmsFromTwoWarehousesAreDistinguished() {
        startCentral();
        var w1 = startWarehouse("w1");
        var w2 = startWarehouse("w2");

        send(w1.temperaturePort(), "sensor_id=t1; value=36");
        send(w2.temperaturePort(), "sensor_id=t1; value=36");

        awaitAlarm("warehouse=w1 sensor=t1");
        awaitAlarm("warehouse=w2 sensor=t1");
    }

    @Test
    void readingsSentWhileCentralIsDownRaiseAlarmsOnceItStarts() {
        var warehouse = startWarehouse("w1");
        send(warehouse.temperaturePort(), "sensor_id=t1; value=40");
        send(warehouse.humidityPort(), "sensor_id=h1; value=60");
        await().atMost(Duration.ofSeconds(15)).until(() -> warehouse.accepted() == 2);
        assertTrue(messages().isEmpty());

        startCentral();

        awaitAlarm("sensor=t1 value=40.0");
        awaitAlarm("sensor=h1 value=60.0");
    }

    private WarehouseApplication startWarehouse(String warehouseId) {
        var config = new WarehouseConfig(warehouseId, 0, 0, "127.0.0.1", KAFKA.getBootstrapServers(), topic);
        var warehouse = WarehouseApplication.start(config, Map.of(), () -> {
            throw new AssertionError("warehouse pipeline died");
        });
        running.add(warehouse);
        return warehouse;
    }

    private void startCentral() {
        var config = new CentralConfig(KAFKA.getBootstrapServers(), topic, "group-" + UUID.randomUUID(),
                new Thresholds(35, 50));
        running.add(CentralApplication.start(config, () -> {
            throw new AssertionError("consumer loop died");
        }));
    }

    private void send(int port, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        try {
            client.send(new DatagramPacket(bytes, bytes.length, InetAddress.getLoopbackAddress(), port));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private void awaitAlarm(String fragment) {
        await().atMost(Duration.ofSeconds(30)).until(() -> messages().stream().anyMatch(m -> m.contains(fragment)));
    }

    private List<String> messages() {
        return alarms.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }
}
