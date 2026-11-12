package dev.monitoring.warehouse;

import dev.monitoring.common.ConfigException;
import dev.monitoring.common.SensorType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Wires the warehouse service by hand. All System.exit calls live in main and nowhere else. */
public final class WarehouseApplication implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WarehouseApplication.class);
    private static final Duration UDP_DISPOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer;
    private final Scheduler sendScheduler;
    private final UdpIngress temperature;
    private final UdpIngress humidity;
    private final AtomicBoolean closed = new AtomicBoolean();

    private WarehouseApplication(Producer<String, byte[]> producer, Scheduler sendScheduler,
                                 UdpIngress temperature, UdpIngress humidity) {
        this.producer = producer;
        this.sendScheduler = sendScheduler;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    public static void main(String[] args) {
        WarehouseConfig config;
        try {
            config = WarehouseConfig.fromEnv(System.getenv());
        } catch (ConfigException e) {
            e.errors().forEach(error -> log.error("Invalid configuration: {}", error));
            System.exit(2);
            return;
        }
        WarehouseApplication app;
        try {
            // exit from a fresh thread: the shutdown hook needs the event loop that reports the fatal error
            app = start(config, Map.of(), () -> new Thread(() -> System.exit(1), "fatal-exit").start());
        } catch (RuntimeException e) {
            log.error("Startup failed: {}", e.getMessage());
            System.exit(1);
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(app::close, "shutdown"));
        app.awaitTermination();
    }

    /**
     * Creates the producer, checks Kafka, then binds both ports. Throws if any step fails; nothing is left running.
     *
     * @param producerOverrides extra producer properties (tests only)
     */
    public static WarehouseApplication start(WarehouseConfig config, Map<String, Object> producerOverrides,
                                             Runnable onFatalError) {
        Producer<String, byte[]> producer = new KafkaProducer<>(producerProperties(config, producerOverrides));
        try {
            KafkaStartupCheck.verify(producer, config.kafkaTopic(), config.kafkaBootstrapServers());
        } catch (RuntimeException e) {
            producer.close(Duration.ZERO);
            throw e;
        }
        Scheduler sendScheduler = Schedulers.newSingle("kafka-send");
        var publisher = new KafkaMeasurementPublisher(producer, config.kafkaTopic());
        var parser = new MeasurementParser();
        UdpIngress temperature = null;
        try {
            temperature = new UdpIngress(config.bindHost(), config.temperaturePort(), SensorType.TEMPERATURE,
                    config.warehouseId(), parser, publisher, sendScheduler, onFatalError);
            UdpIngress humidity = new UdpIngress(config.bindHost(), config.humidityPort(), SensorType.HUMIDITY,
                    config.warehouseId(), parser, publisher, sendScheduler, onFatalError);
            log.info("Warehouse {} started: temperature udp/{} humidity udp/{} kafka={} topic={}",
                    config.warehouseId(), temperature.port(), humidity.port(), config.kafkaBootstrapServers(),
                    config.kafkaTopic());
            return new WarehouseApplication(producer, sendScheduler, temperature, humidity);
        } catch (RuntimeException e) {
            if (temperature != null) {
                temperature.dispose(UDP_DISPOSE_TIMEOUT);
            }
            producer.close(Duration.ZERO);
            sendScheduler.dispose();
            throw new IllegalStateException("could not bind UDP port on " + config.bindHost() + ": " + e.getMessage(), e);
        }
    }

    private static Map<String, Object> producerProperties(WarehouseConfig config, Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.warehouseId());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.putAll(overrides);
        return props;
    }

    public int temperaturePort() {
        return temperature.port();
    }

    public int humidityPort() {
        return humidity.port();
    }

    public long accepted() {
        return temperature.accepted() + humidity.accepted();
    }

    /** Blocks until both UDP bindings are disposed. */
    public void awaitTermination() {
        Mono.when(temperature.onDispose(), humidity.onDispose()).block();
    }

    /** Ordered shutdown: stop UDP first, then flush and close the producer, so nothing received is lost. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Shutdown: stopping UDP ingress");
        temperature.dispose(UDP_DISPOSE_TIMEOUT);
        humidity.dispose(UDP_DISPOSE_TIMEOUT);
        log.info("Shutdown: flushing and closing Kafka producer");
        producer.flush();
        producer.close(PRODUCER_CLOSE_TIMEOUT);
        sendScheduler.dispose();
        log.info("Shutdown complete: accepted={} rejected={} dropped={} failed={}",
                accepted(), temperature.rejected() + humidity.rejected(),
                temperature.dropped() + humidity.dropped(), temperature.failed() + humidity.failed());
    }
}
