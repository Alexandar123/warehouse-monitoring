package dev.monitoring.central;

import dev.monitoring.common.ConfigException;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CentralApplication implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CentralApplication.class);
    private static final Duration STARTUP_CHECK_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

    private final MeasurementConsumer consumer;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CentralApplication(MeasurementConsumer consumer) {
        this.consumer = consumer;
    }

    public static void main(String[] args) throws InterruptedException {
        CentralConfig config;
        try {
            config = CentralConfig.fromEnv(System.getenv());
        } catch (ConfigException e) {
            e.errors().forEach(error -> log.error("Invalid configuration: {}", error));
            System.exit(2);
            return;
        }
        CentralApplication app;
        try {
            app = start(config, () -> new Thread(() -> System.exit(1), "fatal-exit").start());
        } catch (RuntimeException e) {
            log.error("Startup failed: {}", e.getMessage());
            System.exit(1);
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(app::close, "shutdown"));
        app.consumer.awaitTermination();
    }

    public static CentralApplication start(CentralConfig config, Runnable onFatalError) {
        var kafkaConsumer = new KafkaConsumer<String, byte[]>(consumerProperties(config));
        try {
            if (kafkaConsumer.partitionsFor(config.kafkaTopic(), STARTUP_CHECK_TIMEOUT).isEmpty()) {
                throw new IllegalStateException("topic '" + config.kafkaTopic() + "' does not exist");
            }
        } catch (RuntimeException e) {
            kafkaConsumer.close(CloseOptions.timeout(Duration.ZERO));
            throw new IllegalStateException("Kafka at " + config.kafkaBootstrapServers()
                    + " is not reachable or topic '" + config.kafkaTopic() + "' does not exist: " + e.getMessage(), e);
        }
        var consumer = new MeasurementConsumer(kafkaConsumer, config.kafkaTopic(),
                new ThresholdEvaluator(config.thresholds()), new AlarmLogger(), onFatalError);
        consumer.start();
        log.info("Central started: thresholds temperature={} humidity={} topic={} group={} kafka={}",
                config.thresholds().temperature(), config.thresholds().humidity(), config.kafkaTopic(),
                config.kafkaGroupId(), config.kafkaBootstrapServers());
        return new CentralApplication(consumer);
    }

    private static Map<String, Object> consumerProperties(CentralConfig config) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.kafkaGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Raw bytes: JSON is decoded per record, so a bad record cannot make poll() fail forever.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Shutdown: stopping consumer");
        consumer.stop(STOP_TIMEOUT);
        log.info("Shutdown complete: processed={} alarms={} rejected={}",
                consumer.processed(), consumer.alarms(), consumer.rejected());
    }
}
