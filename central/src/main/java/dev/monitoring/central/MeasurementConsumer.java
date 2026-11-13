package dev.monitoring.central;

import dev.monitoring.common.MeasurementDecodingException;
import dev.monitoring.common.MeasurementJson;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class MeasurementConsumer {

    private static final Logger log = LoggerFactory.getLogger(MeasurementConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final Consumer<String, byte[]> consumer;
    private final String topic;
    private final ThresholdEvaluator evaluator;
    private final AlarmLogger alarmLogger;
    private final Runnable onFatalError;
    private final Scheduler pollScheduler = Schedulers.newSingle("kafka-poll");
    private final CountDownLatch finished = new CountDownLatch(1);
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong alarms = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    public MeasurementConsumer(Consumer<String, byte[]> consumer, String topic, ThresholdEvaluator evaluator,
                               AlarmLogger alarmLogger, Runnable onFatalError) {
        this.consumer = consumer;
        this.topic = topic;
        this.evaluator = evaluator;
        this.alarmLogger = alarmLogger;
        this.onFatalError = onFatalError;
    }

    public void start() {
        Mono.fromRunnable(() -> consumer.subscribe(List.of(topic)))
                .thenMany(Mono.fromCallable(() -> consumer.poll(POLL_TIMEOUT)).repeat())
                // prefetch 1: never poll ahead of processing, or auto-commit could commit unprocessed offsets
                .flatMapIterable(records -> records, 1)
                .doOnNext(this::process)
                .onErrorResume(WakeupException.class, e -> Flux.empty())
                .doFinally(signal -> {
                    try {
                        consumer.close();
                    } finally {
                        finished.countDown();
                    }
                })
                .subscribeOn(pollScheduler)
                .subscribe(null, e -> {
                    log.error("Consumer loop failed", e);
                    onFatalError.run();
                });
    }

    private void process(ConsumerRecord<String, byte[]> consumerRecord) {
        try {
            var measurement = MeasurementJson.fromBytes(consumerRecord.value());
            processed.incrementAndGet();
            evaluator.evaluate(measurement).ifPresent(alarm -> {
                alarms.incrementAndGet();
                alarmLogger.log(alarm);
            });
        } catch (MeasurementDecodingException e) {
            rejected.incrementAndGet();
            log.warn("Skipping invalid record topic={} partition={} offset={}: {}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e.getMessage());
        }
    }

    public void stop(Duration timeout) {
        consumer.wakeup();
        try {
            if (!finished.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Consumer did not stop within {}", timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pollScheduler.dispose();
    }

    public void awaitTermination() throws InterruptedException {
        finished.await();
    }

    public long processed() {
        return processed.get();
    }

    public long alarms() {
        return alarms.get();
    }

    public long rejected() {
        return rejected.get();
    }
}
