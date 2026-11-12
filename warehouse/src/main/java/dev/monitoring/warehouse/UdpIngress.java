package dev.monitoring.warehouse;

import dev.monitoring.common.Measurement;
import dev.monitoring.common.SensorType;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Receives datagrams on one UDP port, parses them and forwards every valid reading to Kafka.
 * The sensor type is decided by the port, never by the sensor id (assumption A4).
 */
public final class UdpIngress {

    static final int MAX_DATAGRAM_BYTES = 1024;
    private static final int MAX_IN_FLIGHT_SENDS = 256;
    private static final int MAX_LOGGED_PAYLOAD = 100;

    private static final Logger log = LoggerFactory.getLogger(UdpIngress.class);

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final CountDownLatch pipelineDone = new CountDownLatch(1);

    private final SensorType type;
    private final String warehouseId;
    private final MeasurementParser parser;
    private final KafkaMeasurementPublisher publisher;
    private final Connection connection;

    private record Datagram(String text, InetSocketAddress sender) {
    }

    /**
     * Binds the port immediately.
     *
     * @param onFatalError called if the pipeline itself dies; the application then shuts down
     */
    public UdpIngress(String host, int port, SensorType type, String warehouseId, MeasurementParser parser,
                      KafkaMeasurementPublisher publisher, Scheduler sendScheduler, Runnable onFatalError) {
        this.type = type;
        this.warehouseId = warehouseId;
        this.parser = parser;
        this.publisher = publisher;
        this.connection = UdpServer.create()
                .host(host)
                .port(port)
                .handle((in, out) -> in.receiveObject()
                        .cast(DatagramPacket.class)
                        // Runs on the event loop; the packet is released right after, so only text leaves this step.
                        .<Datagram>handle((packet, sink) -> {
                            Datagram datagram = toDatagram(packet);
                            if (datagram != null) {
                                sink.next(datagram);
                            }
                        })
                        .flatMapIterable(this::toMeasurements)
                        .onBackpressureDrop(m -> dropped.incrementAndGet())
                        .publishOn(sendScheduler)
                        .flatMap(this::publish, MAX_IN_FLIGHT_SENDS)
                        .doOnError(e -> {
                            log.error("{} ingress pipeline failed, shutting down", type, e);
                            onFatalError.run();
                        })
                        .doFinally(signal -> pipelineDone.countDown())
                        .then())
                .bindNow(Duration.ofSeconds(10));
    }

    private Datagram toDatagram(DatagramPacket packet) {
        int size = packet.content().readableBytes();
        if (size > MAX_DATAGRAM_BYTES) {
            rejected.incrementAndGet();
            log.warn("Dropped {} datagram of {} bytes from {}: above the {} byte limit",
                    type, size, packet.sender(), MAX_DATAGRAM_BYTES);
            return null;
        }
        return new Datagram(packet.content().toString(StandardCharsets.UTF_8), packet.sender());
    }

    private List<Measurement> toMeasurements(Datagram datagram) {
        List<Measurement> measurements = new ArrayList<>(1);
        datagram.text().lines().filter(line -> !line.isBlank()).forEach(line -> {
            try {
                var reading = parser.parse(line);
                measurements.add(new Measurement(warehouseId, reading.sensorId(), type, reading.value(),
                        System.currentTimeMillis()));
            } catch (InvalidMeasurementException e) {
                rejected.incrementAndGet();
                log.debug("Rejected {} line from {}: {} payload='{}'", type, datagram.sender(), e.getMessage(),
                        escape(line));
            }
        });
        return measurements;
    }

    private Mono<Void> publish(Measurement measurement) {
        return publisher.publish(measurement)
                .doOnSuccess(metadata -> accepted.incrementAndGet())
                .onErrorResume(e -> {
                    failed.incrementAndGet();
                    log.warn("Failed to publish reading of sensor {}: {} measurement={}",
                            measurement.sensorId(), e.toString(), measurement);
                    return Mono.empty();
                })
                .then();
    }

    /** Untrusted input: replace control characters and cut it short before it reaches a log line. */
    static String escape(String raw) {
        String cut = raw.length() > MAX_LOGGED_PAYLOAD ? raw.substring(0, MAX_LOGGED_PAYLOAD) + "..." : raw;
        StringBuilder sb = new StringBuilder(cut.length());
        cut.codePoints().forEach(cp -> {
            if (Character.isISOControl(cp) || Character.getType(cp) == Character.LINE_SEPARATOR
                    || Character.getType(cp) == Character.PARAGRAPH_SEPARATOR) {
                sb.append(String.format("\\u%04x", cp));
            } else {
                sb.appendCodePoint(cp);
            }
        });
        return sb.toString();
    }

    public int port() {
        return ((InetSocketAddress) connection.address()).getPort();
    }

    public Mono<Void> onDispose() {
        return connection.onDispose();
    }

    /** Stops receiving, then waits (bounded) until readings already received have been handed to the producer. */
    public void dispose(Duration timeout) {
        connection.disposeNow(timeout);
        try {
            if (!pipelineDone.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("{} ingress pipeline did not drain within {}", type, timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long accepted() {
        return accepted.get();
    }

    public long rejected() {
        return rejected.get();
    }

    public long dropped() {
        return dropped.get();
    }

    public long failed() {
        return failed.get();
    }
}
