package dev.monitoring.warehouse;

import org.junit.jupiter.api.Test;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WarehouseStartupTest {

    @Test
    void startupFailsFastWhenKafkaIsUnreachableAndNoPortIsBound() throws Exception {
        int port;
        try (var probe = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
            port = probe.getLocalPort();
        }
        var config = new WarehouseConfig("w1", port, 0, "127.0.0.1", "localhost:1", "measurements");

        long started = System.nanoTime();
        var e = assertThrows(IllegalStateException.class, () -> WarehouseApplication.start(config, Map.of(), () -> {
        }));
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;

        assertTrue(e.getMessage().contains("localhost:1"), e.getMessage());
        assertTrue(elapsedMs < 5000 + 3000, "took " + elapsedMs + " ms"); // max.block.ms + margin
        new DatagramSocket(port, InetAddress.getLoopbackAddress()).close(); // the port was never bound
    }
}
