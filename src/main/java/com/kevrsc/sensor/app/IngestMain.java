package com.kevrsc.sensor.app;

import com.kevrsc.sensor.http.MethodRouter;
import com.kevrsc.sensor.http.ReadingsIngestHandler;
import com.kevrsc.sensor.http.ReadingsQueryHandler;
import com.kevrsc.sensor.kafka.KafkaClientConfig;
import com.kevrsc.sensor.storage.MeasurementHistoryReader;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.Executors;

public final class IngestMain {

    private static final Logger LOG = LoggerFactory.getLogger(IngestMain.class);

    private IngestMain() {
    }

    public static void main(String[] args) throws IOException {
        var host = env("INGEST_HOST", "0.0.0.0");
        var port = Integer.parseInt(env("INGEST_PORT", "8080"));
        var bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        var topic = env("SENSOR_TOPIC", "sensor-measurements");
        var dbUrl = env("DB_URL", "jdbc:postgresql://localhost:5432/sensors");
        var dbUser = env("DB_USER", "sensors");
        var dbPassword = env("DB_PASSWORD", "sensors");
        LOG.info("Starting sensor ingest: host={}, port={}, bootstrapServers={}, topic={}, dbUrl={}, zone={}",
                host, port, bootstrapServers, topic, dbUrl, ZoneId.systemDefault());

        var producer = new KafkaProducer<String, String>(KafkaClientConfig.ingestProducerProperties(bootstrapServers));
        var history = new MeasurementHistoryReader(dbUrl, dbUser, dbPassword, ZoneId.systemDefault());
        var server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/", new MethodRouter(Map.of(
                "POST", new ReadingsIngestHandler(producer, topic, Clock.systemDefaultZone(), Duration.ofSeconds(15)),
                "GET", new ReadingsQueryHandler(history))));
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received; stopping HTTP server");
            server.stop(5);
            producer.close(Duration.ofSeconds(10));
            LOG.info("sensor ingest stopped");
        }));

        server.start();
        var advertisedHost = System.getenv("INGEST_ADVERTISED_HOST");
        if (advertisedHost == null || advertisedHost.isBlank()) {
            advertisedHost = lanIp(env("LAN_PROBE_ADDRESS", "8.8.8.8")) + ":" + port;
        }
        LOG.info("ESP32 posts: http://{}/readings", advertisedHost);
        LOG.info("Web page: http://{}/", advertisedHost);
    }

    private static String lanIp(String probeAddress) {
        try (var probe = new DatagramSocket()) {
            probe.connect(new InetSocketAddress(probeAddress, 80));
            return probe.getLocalAddress().getHostAddress();
        } catch (IOException | RuntimeException e) {
            LOG.warn("Could not detect a LAN address via {}; advertising 127.0.0.1", probeAddress, e);
            return "127.0.0.1";
        }
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
