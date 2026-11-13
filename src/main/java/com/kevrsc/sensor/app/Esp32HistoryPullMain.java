package com.kevrsc.sensor.app;

import com.kevrsc.sensor.importer.Esp32HistoryPublisher;
import com.kevrsc.sensor.kafka.KafkaClientConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class Esp32HistoryPullMain {

    private static final Logger LOG = LoggerFactory.getLogger(Esp32HistoryPullMain.class);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private Esp32HistoryPullMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 2) {
            usage();
        }
        Long after = null;
        if (args.length == 2) {
            try {
                after = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                usage();
            }
            if (after < 0) {
                usage();
            }
        }
        URI uri;
        try {
            uri = Esp32HistoryPublisher.historyUri(args[0], after);
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            usage();
            return;
        }

        var bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        var topic = env("SENSOR_TOPIC", "sensor-measurements");
        LOG.info("Pulling {} into {} via {}", uri, topic, bootstrapServers);

        var client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
        var request = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var body = response.body()) {
            if (response.statusCode() != 200) {
                var snippet = new String(body.readNBytes(256), java.nio.charset.StandardCharsets.UTF_8)
                        .replaceAll("\\s+", " ")
                        .trim();
                throw new IllegalStateException("ESP32 history returned HTTP " + response.statusCode()
                        + (snippet.isEmpty() ? "" : ": " + snippet));
            }
            try (var producer = new KafkaProducer<String, String>(KafkaClientConfig.producerProperties(bootstrapServers))) {
                var result = new Esp32HistoryPublisher(producer, topic).publish(body);
                LOG.info("Published {} readings to {}", result.published(), topic);
                if (!result.nonObjectIndexes().isEmpty()) {
                    LOG.warn("History entries that were not JSON objects (sent on so the pipeline dead-letters them): {}",
                            result.nonObjectIndexes());
                }
            }
        }
    }

    private static void usage() {
        System.err.println("usage: Esp32HistoryPullMain <board-url> [after-unix-seconds]");
        System.exit(2);
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
