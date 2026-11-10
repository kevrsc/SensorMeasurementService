package com.kevrsc.sensor.http;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class ReadingsIngestHandlerTest {

    private static final String TOPIC = "sensor-measurements";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-10T18:47:12Z"), ZoneId.of("America/Los_Angeles"));

    private final HttpClient client = HttpClient.newHttpClient();
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void publishesPostedEventKeyedByDevice() throws Exception {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        start(producer);

        var response = post("/readings", """
                {"device":"vermiculture-esp32","sensors":[],"received_at":"2026-11-10T10:30:00-08:00"}""");

        assertEquals(204, response.statusCode());
        assertEquals(1, producer.history().size());
        var record = producer.history().getFirst();
        assertEquals(TOPIC, record.topic());
        assertEquals("vermiculture-esp32", record.key());
        assertEquals("{\"device\":\"vermiculture-esp32\",\"sensors\":[],\"received_at\":\"2026-11-10T10:30:00-08:00\"}", record.value());
    }

    @Test
    void stampsReceivedAtWhenMissing() throws Exception {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        start(producer);

        var response = post("/readings", """
                {"device":"vermiculture-esp32","received_at":"  "}""");

        assertEquals(204, response.statusCode());
        assertEquals("{\"device\":\"vermiculture-esp32\",\"received_at\":\"2026-11-10T10:47:12-08:00\"}",
                producer.history().getFirst().value());
    }

    @Test
    void rejectsBodyThatIsNotJson() throws Exception {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        start(producer);

        var response = post("/readings", "not json");

        assertEquals(400, response.statusCode());
        assertEquals("expected JSON", response.body());
        assertTrue(producer.history().isEmpty());
    }

    @Test
    void rejectsJsonThatIsNotAnObject() throws Exception {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        start(producer);

        var response = post("/readings", "[1,2]");

        assertEquals(400, response.statusCode());
        assertEquals("expected a JSON object", response.body());
        assertTrue(producer.history().isEmpty());
    }

    @Test
    void returnsServiceUnavailableWhenKafkaWriteFails() throws Exception {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                return CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));
            }
        };
        start(producer);

        var response = post("/readings", """
                {"device":"vermiculture-esp32"}""");

        assertEquals(503, response.statusCode());
    }

    @Test
    void rejectsOtherPathsAndMethods() throws Exception {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        start(producer);

        assertEquals(404, post("/history", "{}").statusCode());
        var get = client.send(HttpRequest.newBuilder(uri("/readings")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(405, get.statusCode());
        assertTrue(producer.history().isEmpty());
    }

    private void start(MockProducer<String, String> producer) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", new ReadingsIngestHandler(producer, TOPIC, CLOCK, Duration.ofSeconds(5)));
        server.start();
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        var request = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }
}
