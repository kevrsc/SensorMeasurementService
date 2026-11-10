package com.kevrsc.sensor.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ReadingsIngestHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReadingsIngestHandler.class);
    private static final String PATH = "/readings";

    private final ObjectMapper mapper = new ObjectMapper();
    private final Producer<String, String> producer;
    private final String topic;
    private final Clock clock;
    private final Duration sendTimeout;

    public ReadingsIngestHandler(Producer<String, String> producer, String topic, Clock clock, Duration sendTimeout) {
        this.producer = producer;
        this.topic = topic;
        this.clock = clock;
        this.sendTimeout = sendTimeout;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!PATH.equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, "not found");
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "POST");
                send(exchange, 405, "method not allowed");
                return;
            }

            JsonNode body;
            try {
                body = mapper.readTree(exchange.getRequestBody());
            } catch (JsonProcessingException e) {
                send(exchange, 400, "expected JSON");
                return;
            }
            if (!(body instanceof ObjectNode reading)) {
                send(exchange, 400, "expected a JSON object");
                return;
            }

            var receivedAt = reading.path("received_at");
            if (!receivedAt.isTextual() || receivedAt.asText().isBlank()) {
                reading.put("received_at", OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS)
                        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            }
            var device = reading.path("device").isTextual() ? reading.get("device").asText() : null;

            try {
                var metadata = producer.send(new ProducerRecord<>(topic, device, mapper.writeValueAsString(reading)))
                        .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("Published reading from {} to {}-{}@{}", device, metadata.topic(), metadata.partition(), metadata.offset());
                send(exchange, 204, "");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while publishing reading from {}", device, e);
                send(exchange, 503, "could not store reading");
            } catch (ExecutionException | TimeoutException | RuntimeException e) {
                LOG.error("Failed to publish reading from {} to {}", device, topic, e);
                send(exchange, 503, "could not store reading");
            }
        }
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        var bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            exchange.getResponseBody().write(bytes);
        }
    }
}
