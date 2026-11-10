package com.kevrsc.sensor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public final class ReadingsQueryHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReadingsQueryHandler.class);
    private static final String JSON = "application/json; charset=utf-8";
    private static final String TEXT = "text/plain; charset=utf-8";
    private static final String PAGE_RESOURCE = "web/index.html";

    private final ObjectMapper mapper = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    private final MeasurementHistory history;
    private final byte[] page;

    public ReadingsQueryHandler(MeasurementHistory history) {
        this.history = history;
        this.page = loadPage();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            var path = exchange.getRequestURI().getPath();
            try {
                switch (path) {
                    case "/" -> send(exchange, 200, "text/html; charset=utf-8", page);
                    case "/history" -> send(exchange, 200, JSON, mapper.writeValueAsBytes(history.history()));
                    case "/readings" -> send(exchange, 200, JSON, mapper.writeValueAsBytes(history.latest().orElse(null)));
                    default -> send(exchange, 404, TEXT, "not found".getBytes(StandardCharsets.UTF_8));
                }
            } catch (RuntimeException e) {
                LOG.error("Failed to serve {}", path, e);
                send(exchange, 503, TEXT, "could not load readings".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static void send(HttpExchange exchange, int status, String contentType, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    private static byte[] loadPage() {
        try (var in = ReadingsQueryHandler.class.getClassLoader().getResourceAsStream(PAGE_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(PAGE_RESOURCE + " not found on classpath");
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read " + PAGE_RESOURCE, e);
        }
    }
}
