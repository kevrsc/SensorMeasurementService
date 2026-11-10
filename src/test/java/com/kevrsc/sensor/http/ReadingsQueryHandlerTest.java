package com.kevrsc.sensor.http;

import com.kevrsc.sensor.event.DeviceStatus;
import com.kevrsc.sensor.event.MoistureReading;
import com.kevrsc.sensor.event.SensorMeasurementEvent;
import com.kevrsc.sensor.event.SensorReading;
import com.kevrsc.sensor.storage.MeasurementHistory;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class ReadingsQueryHandlerTest {

    private static final SensorMeasurementEvent EVENT = new SensorMeasurementEvent("vermiculture-esp32",
            List.of(new SensorReading(0, "2800000000000001", 18.06)),
            List.of(new MoistureReading(0, 35, 1879, 83)),
            new DeviceStatus(229504121L, "POWERON", 3, -58, "192.168.1.20", 2, 255, 256L,
                    187372L, 146388L, "0123456789abcdeffedcba9876543210", "disabled", null, 4),
            OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));

    private static final String EVENT_JSON = "{\"device\":\"vermiculture-esp32\","
            + "\"sensors\":[{\"index\":0,\"address\":\"2800000000000001\",\"celsius\":18.06}],"
            + "\"moisture\":[{\"index\":0,\"pin\":35,\"raw\":1879,\"percent\":83}],"
            + "\"status\":{\"uptime_ms\":229504121,\"reset_reason\":\"POWERON\",\"boot_count\":3,\"rssi_dbm\":-58,"
            + "\"ip\":\"192.168.1.20\",\"wifi_reconnects\":2,\"post_failures\":255,\"seq\":256,\"free_heap\":187372,"
            + "\"min_free_heap\":146388,\"sketch_md5\":\"0123456789abcdeffedcba9876543210\",\"keepalive\":\"disabled\","
            + "\"keepalive_age_ms\":null,\"probe_count\":4},"
            + "\"received_at\":\"2026-11-09T11:56:34-08:00\"}";

    private final HttpClient client = HttpClient.newHttpClient();
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void historyReturnsStoredEventsInThePostedFormat() throws Exception {
        start(new FakeHistory(List.of(EVENT)));

        var response = get("/history");

        assertEquals(200, response.statusCode());
        assertEquals("application/json; charset=utf-8", response.headers().firstValue("Content-Type").orElseThrow());
        assertEquals("no-store", response.headers().firstValue("Cache-Control").orElseThrow());
        assertEquals("[" + EVENT_JSON + "]", response.body());
    }

    @Test
    void readingsReturnsTheLatestEvent() throws Exception {
        start(new FakeHistory(List.of(EVENT)));

        var response = get("/readings");

        assertEquals(200, response.statusCode());
        assertEquals(EVENT_JSON, response.body());
    }

    @Test
    void readingsReturnsNullBeforeAnythingIsStored() throws Exception {
        start(new FakeHistory(List.of()));

        var response = get("/readings");

        assertEquals(200, response.statusCode());
        assertEquals("null", response.body());
    }

    @Test
    void rootServesTheWebPage() throws Exception {
        start(new FakeHistory(List.of()));

        var response = get("/");

        assertEquals(200, response.statusCode());
        assertEquals("text/html; charset=utf-8", response.headers().firstValue("Content-Type").orElseThrow());
        assertTrue(response.body().contains("<title>Vermiculture sensors</title>"));
    }

    @Test
    void unknownPathIsNotFound() throws Exception {
        start(new FakeHistory(List.of()));

        assertEquals(404, get("/nothing-here").statusCode());
    }

    @Test
    void databaseFailureIsServiceUnavailable() throws Exception {
        start(new MeasurementHistory() {
            @Override
            public List<SensorMeasurementEvent> history() {
                throw new IllegalStateException("database down");
            }

            @Override
            public Optional<SensorMeasurementEvent> latest() {
                throw new IllegalStateException("database down");
            }
        });

        assertEquals(503, get("/history").statusCode());
        assertEquals(503, get("/readings").statusCode());
    }

    private void start(MeasurementHistory history) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", new ReadingsQueryHandler(history));
        server.start();
    }

    private HttpResponse<String> get(String path) throws Exception {
        var uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
        return client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private record FakeHistory(List<SensorMeasurementEvent> events) implements MeasurementHistory {
        @Override
        public List<SensorMeasurementEvent> history() {
            return events;
        }

        @Override
        public Optional<SensorMeasurementEvent> latest() {
            return events.isEmpty() ? Optional.empty() : Optional.of(events.getLast());
        }
    }
}
