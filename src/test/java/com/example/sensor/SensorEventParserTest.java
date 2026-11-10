package com.example.sensor;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SensorEventParserTest {

    private final SensorEventParser parser = new SensorEventParser();

    @Test
    void parsesSamplePostedEvent() throws Exception {
        String json;
        try (var in = getClass().getResourceAsStream("/esp32-sensor-event.json")) {
            json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        var event = parser.parse(json);

        assertEquals("vermiculture-esp32", event.device());
        assertEquals(4, event.sensors().size());
        assertEquals(new SensorReading(1, "2800000000000002", 19.94), event.sensors().get(1));
        assertEquals(new MoistureReading(0, 35, 1879, 83), event.moisture().get(0));
        assertEquals(229504121L, event.status().uptimeMs());
        assertNull(event.status().keepaliveAgeMs());
        assertEquals(OffsetDateTime.parse("2026-11-09T11:56:34-08:00"), event.receivedAt());
    }

    @Test
    void treatsMissingArraysAsEmpty() throws Exception {
        var event = parser.parse("""
                {"device": "vermiculture-esp32", "received_at": "2026-11-09T11:56:34-08:00"}
                """);

        assertEquals(List.of(), event.sensors());
        assertEquals(List.of(), event.moisture());
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(MalformedEventException.class, () -> parser.parse("{\"device\": "));
    }

    @Test
    void ignoresUnknownFields() throws Exception {
        var event = parser.parse("""
                {"device": "vermiculture-esp32", "firmware": "1.2.3", "sensors": [], "received_at": "2026-11-09T11:56:34-08:00"}
                """);

        assertEquals("vermiculture-esp32", event.device());
    }
}
