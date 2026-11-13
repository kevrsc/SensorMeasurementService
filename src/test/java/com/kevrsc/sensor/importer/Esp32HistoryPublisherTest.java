package com.kevrsc.sensor.importer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Esp32HistoryPublisherTest {

    @Test
    void publishesEachObjectUnchangedKeyedByDevice() {
        var body = """
                [{"device":"vermiculture-esp32","sensors":[],"received_at":"2026-10-21T22:14:30-07:00"},
                 {"device":"other-esp32","sensors":[],"received_at":"2026-10-21T22:29:30-07:00"}]""";
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        var result = new Esp32HistoryPublisher(producer, "sensor-measurements").publish(stream(body));

        assertEquals(new Esp32HistoryPublisher.Result(2, List.of()), result);
        assertEquals(List.of("vermiculture-esp32", "other-esp32"), producer.history().stream().map(r -> r.key()).toList());
        assertEquals(List.of(
                "{\"device\":\"vermiculture-esp32\",\"sensors\":[],\"received_at\":\"2026-10-21T22:14:30-07:00\"}",
                "{\"device\":\"other-esp32\",\"sensors\":[],\"received_at\":\"2026-10-21T22:29:30-07:00\"}"),
                producer.history().stream().map(r -> r.value()).toList());
        assertEquals("sensor-measurements", producer.history().getFirst().topic());
    }

    @Test
    void stillPublishesEntriesThatAreNotObjectsAndReportsTheirIndexes() {
        var body = """
                [{"device":"vermiculture-esp32"}, 1, null]""";
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        var result = new Esp32HistoryPublisher(producer, "sensor-measurements").publish(stream(body));

        assertEquals(new Esp32HistoryPublisher.Result(3, List.of(2, 3)), result);
        assertEquals("1", producer.history().get(1).value());
        assertEquals("null", producer.history().get(2).value());
        assertEquals(null, producer.history().get(1).key());
    }

    @Test
    void rejectsABodyThatIsNotAJsonArray() {
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> new Esp32HistoryPublisher(producer, "sensor-measurements").publish(stream("{\"device\":\"x\"}")));

        assertEquals("ESP32 history body must be a JSON array", thrown.getMessage());
        assertEquals(0, producer.history().size());
    }

    @Test
    void buildsTheHistoryUriFromABoardUrlAndOptionalAfter() {
        assertEquals("http://vermiculture.local/history",
                Esp32HistoryPublisher.historyUri("http://vermiculture.local", null).toString());
        assertEquals("http://vermiculture.local/history",
                Esp32HistoryPublisher.historyUri("http://vermiculture.local/", null).toString());
        assertEquals("http://vermiculture.local/history?after=1731000000",
                Esp32HistoryPublisher.historyUri("http://vermiculture.local/history", 1731000000L).toString());
        assertThrows(IllegalArgumentException.class, () -> Esp32HistoryPublisher.historyUri("vermiculture.local", null));
    }

    private static ByteArrayInputStream stream(String body) {
        return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
    }
}
