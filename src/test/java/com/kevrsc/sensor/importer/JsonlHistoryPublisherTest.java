package com.kevrsc.sensor.importer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonlHistoryPublisherTest {

    @TempDir
    Path tempDir;

    @Test
    void publishesEachLineUnchangedKeyedByDeviceAndSkipsBlankLines() throws Exception {
        var first = "{\"device\": \"vermiculture-esp32\", \"sensors\": [], \"received_at\": \"2026-10-21T22:14:30-07:00\"}";
        var second = "{\"device\": \"other-esp32\", \"sensors\": [], \"received_at\": \"2026-10-21T22:29:30-07:00\"}";
        var file = tempDir.resolve("readings.jsonl");
        Files.writeString(file, first + "\n\n" + second + "\n");
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        var result = new JsonlHistoryPublisher(producer, "sensor-measurements").publish(file);

        assertEquals(new JsonlHistoryPublisher.Result(2, List.of()), result);
        assertEquals(List.of("vermiculture-esp32", "other-esp32"), producer.history().stream().map(r -> r.key()).toList());
        assertEquals(List.of(first, second), producer.history().stream().map(r -> r.value()).toList());
        assertEquals("sensor-measurements", producer.history().getFirst().topic());
    }

    @Test
    void stillPublishesLinesThatAreNotJsonAndReportsTheirLineNumbers() throws Exception {
        var file = tempDir.resolve("readings.jsonl");
        Files.writeString(file, "{\"device\": \"vermiculture-esp32\"}\nnot json\n");
        var producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        var result = new JsonlHistoryPublisher(producer, "sensor-measurements").publish(file);

        assertEquals(new JsonlHistoryPublisher.Result(2, List.of(2)), result);
        assertEquals("not json", producer.history().getLast().value());
        assertEquals(null, producer.history().getLast().key());
    }
}
