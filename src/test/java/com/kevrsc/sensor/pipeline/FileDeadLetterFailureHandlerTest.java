package com.kevrsc.sensor.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FileDeadLetterFailureHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void appendsOneJsonLinePerFailure() throws Exception {
        var file = tempDir.resolve("dead-letter-failures.jsonl");
        var handler = new FileDeadLetterFailureHandler(file);
        var record = new ConsumerRecord<>("sensor-measurements", 3, 42L, "vermiculture-esp32", "{\"device\": ");

        handler.onFailure(record, List.of("malformed event JSON: boom"), new RuntimeException("dlq unavailable"));
        handler.onFailure(record, List.of("device is required"), new RuntimeException("dlq unavailable"));

        var lines = Files.readAllLines(file);
        assertEquals(2, lines.size());
        var entry = new ObjectMapper().readTree(lines.get(0));
        assertEquals("sensor-measurements", entry.get("topic").asText());
        assertEquals(3, entry.get("partition").asInt());
        assertEquals(42L, entry.get("offset").asLong());
        assertEquals("vermiculture-esp32", entry.get("key").asText());
        assertEquals("{\"device\": ", entry.get("payload").asText());
        assertEquals("malformed event JSON: boom", entry.get("validation_errors").get(0).asText());
        assertEquals("dlq unavailable", entry.get("failure").asText());
        assertNotNull(entry.get("failed_at"));
    }
}
