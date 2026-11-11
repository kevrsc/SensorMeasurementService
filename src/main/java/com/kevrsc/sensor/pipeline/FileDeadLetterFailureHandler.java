package com.kevrsc.sensor.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;

public class FileDeadLetterFailureHandler implements DeadLetterFailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FileDeadLetterFailureHandler.class);

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();

    public FileDeadLetterFailureHandler(Path file) {
        this.file = file;
    }

    @Override
    public synchronized void onFailure(
            ConsumerRecord<String, String> record, List<String> validationErrors, Exception failure) {
        LOG.error("dead-letter publish failed for {}-{}@{} key={} errors={}",
                record.topic(), record.partition(), record.offset(), record.key(), validationErrors, failure);

        var entry = new LinkedHashMap<String, Object>();
        entry.put("failed_at", Instant.now().toString());
        entry.put("topic", record.topic());
        entry.put("partition", record.partition());
        entry.put("offset", record.offset());
        entry.put("key", record.key());
        entry.put("payload", record.value());
        entry.put("validation_errors", validationErrors);
        entry.put("failure", failure.getMessage());

        try {
            Files.writeString(file, mapper.writeValueAsString(entry) + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.error("could not record dead-letter failure; payload was: {}", record.value(), e);
        }
    }
}
