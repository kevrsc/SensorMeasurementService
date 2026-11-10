package com.kevrsc.sensor.importer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class JsonlHistoryPublisher {

    public record Result(int published, List<Integer> unparseableLines) {
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Producer<String, String> producer;
    private final String topic;

    public JsonlHistoryPublisher(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public Result publish(Path file) {
        var sends = new ArrayList<Future<RecordMetadata>>();
        var sendLines = new ArrayList<Integer>();
        var unparseable = new ArrayList<Integer>();
        try (var lines = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            var lineNumber = 0;
            String line;
            while ((line = lines.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                String device = null;
                try {
                    var node = mapper.readTree(line).path("device");
                    device = node.isTextual() ? node.asText() : null;
                } catch (JsonProcessingException e) {
                    unparseable.add(lineNumber);
                }
                sends.add(producer.send(new ProducerRecord<>(topic, device, line)));
                sendLines.add(lineNumber);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not read " + file, e);
        }
        producer.flush();
        for (var i = 0; i < sends.size(); i++) {
            try {
                sends.get(i).get();
            } catch (ExecutionException e) {
                throw new IllegalStateException("failed to publish line " + sendLines.get(i) + " of " + file, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while publishing " + file, e);
            }
        }
        return new Result(sends.size(), List.copyOf(unparseable));
    }
}
