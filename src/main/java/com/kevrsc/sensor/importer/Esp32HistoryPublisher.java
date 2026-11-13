package com.kevrsc.sensor.importer;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class Esp32HistoryPublisher {

    public record Result(int published, List<Integer> nonObjectIndexes) {
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final Producer<String, String> producer;
    private final String topic;

    public Esp32HistoryPublisher(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public static URI historyUri(String boardUrl, Long after) {
        var base = boardUrl.endsWith("/") ? boardUrl.substring(0, boardUrl.length() - 1) : boardUrl;
        if (!base.endsWith("/history")) {
            base = base + "/history";
        }
        if (after != null) {
            base = base + "?after=" + after;
        }
        var uri = URI.create(base);
        var scheme = uri.getScheme();
        if (scheme == null || (!scheme.equals("http") && !scheme.equals("https"))) {
            throw new IllegalArgumentException("board URL must be http or https: " + boardUrl);
        }
        return uri;
    }

    public Result publish(InputStream body) {
        var sends = new ArrayList<Future<RecordMetadata>>();
        var indexes = new ArrayList<Integer>();
        var nonObjects = new ArrayList<Integer>();
        try (var parser = mapper.getFactory().createParser(body)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalArgumentException("ESP32 history body must be a JSON array");
            }
            var index = 0;
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                if (parser.currentToken() == null) {
                    throw new IllegalArgumentException("ESP32 history array ended early");
                }
                index++;
                var node = mapper.readTree(parser);
                String device = null;
                if (node instanceof ObjectNode object && object.path("device").isTextual()) {
                    device = object.get("device").asText();
                } else {
                    nonObjects.add(index);
                }
                sends.add(producer.send(new ProducerRecord<>(topic, device, mapper.writeValueAsString(node))));
                indexes.add(index);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not read ESP32 history", e);
        }
        producer.flush();
        for (var i = 0; i < sends.size(); i++) {
            try {
                sends.get(i).get();
            } catch (ExecutionException e) {
                throw new IllegalStateException("failed to publish history event " + indexes.get(i), e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while publishing ESP32 history", e);
            }
        }
        return new Result(sends.size(), List.copyOf(nonObjects));
    }
}
