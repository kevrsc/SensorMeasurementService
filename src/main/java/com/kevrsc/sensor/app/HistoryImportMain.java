package com.kevrsc.sensor.app;

import com.kevrsc.sensor.importer.JsonlHistoryPublisher;
import com.kevrsc.sensor.kafka.KafkaClientConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public final class HistoryImportMain {

    private static final Logger LOG = LoggerFactory.getLogger(HistoryImportMain.class);

    private HistoryImportMain() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: HistoryImportMain <readings.jsonl>");
            System.exit(2);
        }
        var file = Path.of(args[0]);
        var bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        var topic = env("SENSOR_TOPIC", "sensor-measurements");
        LOG.info("Importing {} into {} via {}", file.toAbsolutePath(), topic, bootstrapServers);

        try (var producer = new KafkaProducer<String, String>(KafkaClientConfig.producerProperties(bootstrapServers))) {
            var result = new JsonlHistoryPublisher(producer, topic).publish(file);
            LOG.info("Published {} readings to {}", result.published(), topic);
            if (!result.unparseableLines().isEmpty()) {
                LOG.warn("Lines that were not JSON (sent on so the pipeline dead-letters them): {}", result.unparseableLines());
            }
        }
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
