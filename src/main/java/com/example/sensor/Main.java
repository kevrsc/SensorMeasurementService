package com.example.sensor;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) {
        var bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        var groupId = env("KAFKA_GROUP_ID", "sensor-measurement-service");
        var topic = env("SENSOR_TOPIC", "sensor-measurements");
        var deadLetterTopic = env("SENSOR_DLQ_TOPIC", topic + ".dlq");
        var failuresFile = Path.of(env("DLQ_FAILURES_FILE", "dead-letter-failures.jsonl"));
        LOG.info("Starting sensor-measurement-service: bootstrapServers={}, groupId={}, topic={}, deadLetterTopic={}, failuresFile={}",
                bootstrapServers, groupId, topic, deadLetterTopic, failuresFile.toAbsolutePath());

        var kafkaConsumer = new KafkaConsumer<String, String>(KafkaClientConfig.consumerProperties(bootstrapServers, groupId));
        var kafkaProducer = new KafkaProducer<String, String>(KafkaClientConfig.producerProperties(bootstrapServers));
        var measurementConsumer = new MeasurementConsumer(
                kafkaConsumer,
                kafkaProducer,
                deadLetterTopic,
                new FileDeadLetterFailureHandler(failuresFile),
                new StructuralValidationStage(new SensorEventParser(), new StructuralValidator()),
                new BusinessValidationStage(new OutlierDetector()),
                new RetryingMeasurementWriter(
                        events -> LOG.info("Received {} enriched events; persistence not yet implemented", events.size()),
                        6, Duration.ofSeconds(1), Duration.ofSeconds(30), Thread::sleep));

        var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received; stopping consumer");
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try (kafkaConsumer; kafkaProducer) {
            kafkaConsumer.subscribe(List.of(topic));
            LOG.info("Subscribed to {}; consuming", topic);
            measurementConsumer.run(Duration.ofMillis(500));
            LOG.info("Consumer loop stopped; closing Kafka clients");
        }
        LOG.info("sensor-measurement-service stopped");
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
