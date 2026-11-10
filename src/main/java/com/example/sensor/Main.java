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
    private static final String DEFAULT_CALIBRATION_RESOURCE = "moisture-calibration.json";

    private Main() {
    }

    public static void main(String[] args) {
        var bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        var groupId = env("KAFKA_GROUP_ID", "sensor-measurement-service");
        var topic = env("SENSOR_TOPIC", "sensor-measurements");
        var deadLetterTopic = env("SENSOR_DLQ_TOPIC", topic + ".dlq");
        var failuresFile = Path.of(env("DLQ_FAILURES_FILE", "dead-letter-failures.jsonl"));
        var dbUrl = env("DB_URL", "jdbc:postgresql://localhost:5432/sensors");
        var dbUser = env("DB_USER", "sensors");
        var dbPassword = env("DB_PASSWORD", "sensors");
        var calibrations = moistureCalibrations();
        LOG.info("Starting sensor-measurement-service: bootstrapServers={}, groupId={}, topic={}, deadLetterTopic={}, failuresFile={}, dbUrl={}",
                bootstrapServers, groupId, topic, deadLetterTopic, failuresFile.toAbsolutePath(), dbUrl);

        DatabaseMigrator.migrate(dbUrl, dbUser, dbPassword);
        LOG.info("Database schema is up to date");

        var kafkaConsumer = new KafkaConsumer<String, String>(KafkaClientConfig.consumerProperties(bootstrapServers, groupId));
        var kafkaProducer = new KafkaProducer<String, String>(KafkaClientConfig.producerProperties(bootstrapServers));
        var measurementConsumer = new MeasurementConsumer(
                kafkaConsumer,
                kafkaProducer,
                deadLetterTopic,
                new FileDeadLetterFailureHandler(failuresFile),
                new StructuralValidationStage(new SensorEventParser(), new StructuralValidator()),
                new BusinessValidationStage(calibrations),
                new RetryingMeasurementWriter(
                        new TimescaleMeasurementWriter(dbUrl, dbUser, dbPassword),
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

    private static MoistureCalibrations moistureCalibrations() {
        var file = System.getenv("MOISTURE_CALIBRATION_FILE");
        if (file != null && !file.isBlank()) {
            var calibrations = MoistureCalibrationLoader.load(Path.of(file));
            LOG.info("Moisture calibration loaded from {}: default={}, perProbe={}",
                    Path.of(file).toAbsolutePath(), calibrations.defaultCalibration(), calibrations.byDeviceAndProbe());
            return calibrations;
        }
        var calibrations = MoistureCalibrationLoader.loadResource(DEFAULT_CALIBRATION_RESOURCE);
        LOG.info("Moisture calibration loaded from classpath {}: default={}, perProbe={}",
                DEFAULT_CALIBRATION_RESOURCE, calibrations.defaultCalibration(), calibrations.byDeviceAndProbe());
        return calibrations;
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
