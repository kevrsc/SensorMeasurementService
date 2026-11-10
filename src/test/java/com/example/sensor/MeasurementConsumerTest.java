package com.example.sensor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class MeasurementConsumerTest {

    private static final String TOPIC = "sensor-measurements";
    private static final String DLQ_TOPIC = "sensor-measurements.dlq";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private final MockConsumer<String, String> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<String, String> producer =
            new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    private final List<Exception> failures = new ArrayList<>();
    private final DeadLetterFailureHandler recordingHandler = (record, errors, failure) -> failures.add(failure);
    private final MeasurementConsumer consumer = consumerWith(producer);

    private MeasurementConsumer consumerWith(MockProducer<String, String> producer) {
        return new MeasurementConsumer(kafka, producer, DLQ_TOPIC, recordingHandler,
                new StructuralValidationStage(new SensorEventParser(), new StructuralValidator()));
    }

    @BeforeEach
    void assignPartition() {
        kafka.assign(List.of(PARTITION));
        kafka.updateBeginningOffsets(Map.of(PARTITION, 0L));
    }

    @Test
    void passesEachPolledRecordThroughStructuralValidation() {
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "vermiculture-esp32", """
                {"device": "vermiculture-esp32", "sensors": [], "received_at": "2026-11-09T11:56:34-08:00"}
                """));
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "vermiculture-esp32", "{\"device\": "));

        var results = consumer.pollOnce(Duration.ZERO);

        assertEquals(2, results.size());
        assertInstanceOf(StructuralResult.Valid.class, results.get(0));
        assertInstanceOf(StructuralResult.Invalid.class, results.get(1));
    }

    @Test
    void commitsOffsetsAfterProcessingBatch() {
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "vermiculture-esp32", """
                {"device": "vermiculture-esp32", "sensors": [], "received_at": "2026-11-09T11:56:34-08:00"}
                """));
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "vermiculture-esp32", "{\"device\": "));

        consumer.pollOnce(Duration.ZERO);

        assertEquals(2L, kafka.committed(Set.of(PARTITION)).get(PARTITION).offset());
    }

    @Test
    @Timeout(5)
    void runKeepsPollingUntilWokenUp() {
        kafka.schedulePollTask(() -> kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "vermiculture-esp32", """
                {"device": "vermiculture-esp32", "sensors": [], "received_at": "2026-11-09T11:56:34-08:00"}
                """)));
        kafka.schedulePollTask(kafka::wakeup);

        consumer.run(Duration.ZERO);

        assertEquals(1L, kafka.committed(Set.of(PARTITION)).get(PARTITION).offset());
    }

    @Test
    void reportsFailedDeadLetterSendAndStillCommits() {
        var failingProducer = new MockProducer<String, String>(false, new StringSerializer(), new StringSerializer()) {
            @Override
            public synchronized void flush() {
                while (errorNext(new RuntimeException("dlq unavailable"))) {
                }
            }
        };
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "vermiculture-esp32", "{\"device\": "));

        consumerWith(failingProducer).pollOnce(Duration.ZERO);

        assertEquals(1, failures.size());
        assertEquals("dlq unavailable", failures.get(0).getMessage());
        assertEquals(1L, kafka.committed(Set.of(PARTITION)).get(PARTITION).offset());
    }

    @Test
    void sendsInvalidRecordToDeadLetterTopicWithErrorHeaders() {
        var payload = """
                {"device": " ", "sensors": [], "received_at": null}
                """;
        kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "vermiculture-esp32", payload));

        consumer.pollOnce(Duration.ZERO);

        assertEquals(1, producer.history().size());
        var sent = producer.history().get(0);
        assertEquals(DLQ_TOPIC, sent.topic());
        assertEquals("vermiculture-esp32", sent.key());
        assertEquals(payload, sent.value());
        var errors = StreamSupport.stream(sent.headers().headers("validation-error").spliterator(), false)
                .map(h -> new String(h.value(), StandardCharsets.UTF_8))
                .toList();
        assertEquals(List.of("device is required", "receivedAt is required"), errors);
    }
}
