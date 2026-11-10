package com.example.sensor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MeasurementConsumer {

    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final String deadLetterTopic;
    private final DeadLetterFailureHandler failureHandler;
    private final StructuralValidationStage stage;

    public MeasurementConsumer(
            Consumer<String, String> consumer,
            Producer<String, String> producer,
            String deadLetterTopic,
            DeadLetterFailureHandler failureHandler,
            StructuralValidationStage stage) {
        this.consumer = consumer;
        this.producer = producer;
        this.deadLetterTopic = deadLetterTopic;
        this.failureHandler = failureHandler;
        this.stage = stage;
    }

    public void run(Duration pollTimeout) {
        try {
            while (true) {
                pollOnce(pollTimeout);
            }
        } catch (WakeupException e) {
            // consumer.wakeup() was called to request shutdown
        }
    }

    public List<StructuralResult> pollOnce(Duration timeout) {
        var results = new ArrayList<StructuralResult>();
        for (var record : consumer.poll(timeout)) {
            var result = stage.process(record.value());
            if (result instanceof StructuralResult.Invalid invalid) {
                sendToDeadLetter(record, invalid.errors());
            }
            results.add(result);
        }
        producer.flush();
        consumer.commitSync();
        return results;
    }

    private void sendToDeadLetter(ConsumerRecord<String, String> record, List<String> errors) {
        var deadLetter = new ProducerRecord<>(deadLetterTopic, record.key(), record.value());
        for (var error : errors) {
            deadLetter.headers().add("validation-error", error.getBytes(StandardCharsets.UTF_8));
        }
        try {
            producer.send(deadLetter, (metadata, exception) -> {
                if (exception != null) {
                    failureHandler.onFailure(record, errors, exception);
                }
            });
        } catch (RuntimeException e) {
            failureHandler.onFailure(record, errors, e);
        }
    }
}
