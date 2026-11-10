package com.kevrsc.sensor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface DeadLetterFailureHandler {

    void onFailure(ConsumerRecord<String, String> record, List<String> validationErrors, Exception failure);
}
