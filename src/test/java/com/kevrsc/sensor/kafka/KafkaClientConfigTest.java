package com.kevrsc.sensor.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaClientConfigTest {

    @Test
    void consumerCommitsManually() {
        var props = KafkaClientConfig.consumerProperties("broker:9092", "sensor-measurement-service");

        assertEquals("broker:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("sensor-measurement-service", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("false", props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(StringDeserializer.class.getName(), props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class.getName(), props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    @Test
    void producerRetriesSafelyBeforeGivingUp() {
        var props = KafkaClientConfig.producerProperties("broker:9092");

        assertEquals("broker:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("true", props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("120000", props.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void ingestProducerGivesUpWithinAnHttpRequest() {
        var props = KafkaClientConfig.ingestProducerProperties("broker:9092");

        assertEquals("broker:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("true", props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("5000", props.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
        assertEquals("5000", props.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals("10000", props.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
    }
}
