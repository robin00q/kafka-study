package me.study.apachekafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.UniformStickyPartitioner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleProducerTest {

    SimpleProducer producer = new SimpleProducer();

    @Test
    void produceWithoutMessageKey() {
        producer.produceWithoutMessageKey();
    }

    @Test
    void produceWithMessageKey() {
        producer.produceWithMessageKey();
    }

    @Test
    void produceToPartitionNumber() {
        producer.produceToPartitionNumber();
    }

    @Test
    void produceWithCustomPartitioner() {
        producer.produceWithCustomPartitioner();
    }

    @Test
    void produceAndGetResult() {
        producer.produceAndGetResult();
    }

}