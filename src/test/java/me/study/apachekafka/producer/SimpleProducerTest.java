package me.study.apachekafka.producer;

import org.junit.jupiter.api.Test;

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

    @Test
    void idempotentProducer() {
        producer.idempotentProducer();
    }

    @Test
    void transactionProducer() {
        producer.transactionProducer();
    }

}