package me.study.apachekafka.consumer;

import org.junit.jupiter.api.Test;

class SimpleConsumerTest {

    private SimpleConsumer simpleConsumer = new SimpleConsumer();

    @Test
    void consume() {
        simpleConsumer.consume();
    }

    @Test
    void consumeWithSafeClose() {
        simpleConsumer.consumeWithSafeClose();
    }

    @Test
    void consumeWithAutoCommit() {
        simpleConsumer.consumeWithAutoCommit();
    }

    @Test
    void consumeWithCommitAsync() {
        simpleConsumer.consumeWithCommitAsync();
    }

    @Test
    void rebalanceListener() {
        simpleConsumer.rebalanceListener();
    }

    @Test
    void assignConsumer() {
        simpleConsumer.assignConsumer();
    }
}