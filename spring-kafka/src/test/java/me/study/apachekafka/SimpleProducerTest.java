package me.study.apachekafka;

import org.junit.jupiter.api.Test;

class SimpleProducerTest {

    @Test
    void produce() {
        SimpleProducer simpleProducer = new SimpleProducer();

        simpleProducer.produce();
    }

}