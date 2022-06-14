package me.study.apachekafka.streams;

import org.junit.jupiter.api.Test;

class StatelessStreamsTest {

    StatelessStreams statelessStreams = new StatelessStreams();

    @Test
    void filterValueLengthLargerThan5() {
        statelessStreams.filterValueLengthLargerThan5();
    }
}