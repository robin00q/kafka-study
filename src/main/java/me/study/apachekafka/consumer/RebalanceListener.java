package me.study.apachekafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@Slf4j
public class RebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // 리밸런스가 발생하기 직전에, 어떤 파티션이 할당됐었는지
        log.info("onPartitionsRevoked : {}", partitions.toString());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 리밸런스가 발생한 뒤, 컨슈머에 할당된 파티션
        log.info("onPartitionsAssigned : {}", partitions.toString());
    }
}
