package me.study.apachekafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class SimpleConsumer {

    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";

    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createProperties());

        // 메세지를 가져올 토픽을 구독한다.
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record: {}", record);
            }
        }
    }

    /**
     * 안전한 컨슈머 종료
     */
    public void consumeWithSafeClose() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown hook");
            consumer.wakeup();
        }));

        // 메세지를 가져올 토픽을 구독한다.
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("record: {}", record);
                }
            }
        } catch (WakeupException e) {
            log.warn("Wakeup consumer");
        } finally {
            log.warn("Consumer close");
            consumer.close();
        }
    }

    /**
     * 파티션 할당 컨슈머
     */
    public void assignConsumer() {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 그룹아이디가 필요없다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // 메세지를 가져올 토픽을 구독한다.
        // assign 으로 변경
        int PARTITION_NUMBER = 0;
        consumer.assign(Collections.singletonList(
                new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record: {}", record);
            }
        }
    }

    /**
     * 리밸런스 리스너
     */
    public void rebalanceListener() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createProperties());

        // 메세지를 가져올 토픽을 구독한다.
        // 리밸런스 리스너를 할당한다.
        consumer.subscribe(Collections.singletonList(TOPIC_NAME), new RebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record: {}", record);
            }
        }
    }

    /**
     * 오토커밋
     */
    public void consumeWithAutoCommit() {
        Properties properties = createProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // default
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 60000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record: {}", record);
            }
        }
    }

    /**
     * 수동커밋 (모든 레코드에 대해서 동기적으로 커밋한다.)
     */
    public void consumeWithSyncCommitForAllRecords() {
        Properties properties = createProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record: {}", record);
            }
            consumer.commitSync();
        }
    }

    /**
     * 비동기 오프셋 커밋
     */
    public void consumeWithCommitAsync() {
        Properties properties = createProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record: {}", record);
            }
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) {
                        log.error("commit failed for offsets {}", offsets, e);
                    }
                    log.info("commit succeeded");

                }
            });
        }
    }

    private Properties createProperties() {
        Properties configs = new Properties();

        // 필수옵션
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 선택옵션
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return configs;
    }
}
