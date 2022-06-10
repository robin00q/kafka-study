package me.study.apachekafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 필수옵션 세팅
 * <ul>
 *     <li>BootStrap Server IP</li>
 *     <li>Key Serializer</li>
 *     <li>Value Serializer</li>
 * </ul>
 */
public class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public void produce() {
        // 필수옵션 세팅
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 카프카 프로듀서 인스턴스 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // 레코드 생성
        // 현재는 토픽명과 메세지값만 생성한다.
        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        // send() to Partitioner/Accumulator
        producer.send(record);
        log.info("record: {}", record);

        // flush() 하여 Accumulator 에 있는 데이터를 반영한다.
        producer.flush();

        // close() 하여 리소스 해제
        producer.close();
    }
}
