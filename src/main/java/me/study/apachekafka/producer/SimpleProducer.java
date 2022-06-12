package me.study.apachekafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * 필수옵션 세팅
 * <ul>
 *     <li>BootStrap Server IP</li>
 *     <li>Key Serializer</li>
 *     <li>Value Serializer</li>
 * </ul>
 */
@Slf4j
public class SimpleProducer {

    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    /**
     * 트랜잭션 프로듀서
     */
    public void transactionProducer() {
        Properties properties = createProperties();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        // 트랜잭션 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 레코드 생성
        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(record);
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.commitTransaction();

        flushAndClose(producer);
    }

    /**
     * 멱등성 프로듀서
     */
    public void idempotentProducer() {
        Properties properties = createProperties();
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 멱등성 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 레코드 생성
        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        producer.send(record);

        flushAndClose(producer);
    }

    public void produceWithoutMessageKey() {
        Properties properties = createProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 레코드 생성
        // 현재는 토픽명과 메세지값만 생성한다.
        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        // send() to Partitioner/Accumulator
        producer.send(record);
        log.info("record: {}", record);

        flushAndClose(producer);
    }

    public void produceWithMessageKey() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(createProperties());

        // 레코드 생성
        // 토픽명, 메세지키, 메세지 값 정의
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Seoul", "Seoul");
        producer.send(record);
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "Busan", "Busan");
        producer.send(record2);

        flushAndClose(producer);
    }

    /**
     * 파티션 동작방식 (같은 키는 같은 파티션에 해시돼서 들어감) 과는 다르게, 지정한 파티션에 전송되게 된다.
     */
    public void produceToPartitionNumber() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(createProperties());

        // 레코드 생성
        // 토픽명, 메세지키, 메세지 값, 파티션 번호 지정
        int partitionNumber = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNumber, "Seoul", "Seoul");
        producer.send(record);

        flushAndClose(producer);
    }

    /**
     * 특정 데이터를 가지는 레코드를 특정 파티션으로 보내야 하는 경우, "커스텀 파티셔너" 를 사용할 수 있다.
     * @see CustomPartitioner
     */
    public void produceWithCustomPartitioner() {
        Properties properties = createProperties();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 레코드 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Seoul", "Seoul");
        producer.send(record);

        flushAndClose(producer);
    }

    /**
     * 레코드의 전송 결과를 확인하는 프로듀서 (sync callback)
     * - 전송 결과 포맷 (acks=1) : [토픽명-파티션번호@오프셋]
     * - 전송 결과 포맷 (acks=0) : [토픽명-파티션번호@-1]
     * send() 메서드는 Future 객체를 반환한다.
     * @see java.util.concurrent.Future
     */
    public void produceAndGetResult() {
        Properties properties = createProperties();
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // 레코드 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Seoul", "Seoul");

        try {
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata metadata = send.get();
            log.info("metadata: {}", metadata.toString());
        } catch (Exception e) {
            log.error("errorMessage : {}", e.getMessage(), e);
        } finally {
            flushAndClose(producer);
        }
    }


    private Properties createProperties() {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return configs;
    }

    /**
     * producer.close() 를 사용하여 Accumulator 에 저장된 데이터를 카프카 클러스터로 전송해야 한다.
     */
    private void flushAndClose(KafkaProducer<String, String> producer) {
        producer.flush();
        producer.close();
    }
}
