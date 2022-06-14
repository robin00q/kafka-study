package me.study.apachekafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

@Slf4j
public class KStreamJoinGlobalKTable {

    private static final String APPLICATION_NAME = "global-order-join-application";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ADDRESS_TABLE = "address_v3"; // source 토픽명
    private static final String ORDER_STREAM = "order"; // source 토픽명
    private static final String ORDER_JOIN_STREAM = "order_join"; // sink 토픽명

    public void join() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 최신의 메세지 키 값만 사용 (like key value store)
        GlobalKTable<String, String> addressTable = builder.globalTable(ADDRESS_TABLE);
        // 모든 레코드 사용
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        // 메시지키가 같은 경우만 조인이 된다
        orderStream.join(addressTable,
                        (orderKey, orderValue) -> orderKey, // 키 기준으로 조인하겠다.
                        (order, address) -> order + " send to " + address) // join()
                .to(ORDER_JOIN_STREAM); // to()

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
