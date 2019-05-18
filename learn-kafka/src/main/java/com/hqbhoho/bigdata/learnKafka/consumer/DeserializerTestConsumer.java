package com.hqbhoho.bigdata.learnKafka.consumer;

import com.hqbhoho.bigdata.learnKafka.avro.User;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class DeserializerTestConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MyNewConsumer.class);

    public static void main(String[] args) {
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("flink-test-2"));
        while(true) {
            ConsumerRecords<String, User> records = consumer.poll(100);
            records.forEach(record ->
            {
                LOG.info("<====> key:{},partition:{},value:{},", record.key(),record.partition(),record.value());
            });
        }
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.hqbhoho.bigdata.learnKafka.serialization.UserAvroDeserializer");
        props.put("group.id", "hqbhoho004");
        props.put("client.id", "hqbhoho-client");
//        props.put("auto.offset.reset","earliest");
//        props.put("isolation.level","read_committed");
        return props;
    }
}
