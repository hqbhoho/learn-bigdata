package com.hqbhoho.bigdata.learnKafka.producer;


import com.hqbhoho.bigdata.learnKafka.pojo.Account;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class FlinkSerializerProducer {
    private final static Logger LOGGER = LoggerFactory.getLogger(FlinkSerializerProducer.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, Account> producer = new KafkaProducer<>(properties);
        IntStream.rangeClosed(300, 320).forEach(i ->
        {

            Account account = new Account("item" + i, i + 9.9, System.currentTimeMillis());
            ProducerRecord<String, Account> record =
                    new ProducerRecord<>("flink-test-1", null, System.currentTimeMillis(), String.valueOf(i), account);
            Future<RecordMetadata> future = producer.send(record);
            try {
                TimeUnit.MILLISECONDS.sleep(2000);
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {},timestamp {}", i, metaData.offset(), metaData.timestamp());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.hqbhoho.bigdata.learnKafka.serialization.AccountJsonSerializer");
        return props;
    }
}
