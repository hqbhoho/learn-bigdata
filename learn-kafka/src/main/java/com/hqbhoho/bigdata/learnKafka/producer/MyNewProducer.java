package com.hqbhoho.bigdata.learnKafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * describe:
 * Kafka新生产者
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/10
 */
public class MyNewProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(MyNewProducer.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        ProducerRecord<String, Long> record1 =
//                new ProducerRecord<>("wordcount-example-6-sink","kafka",100L);
//        ProducerRecord<String, Long> record2 =
//                new ProducerRecord<>("wordcount-example-6-sink","kafka",110L);
//        producer.send(record1);
//        producer.send(record2);

        //定义同（异）步
        boolean isAsync = false;
        IntStream.rangeClosed(0, 10).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("testTopic001", String.valueOf(i), "hello " + i);
            if (isAsync) {
                //异步发送，可以注册回调函数,有结果返回，将调用回调函数
                producer.send(record, (r, e) -> {
                    if (e == null) {
                        //正常发送
                        LOGGER.info("The message is send done and the key is {},offset {}", i, r.offset());
                    } else {
                        //发送异常
                        e.printStackTrace();
                    }
                });
            } else {
                //同步发送，会block住
                Future<RecordMetadata> future = producer.send(record);
                try {
                    RecordMetadata metaData = future.get();
                    LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
