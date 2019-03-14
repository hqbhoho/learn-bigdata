package com.hqbhoho.bigdata.learnKafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * describe:
 * Kafka旧生产者
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/10
 */
public class MyOldProducer {

    public static void main(String[] args)
    {
        Properties properties = initProps();
        Producer<String, String> producer = new Producer<>(new ProducerConfig(properties));
        KeyedMessage<String, String> message = new KeyedMessage<>("oldTopic", "key-1","hello");
        producer.send(message);
        producer.close();
    }

    private static Properties initProps()
    {
        final Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        return props;
    }
}
