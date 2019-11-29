package com.hqbhoho.bigdata.learnKafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * describe:
 * Kafka新版本消费者,自定义消费者再平衡监听器
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class MyNewTestConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MyNewTestConsumer.class);

    public static void main(String[] args) {
        //创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        // 订阅topic并定义再平衡监听器
        consumer.subscribe(Collections.singletonList("apex_irc_real_alert"), new ConsumerRebalanceListener() {
            private int count =0;
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                count++;
                LOG.info("<========>rebalance begin ,number:{} timestamp:{}",count,System.currentTimeMillis());
                LOG.info("<========>consumer:consumer-1,PartitionsRevoked:{}",collection);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                LOG.info("<========>rebalance end ,number:{} timestamp:{}",count,System.currentTimeMillis());
                LOG.info("<========>consumer:consumer-1,PartitionsAssigned:{}",collection);
            }
        });
        try {
            while (true) {
                // 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(100);
                records.forEach(record ->
                {
                    LOG.info("topic:{},partition:{},offset:{},key:{},value:{}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                });
                // 异步提交
                consumer.commitAsync();
            }
        } finally {
            try {
                // 同步提交
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.105.1.172:9092,10.105.1.175:9092,10.105.1.177:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "hqbhoho004");
        props.put("client.id", "hqbhoho-client");
        props.put("auto.offset.reset","earliest");
        return props;
    }
}
