package com.hqbhoho.bigdata.learnKafka.consumer.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * describe:
 * 定义一个ConsumerGroup类，里面包含多个Consumer线程
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/20
 */
public class MyConsumerGroup {

    private List<MyConsumerRunnable> consumers;

    private ExecutorService threadPool;

    public MyConsumerGroup(int consumerNum, Properties prop, List<String> topics) {
        threadPool = Executors.newFixedThreadPool(consumerNum);
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; ++i) {
            MyConsumerRunnable consumerThread = new MyConsumerRunnable(prop, topics);
            consumers.add(consumerThread);
        }
    }

    /**
     * 多KakfaConsumer线程，同时独立处理消息
     */
    public void execute() {
        consumers.forEach(consumer -> {
            threadPool.submit(consumer);
        });
    }

    /**
     * 定义一个KafkaConsumer线程类，执行具体的Consumer逻辑
     */
    static class MyConsumerRunnable implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(MyConsumerRunnable.class);
        // 每个线程维护私有的KafkaConsumer实例
        private final KafkaConsumer<String, String> consumer;

        public MyConsumerRunnable(Properties prop, List<String> topics) {
            //创建consumer
            consumer = new KafkaConsumer<>(prop);
            // 订阅topic
            consumer.subscribe(topics);
        }

        /**
         * 消费消息的逻辑
         */
        @Override
        public void run() {
            while (true) {
                // 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(100);
                records.forEach(record ->
                {
                    LOG.info("<=======>thread_id:{},topic:{},partition:{},offset:{},key:{},value:{}",
                            Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value());
                });
            }
        }
    }

    // 测试
    public static void main(String[] args) {
        new MyConsumerGroup(3, loadProp(), Collections.singletonList("testTopic001")).execute();
    }

    // 加载配置项
    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "hqbhoho009");
        return props;
    }
}
