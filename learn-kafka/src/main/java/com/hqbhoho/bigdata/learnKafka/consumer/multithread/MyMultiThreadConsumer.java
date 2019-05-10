package com.hqbhoho.bigdata.learnKafka.consumer.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.AtomicInteger;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * describe:
 * 单个KafkaConsumer,多个线程处理拉取回来的数据
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/20
 */
public class MyMultiThreadConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MyMultiThreadConsumer.class);

    private KafkaConsumer consumer;

    public MyMultiThreadConsumer(Properties prop, List<String> topics) {
        this.consumer = new KafkaConsumer(prop);
        consumer.subscribe(topics);
    }

    public void execute() {
        while (true) {
            // 拉取消息
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (!records.isEmpty()) {
                int num = records.count();
                CountDownLatch latch = new CountDownLatch(num);
                AtomicInteger counter = new AtomicInteger(0);
                LOG.info("<=======>本批次返回的消息数:{}", num);
                records.forEach(record ->
                {
                    CompletableFuture.runAsync(() -> {
                        LOG.info("<=======>thread_id:{},topic:{},partition:{},offset:{},key:{},value:{}",
                                Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }).whenComplete((s, e) -> {
                        if (e == null) counter.getAndIncrement();
                        try {
                            latch.countDown();
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }
                    });
                });
                try {
                    latch.await();
                    LOG.info("<=======>records is processed!!! ");
                    if (counter.get() == num) {
                        consumer.commitAsync();
                        LOG.info("<=======>commited complete!!!");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //测试
    public static void main(String[] args) {
        new MyMultiThreadConsumer(loadProp(), Collections.singletonList("testTopic001")).execute();
    }

    // 加载配置项
    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "hqbhoho009");
        props.put("enable.auto.commit", "false");
        return props;
    }
}
