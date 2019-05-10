package com.hqbhoho.bigdata.learnKafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;

/**
 * describe:
 * 自定义消费者拦截器
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/19
 */
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(MyConsumerInterceptor.class);

    private AtomicInteger count = new AtomicInteger(0);

    /**
     * This is called just before the records are returned by KafkaConsumer.poll(long)
     *
     * @param consumerRecords
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> results = new HashMap<>();

        Set<TopicPartition> partitions = consumerRecords.partitions();
        partitions.forEach(p ->
        {
            // 消息中包含0的消息被返回，其他消息被丢弃
            List<ConsumerRecord<String, String>> result = consumerRecords.records(p)
                    .stream().filter(record -> record.value().contains("0"))
                    .collect(toList());
            results.put(p, result);
        });
        return new ConsumerRecords<>(results);
    }

    /**
     * This is called when offsets get committed.
     *
     * @param map
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        // 统计一下   什么时候提交了offset  提交了几次
        LOG.info("<=======>time:{},commit offset:{},count:{}", System.currentTimeMillis(), map, count.incrementAndGet());
    }

    /**
     * 关闭interceptor，主要用于执行一些资源清理工作
     */
    @Override
    public void close() {
        // do nothing
    }

    /**
     * 加载一些配置项
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
    }

    // 测试
    public static void main(String[] args) {
        //创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        // 订阅topic
        consumer.subscribe(Collections.singletonList("testTopic001"));
        while (true) {
            // 拉取消息
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                LOG.info("<=======>topic:{},partition:{},offset:{},key:{},value:{}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            });
        }
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "hqbhoho008");
        props.put("client.id", "hqbhoho-client");
        // 指定拦截器类
        props.put("interceptor.classes", "com.hqbhoho.bigdata.learnKafka.consumer.MyConsumerInterceptor");
        return props;
    }
}
