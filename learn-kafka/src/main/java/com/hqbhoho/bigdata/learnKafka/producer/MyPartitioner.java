package com.hqbhoho.bigdata.learnKafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * describe:
 * 自定义Partitioner  根据key的字段值分区
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/12
 */
public class MyPartitioner implements Partitioner {

    private final String PARTITIONS = "topic.partitions";
    private int partitions;
    private final String LOG_KEYS = "log.keys";
    private String keys;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        if (keyBytes == null || keyBytes.length == 0) {
            throw new IllegalArgumentException("The key is required for BIZ.");
        }

        String[] keyList = keys.split(",");

        if (keyList.length > partitions) {
            throw new IllegalArgumentException("key num more than partition num ");
        }

        //  按照配置的顺序发送到相应的分区   eg：keys="register,login,trade"  那么key为login的消息会被发送到partition-1
        for (int i = 0; i < keyList.length; i++) {
            if (key.toString().equalsIgnoreCase(keyList[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("The key is invalid.");
    }

    @Override
    public void close() {
        //  do nothing
    }

    @Override
    public void configure(Map<String, ?> configs) {
        partitions = Integer.valueOf((String) configs.get(PARTITIONS));
        keys = (String) configs.get(LOG_KEYS);
    }

    //测试
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>("testTopic001", "login", "login-1");
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        System.out.println("==========================================================================");
        System.out.println("message which key='login',except send to partition-1,and actually send to " + recordMetadata.partition());
        System.out.println("==========================================================================");
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.hqbhoho.bigdata.learnKafka.producer.MyPartitioner");
        // 自定义配置项
        props.put("topic.partitions", "3");
        props.put("log.keys", "register,login,trade");
        return props;
    }
}
