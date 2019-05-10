package com.hqbhoho.bigdata.learnKafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * describe:
 * Producer中的事物
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/26
 */
public class TransactionProducer {

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(initProps());
        // 1.初始化事务
        producer.initTransactions();
        // 2.开启事务
        producer.beginTransaction();

        try {
            // 3.kafka写操作集合
            // 3.1 do业务逻辑
            // 3.2 发送消息
            producer.send(new ProducerRecord<String, String>("test", "transaction-data-1"));

            producer.send(new ProducerRecord<String, String>("test", "transaction-data-2"));
            // 3.3 do其他业务逻辑,还可以发送其他topic的消息。
//            producer.sendOffsetsToTransaction(null,null);
            // 4.事务提交
            producer.commitTransaction();
        } catch (Exception e) {
            // 5.放弃事务
            producer.abortTransaction();
        }
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置事务id
        props.put("transactional.id", "transaction-1");
        // 设置幂等性
        props.put("enable.idempotence",true);
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        return props;
    }
}


