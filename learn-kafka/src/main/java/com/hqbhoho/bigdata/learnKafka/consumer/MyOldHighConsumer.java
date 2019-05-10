package com.hqbhoho.bigdata.learnKafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * describe:
 * Kafka旧消费者客户端高级API
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/17
 */
public class MyOldHighConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MyOldHighConsumer.class);

    public static void main(String[] args) {
        ConsumerConfig conf = new ConsumerConfig(loadProp());
        Map<String, Integer> topicAssignMap = new HashMap<>();
        String topic = "testTopic001";
        // 指定订阅的topic和消费组内的消费者线程数
        topicAssignMap.put(topic, 3);
        // 创建消费者连接器
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(conf);
        Map<String, List<KafkaStream<byte[], byte[]>>> messagesMap =
                consumer.createMessageStreams(topicAssignMap);
        // 每个消费者线程对应一个消息流
        List<KafkaStream<byte[], byte[]>> streams = messagesMap.get(topic);
        // 多线程处理多个消费者线程的消息流
        ExecutorService pool = Executors.newFixedThreadPool(3);
        for (KafkaStream<byte[], byte[]> stream : streams) {
            pool.submit(()->{
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (it.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> mg = it.next();
                    LOG.info(Thread.currentThread().getName()+" <===> topic:{},partition:{},offset:{},message:{}"
                            , mg.topic(), mg.partition(),mg.offset(), new String(mg.key()) + ":" + new String(mg.message()));
                }
            });
        }
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.5.108:2181,192.168.5.109:2181,192.168.5.110:2181/kafka");
        props.put("group.id", "hqbhoho007");
        // 自动提交偏移量间隔
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","smallest");
        return props;
    }
}
