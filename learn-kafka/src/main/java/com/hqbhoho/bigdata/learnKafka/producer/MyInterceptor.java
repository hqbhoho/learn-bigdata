package com.hqbhoho.bigdata.learnKafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * describe:
 * 自定义KafkaProducer的拦截器
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/14
 */
public class MyInterceptor implements ProducerInterceptor<String, String> {

    private final static Logger LOGGER = LoggerFactory.getLogger(MyNewProducer.class);

    private AtomicInteger failCount = new AtomicInteger(0);
    private AtomicInteger successCount = new AtomicInteger(0);

    /**
     * Producer在消息被序列化、计算分区前调用该方法。
     * 用户可以在该方法中对消息做任何操作,相当于一个增强的方法。
     *
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 将消息中value值全部转换成大写,并且给消息加一个timestamp值
        //ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers)
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(),
                System.currentTimeMillis(), producerRecord.key(),
                producerRecord.value().toUpperCase(), producerRecord.headers());
    }

    /**
     * 该方法会在消息被应答之前或消息发送失败时调用，并且通常都是在producer回调逻辑触发之前。
     *
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 统计成功发送（失败）的记录数
        if (e == null) {
            successCount.incrementAndGet();
        } else {
            failCount.incrementAndGet();
        }
    }

    /**
     * 关闭interceptor，主要用于执行一些资源清理工作
     */
    @Override
    public void close() {
        LOGGER.info("<=======> Successful sent: " + successCount.get());
        LOGGER.info("<=======> Failed sent: " + failCount.get());
    }

    /**
     * 加载一些配置项
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        //   do nothing
    }

    //测试
    public static void main(String[] args) throws Exception {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.rangeClosed(0, 10).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("testTopic001", String.valueOf(i), "hello world" + i);
            producer.send(record);
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("interceptor.classes","com.hqbhoho.bigdata.learnKafka.producer.MyInterceptor");
        return props;
    }
}
