package com.hqbhoho.bigdata.learnKafka.producer;

import com.hqbhoho.bigdata.learnKafka.pojo.Item;
import com.hqbhoho.bigdata.learnKafka.pojo.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class SerializerTestProducer {
    private final static Logger LOGGER = LoggerFactory.getLogger(SerializerTestProducer.class);

    public static void main(String[] args) {
        Properties properties = initProps();
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        IntStream.rangeClosed(100, 110).forEach(i ->
        {
            List<Item> items = new ArrayList<>();
            items.add(new Item(i, "item---" + i, i + 0.99));
            ProducerRecord<String, User> record =
                    new ProducerRecord<>("testTopic001", String.valueOf(i), new User(i, "user---" + i, items));
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.hqbhoho.bigdata.learnKafka.serialization.UserAvroSerializer");
        return props;
    }
}
