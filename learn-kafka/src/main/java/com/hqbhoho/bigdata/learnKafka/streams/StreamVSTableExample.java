package com.hqbhoho.bigdata.learnKafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

/**
 * describe:
 * KStream VS KTable
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/31
 */
public class StreamVSTableExample {

    public static void main(String[] args) {
        KStreamBuilder builder = new KStreamBuilder();
        builder.stream("wordcount-example-6-sink").print();
//        builder.table("wordcount-example-6-sink", "Counts6").print();
        KafkaStreams streams = new KafkaStreams(builder, loadProp());

        streams.start();
    }
    /**
     * 加载配置项
     *
     * @return
     */
    public static Properties loadProp() {
        Properties prop = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example-8");
        // Where to find Kafka broker(s).
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092,");
        // Specify default (de)serializers for record keys and for record values.
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        return prop;
    }
}
