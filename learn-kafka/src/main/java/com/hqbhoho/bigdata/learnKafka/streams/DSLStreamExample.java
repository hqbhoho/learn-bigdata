package com.hqbhoho.bigdata.learnKafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

/**
 * describe:
 * High-Level Streams DSL Example    wordCount
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/29
 */
public class DSLStreamExample {

    public static void main(String[] args) {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("wordcount-example-6-source");
        KTable<String, Long> wordCountsTable = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(" ")))
                .groupBy((key, word) -> word)
                .count("Count-6");
//        KTable<Windowed<String>, Long> wordCountsTable = wordCountsStream.count(TimeWindows.of(60_000), "Counts");
        wordCountsTable.to(Serdes.String(), Serdes.Long(), "wordcount-example-6-sink");

//        wordCountsTable.toStream().to(Serdes.String(), Serdes.Long(), "wordcount-example-2-sink");
//        wordCountsTable
//                .toStream()
//                .map((k,v)-> new KeyValue<>(k.key(),v))
//                .to(Serdes.String(), Serdes.Long(), "wordcount-example-2-sink");
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
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example-2");
        // Where to find Kafka broker(s).
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092,");
        // Specify default (de)serializers for record keys and for record values.
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return prop;
    }
}
