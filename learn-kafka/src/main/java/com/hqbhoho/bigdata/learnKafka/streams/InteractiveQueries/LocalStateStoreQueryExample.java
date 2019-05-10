package com.hqbhoho.bigdata.learnKafka.streams.InteractiveQueries;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * Querying local state stores (for an application instance)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/01
 */
public class LocalStateStoreQueryExample {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStateStoreQueryExample.class);

    public static void main(String[] args) {
        // DSL Stream
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("query-example-1-source");
        KTable<String, Long> wordCountsTable = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(" ")))
                .groupBy((key, word) -> word)
                .count("Count-2");
        wordCountsTable.to(Serdes.String(), Serdes.Long(), "query-example-1-sink");
        KafkaStreams streams = new KafkaStreams(builder, loadProp());
        streams.start();
        // schedule get local state store
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            // Get the key-value store CountsKeyValueStore
            ReadOnlyKeyValueStore<String, Long> keyValueStore =
                    streams.store("Count-2", QueryableStoreTypes.keyValueStore());
            // Get the values for all of the keys available in this application instance
            KeyValueIterator<String, Long> range = keyValueStore.all();
            while (range.hasNext()) {
                KeyValue<String, Long> next = range.next();
                LOG.info("<==========================>local state: " + next.key + ": " + next.value);
            }
            range.close();
        }, 10, 10, TimeUnit.SECONDS);
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
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "query-example-1");
        // Where to find Kafka broker(s).
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092");
        // Specify default (de)serializers for record keys and for record values.
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return prop;
    }
}
