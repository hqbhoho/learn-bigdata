package com.hqbhoho.bigdata.learnKafka.streams.InteractiveQueries;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Arrays;
import java.util.Properties;

/**
 * describe:
 * Querying remote state stores (for the entire application)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/01
 */
public class RemoteStateQueryExample {

    private final static String HOST = "localhost";
    private final static int PORT = 19999;

    public static void main(String[] args) throws Exception {
        // DSL Stream
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("query-example-1-source");
        KTable<String, Long> wordCountsTable = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(" ")))
                .groupBy((key, word) -> word)
                .count("Count-3");
        wordCountsTable.to(Serdes.String(), Serdes.Long(), "query-example-1-sink");
        KafkaStreams streams = new KafkaStreams(builder, loadProp());
        streams.start();
        // start rest proxy
        HostInfo hostInfo = new HostInfo(HOST, PORT);
        InteractiveQueriesRestService
                interactiveQueriesRestService = new InteractiveQueriesRestService(streams, hostInfo);
        interactiveQueriesRestService.start(PORT);
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                interactiveQueriesRestService.stop();
            } catch (Exception e) {
                // ignored
            }
        }));
    }

    // 加载配置项
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
        // Set the unique RPC endpoint of this application instance
        prop.put(StreamsConfig.APPLICATION_SERVER_CONFIG, HOST + ":" + PORT);
        // Set
        prop.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        // Set state dirs
        prop.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-18888");
        return prop;
    }

}
